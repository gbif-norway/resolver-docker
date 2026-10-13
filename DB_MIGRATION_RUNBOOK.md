# Database Migration Runbook (DigitalOcean -> NIRD In-Cluster PostgreSQL)

This runbook documents the migration pattern used for `resolver`, and can be reused for `publishgpt`.

## Scope

- Source: DigitalOcean managed PostgreSQL (remote).
- Target: in-cluster shared PostgreSQL (`postgres-shared`) in namespace `gbif-no-ns8095k`.
- Cluster context: `nird-lmd`.
- Storage model: existing project PVC `573890b9-3346-4027-ab0c-22eec6dfd665` with subdir `postgres-shared/`.

## Current NIRD DB Objects

- StatefulSet: `postgres-shared`
- Service: `postgres-shared` (port `5432`)
- Secret: `postgres-shared-secrets`
  - Contains admin credentials and per-app DB user/password values.

## Important Constraints Seen on This Cluster

1. No dynamic StorageClass available for this namespace.
2. Namespace `LimitRange` enforces memory `limit/request <= 2`.
3. Default-deny network policies exist in namespace, so explicit allow policies are needed for app ingress.

## One-Time Setup (Already Done)

Shared PostgreSQL manifests live in:

- `k8s/nird/postgres-shared/`

Do not apply placeholder secret from template in kustomize; create real secret first.

## Per-Database Migration Procedure

Use this for each app database (`resolver`, `publishgpt`, etc.).

### 1) Pre-checks

1. Ensure source DB credentials are known.
2. Ensure target DB and target DB user exist in `postgres-shared` (via init script or manual SQL).
3. Verify source row count:

```sh
psql "<source-conn>" -Atc "SELECT count(*) FROM <important_table>;"
```

### 2) Full logical copy (pg_dump -> psql stream)

Run from an in-cluster Job (recommended), not local laptop.

Core command pattern:

```sh
PGPASSWORD="<source_pw>" pg_dump "<source-conn>" --no-owner --no-privileges \
  | PGPASSWORD="<target_pw>" psql "<target-conn>"
```

Recommended sequence:

1. Terminate active connections to target DB.
2. Drop and recreate target DB owned by target app user.
3. Stream dump from source to target.
4. Compare source vs target key row counts.

### 3) Long-running index build notes

Large GIN indexes may take a long time.

Progress checks:

```sh
kubectl --context nird-lmd -n gbif-no-ns8095k exec postgres-shared-0 -- \
  psql -U postgres -d <db> -c \
  "SELECT * FROM pg_stat_progress_create_index;"
```

Validity checks:

```sh
kubectl --context nird-lmd -n gbif-no-ns8095k exec postgres-shared-0 -- \
  psql -U postgres -d <db> -c \
  "SELECT c.relname, i.indisvalid, i.indisready, i.indislive
   FROM pg_index i
   JOIN pg_class c ON c.oid=i.indexrelid
   WHERE c.relname='<index_name>';"
```

If an index already exists and is valid, skip rebuild.

### 4) Cutover app to in-cluster DB

Update app secret/env to:

- `SQL_HOST=postgres-shared.gbif-no-ns8095k.svc.cluster.local`
- `SQL_PORT=5432`
- `SQL_DATABASE=<app_db>`
- `SQL_USER=<app_db_user>`
- `SQL_PASSWORD=<target_user_password>`
- `SQL_SSLMODE=disable` (internal cluster traffic)

Then restart deployment:

```sh
kubectl --context nird-lmd -n gbif-no-ns8095k rollout restart deploy/<app-deployment>
kubectl --context nird-lmd -n gbif-no-ns8095k rollout status deploy/<app-deployment>
```

### 5) Post-cutover verification

1. API/app endpoint returns expected data.
2. App logs show normal startup and no DB auth/connection errors.
3. Spot-check key queries/records.

## Rollback Procedure

If issues occur after cutover:

1. Restore previous app DB env/secret values (DigitalOcean host/user/password/db).
2. Restart app deployment.
3. Verify app traffic/data returns to baseline.

```sh
kubectl --context nird-lmd -n gbif-no-ns8095k rollout restart deploy/<app-deployment>
kubectl --context nird-lmd -n gbif-no-ns8095k rollout status deploy/<app-deployment>
```

## Notes for `publishgpt` Migration

For the next migration agent:

1. Repeat this runbook with `publishgpt` source credentials.
2. Migrate DB first, then switch the app.
3. Keep source DO DB untouched until publishgpt app verification passes.
