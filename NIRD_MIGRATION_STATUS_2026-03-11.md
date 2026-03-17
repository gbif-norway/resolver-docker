# NIRD Deployment and Migration Status (March 11, 2026)

Update (March 17, 2026): `resolver.gbif.no` DNS/TLS cutover is complete and the old DigitalOcean resolver app workload has been decommissioned.

## Executive Summary

- We're in the process of migrating a bunch of things from DigitalOcean to the Norwegian Sigma2's NIRD platform which has a k8s cluster. 
- `resolver` app is deployed and running on NIRD in namespace `gbif-no-ns8095k`.
- `resolver` now uses in-cluster PostgreSQL (`postgres-shared`), not DigitalOcean.
- Data migration for `resolver` is complete and validated.
- DNS/TLS for `resolver.gbif-no.sigma2.no` is working.
- DNS/TLS for `resolver.gbif.no` is now working on NIRD.

## Current Live State

### App

- Deployment: `resolver-web` (Ready `1/1`)
- Image: `gbifnorway/resolver:latest`
- Ingress host: `resolver.gbif-no.sigma2.no`
- Ingress address: `158.36.102.245`
- API check: `https://resolver.gbif-no.sigma2.no/?format=json` returns populated results.

### Database

- In-cluster DB workload: StatefulSet `postgres-shared` (Ready `1/1`)
- Service endpoint for apps: `postgres-shared.gbif-no-ns8095k.svc.cluster.local:5432`
- Resolver app DB config in secret points to in-cluster DB:
  - `SQL_HOST=postgres-shared.gbif-no-ns8095k.svc.cluster.local`
  - `SQL_PORT=5432`
  - `SQL_DATABASE=resolver`
  - `SQL_USER=resolver_user`
  - `SQL_SSLMODE=disable`

## Resolver DB Migration Progress

### Completed

1. Shared PostgreSQL stack deployed on existing project PVC using subdirectory `postgres-shared/`.
2. Source (`DO`) to target (`postgres-shared`) logical migration executed.
3. Row count validation completed:
   - `website_resolvableobject`: `17,726,416` rows on both source and target.
4. Important indexes confirmed valid and ready:
   - `website_resolvableobject_pkey`
   - `website_res_data_00a3fa_gin` (GIN on `data`)

### Current Size

- In-cluster `resolver` DB size: `41 GB`
- This is lower than source (`~51 GB` observed before cutover), which is expected after dump/restore cleanup.

## Key Findings Discovered During Work

1. Namespace has default-deny network policy behavior.
   - Required explicit ingress allow policy for `resolver-web` from `kube-ingress`.
2. Namespace has resource ratio limits (`memory limit/request <= 2`).
   - StatefulSet memory request/limit had to be adjusted accordingly.
3. No dynamic StorageClass access for this namespace.
   - New PVC probes stayed `Pending` without explicit storage class/PV.
   - Existing project storage PVC is the practical path.
4. Existing TLS secret `wildcard-tls` only covers:
   - `*.gbif-no.sigma2.no`
   - `gbif-no.sigma2.no`
   - It does **not** cover `*.gbif.no`.

## DNS/TLS Status and Impact

### Working now

- `resolver.gbif-no.sigma2.no` works with current ingress + cert.
- `resolver.gbif.no` works via CNAME to `ingress.nird-lmd.sigma2.no` with NIRD-managed ingress/certificates.
- Old DO resolver deployment/service/ingress were removed after cutover.

## What Is Ready for the Next Agent (`publishgpt`)

- Shared PostgreSQL infrastructure is in place and healthy.
- Reusable migration runbook exists:
  - `DB_MIGRATION_RUNBOOK.md`
- Postgres manifests and notes:
  - `k8s/nird/postgres-shared/`

Suggested next scope for next agent:

1. Migrate `publishgpt` DB into `postgres-shared`.
2. Update `chatipt` app DB config to in-cluster DB.
3. Verify app behavior after cutover.
