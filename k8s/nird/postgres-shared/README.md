# Shared PostgreSQL on Existing Project PVC

This deploys one PostgreSQL 15 instance for multiple app databases in namespace `gbif-no-ns8095k`.

## Storage model

- Uses existing project PVC: `573890b9-3346-4027-ab0c-22eec6dfd665`.
- PostgreSQL data lives under subdirectory: `postgres-shared/`.
- No new PVC is created.
- PostgreSQL does not reserve fixed capacity up front; it grows as data grows.

## Included databases/users

Created during first initialization only:

- `resolver` owned by `resolver_user`
- `publishgpt` owned by `publishgpt_user`

## 1) Set secrets

`secret-template.yaml` is a reference template and is not applied by `kustomize`.
Create the secret explicitly before deploying:

```sh
kubectl apply -f k8s/nird/postgres-shared/secret-template.yaml
```

If you prefer, create the secret from env file instead:

```sh
kubectl create secret generic postgres-shared-secrets \
  -n gbif-no-ns8095k \
  --from-env-file=postgres-shared.env \
  --dry-run=client -o yaml | kubectl apply -f -
```

Then apply remaining resources.

## 2) Verify

```sh
kubectl -n gbif-no-ns8095k get sts,pods,svc
kubectl -n gbif-no-ns8095k rollout status sts/postgres-shared
kubectl -n gbif-no-ns8095k exec -it postgres-shared-0 -- psql -U postgres -d postgres -c "\\l"
```

## 3) Connection strings

- Host: `postgres-shared.gbif-no-ns8095k.svc.cluster.local`
- Port: `5432`

Resolver app target values:

- `SQL_ENGINE=django.db.backends.postgresql`
- `SQL_HOST=postgres-shared.gbif-no-ns8095k.svc.cluster.local`
- `SQL_PORT=5432`
- `SQL_DATABASE=resolver`
- `SQL_USER=resolver_user`
- `SQL_PASSWORD=<from secret>`

## Patching

This setup uses `postgres:15`.
For regular patching, update the image tag and roll out:

```sh
kubectl -n gbif-no-ns8095k set image sts/postgres-shared postgres=postgres:15
kubectl -n gbif-no-ns8095k rollout status sts/postgres-shared
```

Use a specific patch tag if you want tighter control, e.g. `postgres:15.x`.

## Notes

- Existing platform-level backup for the shared project storage is assumed.
- `NetworkPolicy` allows DB ingress on `5432` from pods in the same namespace.
