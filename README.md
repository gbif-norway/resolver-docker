[![run-tests](https://github.com/gbif-norway/resolver-docker/actions/workflows/run-tests.yml/badge.svg)](https://github.com/gbif-norway/resolver-docker/actions/workflows/run-tests.yml)

# resolver-docker

Django API for resolver data, deployed on Sigma2 NIRD (`nird-lmd`) in namespace `gbif-no-ns8095k`.

## Local development

Create environment settings in `dev.env` (one `KEY=value` per line), then run:

```sh
docker compose -f docker-compose.dev.yml up
```

Run tests:

```sh
docker compose -f docker-compose.test.yml up --abort-on-container-exit --exit-code-from web
```

## Production deployment target

- Namespace: `gbif-no-ns8095k`
- Deployment: `resolver-web`
- Service: `resolver-web`
- Ingress hosts:
  - `resolver.gbif.no` (public canonical host)
  - `resolver.gbif-no.sigma2.no` (platform host)
- K8s manifests: `k8s/nird/`

Detailed cluster setup notes are in [k8s/nird/README.md](k8s/nird/README.md).

## One-time prerequisites

1. Configure kubectl context `nird-lmd` with `nird-toolkit-auth-helper`.
2. Ensure `resolver-env` secret exists in `gbif-no-ns8095k`:

```sh
kubectl create secret generic resolver-env \
  -n gbif-no-ns8095k \
  --from-env-file=prod.env \
  --dry-run=client -o yaml | kubectl apply -f -
```

## Deploying new code changes

### Recommended flow (immutable image tag)

1. Build and push a tagged image:

```sh
export IMAGE_TAG=$(git rev-parse --short HEAD)
docker buildx build --platform linux/amd64 -t gbifnorway/resolver:${IMAGE_TAG} --push .
```

2. Update deployment image and wait for rollout:

```sh
kubectl --context nird-lmd -n gbif-no-ns8095k set image deploy/resolver-web resolver-web=gbifnorway/resolver:${IMAGE_TAG}
kubectl --context nird-lmd -n gbif-no-ns8095k rollout status deploy/resolver-web
```

3. Verify:

```sh
kubectl --context nird-lmd -n gbif-no-ns8095k get pods,svc,ingress
curl -fsS https://resolver.gbif-no.sigma2.no/?format=json | head
```

### Current CI flow (`latest`)

This repo has GitHub Actions to test and publish `gbifnorway/resolver:latest` on push to `main`.

After CI publishes, restart deployment to pull the new image:

```sh
kubectl --context nird-lmd -n gbif-no-ns8095k rollout restart deploy/resolver-web
kubectl --context nird-lmd -n gbif-no-ns8095k rollout status deploy/resolver-web
```

## Rollback

If a deploy fails:

```sh
kubectl --context nird-lmd -n gbif-no-ns8095k rollout history deploy/resolver-web
kubectl --context nird-lmd -n gbif-no-ns8095k rollout undo deploy/resolver-web
kubectl --context nird-lmd -n gbif-no-ns8095k rollout status deploy/resolver-web
```
