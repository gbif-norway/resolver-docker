# Deploy resolver-docker to Sigma2 NIRD

This repo now contains a basic Kubernetes deployment in `k8s/nird`.
Target namespace: `gbif-no-ns8095k`.
Ingress host: `resolver.gbif-no.sigma2.no`.

## 1) Configure kubectl access

Follow `nird-toolkit-auth-helper-README.md`:

1. Install `nird-toolkit-auth-helper`.
2. Put the provided kubeconfig in `$HOME/.kube/config`.
3. Select context:

```sh
kubectl config use-context nird-lmd
kubectl get ns
```

## 2) Set app environment

The deployment expects a secret named `resolver-env`.

Option A (recommended): generate from your local env file:

```sh
kubectl create secret generic resolver-env \
  --namespace gbif-no-ns8095k \
  --from-env-file=prod.env \
  --dry-run=client -o yaml | kubectl apply -f -
```

Option B: edit `k8s/nird/secret-template.yaml` and apply it.

## 3) Deploy

```sh
kubectl apply -k k8s/nird
kubectl -n gbif-no-ns8095k rollout status deploy/resolver-web
kubectl -n gbif-no-ns8095k get pods,svc,ingress
```

## 3b) Deploy app updates

If you publish a new image with an immutable tag:

```sh
kubectl -n gbif-no-ns8095k set image deploy/resolver-web resolver-web=gbifnorway/resolver:<tag>
kubectl -n gbif-no-ns8095k rollout status deploy/resolver-web
```

If you keep using `gbifnorway/resolver:latest`:

```sh
kubectl -n gbif-no-ns8095k rollout restart deploy/resolver-web
kubectl -n gbif-no-ns8095k rollout status deploy/resolver-web
```

## 4) Test access

For quick testing from your laptop:

```sh
kubectl -n gbif-no-ns8095k port-forward svc/resolver-web 8080:80
```

Then open `http://127.0.0.1:8080`.

## 5) DNS and TLS checks

Before rollout, make sure:

1. `resolver.gbif-no.sigma2.no` points to the ingress endpoint.
2. `resolver.gbif.no` is configured as CNAME to `ingress.nird-lmd.sigma2.no` (if using public `gbif.no` host).
3. TLS secrets exist in `gbif-no-ns8095k`:
   - `wildcard-tls` for `*.gbif-no.sigma2.no`
   - `gbif-tls` for `resolver.gbif.no`
4. You are using the image version you want to run (replace `:latest` with a fixed tag for safer rollbacks).

## Notes

- Current image: `gbifnorway/resolver:latest`.
- This image runs DB migrations and starts an internal weekly cron on pod startup.
- Keep `replicas: 1` unless you first move scheduled jobs out of the web pod.
- The bundle includes a `NetworkPolicy` that allows ingress traffic from namespace `kube-ingress` to `resolver-web` on port `8080`.
- Shared in-cluster PostgreSQL manifests are in `k8s/nird/postgres-shared/`.
