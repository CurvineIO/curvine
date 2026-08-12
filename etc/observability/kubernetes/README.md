# Curvine production observability

These examples target Prometheus Operator `monitoring.coreos.com/v1` resources.

1. Expose the master metrics port (`9010` by default), worker metrics port
   (`9001` by default) and Transfer metrics port (`9011` by default) as a named
   `metrics` Service or container port.
2. Label workloads and Services with `app.kubernetes.io/name=curvine` and
   `app.kubernetes.io/component=master|worker|transfer`.
3. Apply either `servicemonitors.yaml` or `podmonitors.yaml`. Applying both can
   create duplicate series.
4. Apply `prometheusrules.yaml` if Prometheus Operator rule discovery is enabled.
5. Configure `web.observability.prometheus.url`, select `auto` or `prometheus`
   mode, then open `/operations` to verify reachability and complete coverage.

The monitor examples inject the mandatory `component` target label consumed by
curvine-web queries. Adjust namespaces, selectors, TLS, authentication and the
Prometheus release label for the target cluster before applying them.
