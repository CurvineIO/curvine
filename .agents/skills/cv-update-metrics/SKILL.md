---
name: cv-update-metrics
description: Update the checked-in Curvine Grafana dashboards when Prometheus metrics are added, renamed, removed, or change meaning. Use when changing master, worker, client, or transfer metrics, or when editing etc/grafana dashboard JSON.
---

# Update Grafana metrics dashboards

A Prometheus metric change is not done until the checked-in Grafana model matches it. Do this in the same change as the exporter.

## Dashboards

| File | Scope |
|---|---|
| `etc/grafana/curvine-all.json` | Cluster overview. uid `curvine-overview`. |
| `etc/grafana/curvine-fuse-dashboard.json` | FUSE client triage. Update this file when a `curvine_fuse_*` metric changes. |

Import notes live in `etc/grafana/README.md`.

## Where a panel goes

Keep the existing rows in `curvine-all.json`. Add a panel to the row that already matches the component. Do not create a new row when one of these already fits:

- key posture: online ratio, capacity, namespace size, current throughput
- overview: capacity mix, watermark, throughput, IOPS
- master: process, RPC, Raft journal, directory lock, namespace
- worker: online, disk, capacity, read/write latency
- client SDK, scrape, and process
- transfer: scheduler, task queue, object store, worker execution

Use the template variables already on the dashboard. Do not hard-code a datasource uid, site name, cluster size, core count, or scrape token.

| Variable | Use |
|---|---|
| `$datasource` | Prometheus datasource |
| `$appid` | Application id matcher. Default `.*` |
| `$database`, `$token` | Optional routing labels. Default `.*`. Never commit a real token |
| `$master_instance`, `$worker_instance`, `$transfer_instance` | Instance filters |
| `$interval` | Range interval where a panel already uses it |

`label=~".*"` matches only series that have the label. If a deployment does not inject `__DATABASE__` or `__TOKEN__`, delete those matchers. Do not put the production values back into git.

Keep `group="master"`, `group="worker"`, or `group="transfer"` aligned with the component that exports the series.

## Panel text

Grafana shows the panel `description` from the information icon. For every panel in `curvine-all.json`, write that text in Chinese with two parts:

- what the metric means
- what an upward or downward trend means

Stat panels describe the current reading. Timeseries panels describe the curve. Do not put an expected target count, worker count, or machine size in the description or the query.

## Checklist

- [ ] Query uses the metric name and labels the process actually exports
- [ ] Panel sits on the matching existing row
- [ ] Description states meaning and trend in Chinese
- [ ] JSON has no site name, token, database name, or fixed cluster size
- [ ] FUSE metric changes also update `curvine-fuse-dashboard.json`
- [ ] `etc/grafana/README.md` still matches the template variables
