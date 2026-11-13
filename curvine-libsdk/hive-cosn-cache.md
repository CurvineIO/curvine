# Hive COSN transparent acceleration (no table LOCATION change)

This guide deploys the Curvine COSN cache proxy so Hive tables keep their
`cosn://...` LOCATIONs. Jobs opt in per-job (or per-session) instead of
changing the table metadata: reads of paths mounted in Curvine are served by
Curvine, everything else keeps going to COS.

## How it works

`io.curvine.CosnProxyFileSystem` (shipped in `curvine-hadoop` jar) replaces
the native COSN implementation as `fs.cosn.impl`:

| Operation | Path mounted in Curvine | Path not mounted |
|-----------|------------------------|------------------|
| Read      | Curvine (falls back to COSN on miss when `fs.cv.enable_fallback_read_ufs=true`) | COSN |
| Write     | COSN (never through Curvine) | COSN |

The switch can be toggled per job with `fs.cv.proxy.enabled`, and globally
through ZooKeeper (`fs.cv.proxy.zk.server`) so a bad rollout can be killed
cluster-wide without restarting jobs.

## Prerequisites

1. A running Curvine cluster with COSN mounts. Mount the exact Hive LOCATION
   prefix - keep the short bucket name when Hive uses it, and pass `cosn.appid`
   so hadoop-cos / the CHDFS trsf adapter resolve the real `bucket-appid` on the
   Java side (add `-c fs.cosn.trsf.fs.ofs.use.short.bucketname=true` for
   metadata-accelerated buckets):

   ```bash
   cv mount cosn://mybucket/warehouse/db/hot_table/ /cache/db/hot_table \
     --provider cosn \
     -c cosn.secretId=AKIDxxx \
     -c cosn.secretKey=yyy \
     -c cosn.region=ap-guangzhou \
     -c cosn.appid=1250000000 \
     -c fs.cosn.trsf.fs.ofs.use.short.bucketname=true
   ```

   See [curvine-ufs-cosn README](../../crates/adapters/curvine-ufs-cosn/README.md)
   for the full mount property list.

2. On every compute node:
   - `curvine-hadoop` jar (with native `libcurvine_sdk` bundled) on the
     Hadoop/Spark classpath
   - `hadoop-cos` jar (the COSN connector already required by existing
     `cosn://` tables)

## Configuration

Add to `core-site.xml` (or ship with the job):

```xml
<property>
    <name>fs.cosn.impl</name>
    <value>io.curvine.CosnProxyFileSystem</value>
</property>

<!-- Curvine cluster connection (cluster name is "default" by default) -->
<property>
    <name>fs.cv.impl</name>
    <value>io.curvine.CurvineFileSystem</value>
</property>
<property>
    <name>fs.cv.default.master_addrs</name>
    <value>cv-master-0.svc.cluster.local:8995,cv-master-1.svc.cluster.local:8995</value>
</property>
<property>
    <name>fs.cv.enable_unified_fs</name>
    <value>true</value>
</property>
<property>
    <name>fs.cv.enable_rust_read_ufs</name>
    <value>false</value>
</property>

<!-- Transparent-acceleration proxy switches -->
<property>
    <name>fs.cv.proxy.enabled</name>
    <value>true</value>
</property>
<property>
    <name>fs.cv.proxy.cluster.name</name>
    <value>default</value>
</property>
```

Optional global kill switch - when the ZK node value is `1` the proxy is
active, anything else disables Curvine reads:

```xml
<property>
    <name>fs.cv.proxy.zk.server</name>
    <value>zk1:2181,zk2:2181</value>
</property>
<property>
    <name>fs.cv.proxy.zk.path</name>
    <value>/curvine/proxy_enabled</value>
</property>
<property>
    <name>fs.cv.proxy.timeout</name>
    <value>60s</value>
</property>
```

## Per-job usage

Spark:

```bash
spark-submit --conf spark.hadoop.fs.cosn.impl=io.curvine.CosnProxyFileSystem ...
```

Tez / Hive on MR: set on the job configuration:

```bash
hive -hiveconf fs.cosn.impl=io.curvine.CosnProxyFileSystem \
     -hiveconf fs.cv.default.master_addrs=cv-master-0:8995
```

Disable acceleration for a single job (reads go straight to COSN):

```bash
spark-submit --conf spark.hadoop.fs.cv.proxy.enabled=false ...
```

## Verifying

- Curvine reads: the job logs contain `Failed to open curvine file ...` only on
  real errors; hits are silent. Check Curvine metrics (`cv fsstat`) for read
  traffic, or `cv ls` on the mount point for cached data.
- A quick smoke test - read a mounted file through the proxy and watch block
  metrics grow in the Curvine dashboard; then disable with
  `fs.cv.proxy.enabled=false` and confirm reads still work directly from COSN.
