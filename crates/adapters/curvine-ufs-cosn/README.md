# curvine-ufs-cosn

Native UFS adapter for Tencent COS via the Hadoop **COSN** connector (`hadoop-cos` jar) and **libhdfs**.

Scheme: `cosn://<bucket>-<appid>/...`

## Build

```bash
# Workspace / distribution
make build ARGS='--ufs cosn'
# or
./build/build.sh --ufs cosn

# Crate-only check (needs libhdfs + JVM on the link path)
cargo check -p curvine-ufs-cosn --features cosn
```

Runtime requirements:

- `libhdfs.so` (Hadoop 3.1.x C API; this crate uses `hdfs-sys` feature `hdfs_3_1`)
- `hadoop-cos` (or equivalent) on the Hadoop classpath
- JVM available to libhdfs

## Mount properties

Per-mount credentials are passed through `hdfsBuilderConfSetStr` with
`hdfsBuilderSetForceNewInstance`, so multiple mounts can use different accounts.

Short form (`cv mount -c`):

| Key | Required | Maps to |
|-----|----------|---------|
| `cosn.secretId` | yes | `fs.cosn.userinfo.secretId` |
| `cosn.secretKey` | yes | `fs.cosn.userinfo.secretKey` |
| `cosn.region` | yes | `fs.cosn.bucket.region` |
| `cosn.endpoint_suffix` | no | `fs.cosn.bucket.endpoint_suffix` |
| `cosn.appid` | no | `fs.cosn.userinfo.appid` — when set, libhdfs connects as `cosn://<bucket>-<appid>` even if the mount URI uses the short Hive name `cosn://<bucket>/` |
| `cosn.user` | no | Hadoop user (`hdfsBuilderSetUserName`) |

Hadoop keys may be used directly instead of the short form.

Example (URI already `bucket-appid`):

```bash
cv mount cosn://mybucket-1250000000/ /mnt/cosn \
  -c cosn.secretId=AKIDxxx \
  -c cosn.secretKey=yyy \
  -c cosn.region=ap-guangzhou \
  --provider cosn
```

The mount URI may use the short Hive bucket name (`cosn://mybucket/...`). Pass appid so native HeadBucket still uses `mybucket-1250000000`:

```bash
cv mount cosn://mybucket/warehouse/db/hot_table/ /cache/db/hot_table \
  --provider cosn \
  -c cosn.secretId=AKIDxxx \
  -c cosn.secretKey=yyy \
  -c cosn.region=ap-guangzhou \
  -c cosn.appid=1250000000 \
  -c fs.cosn.trsf.fs.ofs.use.short.bucketname=true
```

## Limitations

- **Append is not supported** (COSN buckets in this deployment do not enable append).
- Directory rename is supported via native libhdfs (`FsKind::Cosn` reports `support_rename`).

## Integration tests

Ignored by default (need a real bucket):

```bash
export COSN_BUCKET=mybucket-1250000000
export COSN_SECRET_ID=...
export COSN_SECRET_KEY=...
export COSN_REGION=ap-guangzhou
# Optional: confine all test objects under this bucket prefix
export COSN_PREFIX=tmp/curvine-it

# Adapter-level (no Curvine cluster)
cargo test -p curvine-ufs-cosn --features cosn --test integration -- --ignored

# Through UfsFileSystem (needs full test harness / cluster deps)
cargo test -p curvine-tests --features cosn --test cosn_ufs_test -- --ignored
```
