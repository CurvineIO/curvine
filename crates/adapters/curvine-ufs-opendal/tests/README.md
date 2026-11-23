# curvine-ufs-opendal tests

Integration tests for the OpenDAL UFS adapter. Cargo builds each top-level `tests/*.rs`
file as its own test target.

| Target | Needs | What it covers |
| ------ | ----- | -------------- |
| `s3_blackhole` | `--features opendal-s3` (declared as `required-features` in `Cargo.toml`) | Local stand-in endpoints that stall or answer scripted responses: stat, open, create, read, write, close and rename end with a timeout error within `opendal.operation_timeout` / `opendal.commit_timeout` instead of hanging (issue #1815); `create` pre-check round trips; multipart uploads are aborted when a part fails or the write is cancelled |
| `s3_sts_wire` | `--features opendal-s3` | A loopback S3 stand-in records the signed requests: an STS mount sends `x-amz-security-token`, a static mount does not, and an `auto` mount without source credentials fails with a credential error |
| `s3_fs` | A real S3-compatible service and `testing/s3.toml` (see below); without them it prints a skip message and passes | `mkdir`, `list_status`, write and read back a file with a checksum, against `s3://curvine-test/cv-fs-test/` |

`s3_blackhole` and `s3_sts_wire` need no network or credentials; they bind `127.0.0.1` only.

## Run

The build job of `.github/workflows/build.yml` runs all of them:

```bash
cargo test -p curvine-ufs-opendal --features opendal-s3 --tests
```

One target:

```bash
cargo test -p curvine-ufs-opendal --features opendal-s3 --test s3_blackhole
cargo test -p curvine-ufs-opendal --features opendal-s3 --test s3_sts_wire
cargo test -p curvine-ufs-opendal --features opendal-s3 --test s3_fs -- --nocapture
```

## S3 service for `s3_fs`

Any S3-compatible service works, for example MinIO:

```bash
docker run -p 9000:9000 -p 9001:9001 \
  -e "MINIO_ROOT_USER=minioadmin" \
  -e "MINIO_ROOT_PASSWORD=minioadmin" \
  minio/minio server /data --console-address ":9001"
```

Create the bucket `curvine-test`, then write the mount options to
`crates/adapters/curvine-ufs-opendal/testing/s3.toml` (Cargo runs the tests from the crate
directory; `../testing/s3.toml` is also tried). The file is flattened into `s3.*` keys and only
string values are kept, so quote every value:

```toml
[s3]
endpoint_url = "http://127.0.0.1:9000"
region_name = "us-east-1"
force.path.style = "true"

[s3.credentials]
access = "minioadmin"
secret = "minioadmin"
```

`testing/` is in `.gitignore`, so the file stays local.

## HDFS tests

`hdfs/hdfs_integration_test.rs` and `hdfs/hdfs_mount_integration_test.rs` sit in a
subdirectory without a `main.rs` and have no `[[test]]` entry, so Cargo does not build them and
the `--test hdfs_integration_test` commands in their headers fail with "no test target". They
are kept for reference until they are registered as test targets.
