# ListObjectsV2 traversal and regression tests

## Semantics

The gateway traverses filesystem directories to present a flat object-key space.
Without a delimiter it returns complete nested keys, not directory entries.
`Prefix` is a string prefix, including partial filenames. Missing prefixes and
empty directories produce empty results; a missing bucket remains an error.

With a nonempty `Delimiter`, matching keys are rolled up at the first delimiter
after `Prefix`. Deduplicated `CommonPrefixes` and individual objects share the
`MaxKeys` budget. The default and maximum page size are 1000; zero returns an
empty, nontruncated page. Negative or malformed limits are rejected.

Pages are ordered by UTF-8 key bytes. A directory is ordered as its key plus `/`,
so traversal handles names such as `a-`, `a/file`, and `a0` in the right order.
Only the existing reserved `.curvine-s3-*` namespace is filtered; ordinary dot
files remain visible. A prefix is never directly interpolated into a filesystem
path: traversed directory names come from filesystem listings.

Continuation tokens are versioned, URL-safe opaque cursors bound to the bucket,
prefix and delimiter. Treat them as opaque; they are not credentials or signed
authorization grants. Changing these listing parameters requires a fresh listing.
A continuation token takes precedence over `StartAfter`; changing `MaxKeys` is
allowed. Tokens from the old raw-key implementation are not accepted, so restart
in-flight listings after upgrading. `encoding-type=url` encodes key-related XML
fields, not the opaque continuation tokens.

## Cost and consistency limits

Traversal sorts one directory at a time and stops after finding one logical
entry beyond the requested page. Prefix and cursor checks skip irrelevant or
already-consumed subtrees. It does not recursively collect and sort the entire
bucket before returning a page.

The filesystem's current `list_status` API returns an entire directory. A large
single directory still requires a full directory read and sort. Memory includes
remaining entries of active ancestor directories, not only `MaxKeys` objects.
Later pages may relist directories, and sparse matches can still require a large
scan. This is not an O(MaxKeys)-memory or constant-time backend pagination claim.
A fully bounded per-directory scan requires a future filesystem/RPC cursor API.

Pagination is not a snapshot under concurrent object creation/deletion. Filesystem
errors are propagated rather than silently returning an apparently complete page.
This change does not redesign filesystem-backed S3 key/empty-directory semantics.

## Tests

From the repository root, with the required Rust/native dependencies installed:

```bash
cargo test --locked --release -p curvine-s3-gateway
cargo fmt -p curvine-s3-gateway --check
```

The Rust tests compare traversal with an independent flat-key projection across
prefixes, delimiters, start markers and page sizes. They also cover cursor context,
zero/max limits, staging filtering, error propagation, subtree pruning, early
termination and the serialized HTTP response, including URL encoding.

The handoff E2E runner starts and stops its own isolated local cluster:

```bash
python curvine-s3-gateway/src/test/e2e_local.py \
  --suite listing --output-dir /tmp/curvine-listing-UNIQUE

python curvine-s3-gateway/src/test/e2e_local.py \
  --suite full --output-dir /tmp/curvine-full-UNIQUE
```

Each output directory must not already exist. The default suite is `full`.
Both suites treat the recursive and delimiter regressions as required tests, not
optional compatibility limitations. `listing` is a focused #2091 suite, not a
claim that unrelated S3 compatibility issues are resolved. In `full`, the strict
metadata test remains a required test with its known #2094 failure, and presigned
GET remains a separately reported #2093 limitation until those issues are fixed.

When testing an uncommitted working tree, the reported source HEAD alone does not
identify the fix. The runner additionally records gateway Rust source hashes and
its own hash; preserve these together with build logs and binary hashes. Do not
publish raw cluster logs, credentials or signed URLs.
