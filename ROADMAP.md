# Curvine Roadmap

This roadmap lists the areas the maintainers are focusing on. It is grouped by priority rather than dates: **Now** is in active development, **Next** is planned once current work lands, and **Later** is accepted in direction but not yet scheduled.

Each item links to its tracking issue. Priorities change as we learn from users. If something here matters to you, or something is missing, comment on the issue or start a thread in [GitHub Discussions](https://github.com/CurvineIO/curvine/discussions).

The maintainers review this file every quarter. Changes follow the normal pull request process described in [GOVERNANCE.md](GOVERNANCE.md).

## Now

### Data integrity and write-path reliability

Make sure data is never silently lost or corrupted, even under worker failures, reboots, and slow storage.

- End-to-end block checksums on the worker read and write paths ([#1921](https://github.com/CurvineIO/curvine/issues/1921), [#1991](https://github.com/CurvineIO/curvine/issues/1991), [#1958](https://github.com/CurvineIO/curvine/issues/1958))
- Quarantine uncertain SPDK writes and keep quarantined blocks out of reads and replication ([#2011](https://github.com/CurvineIO/curvine/issues/2011), [#2014](https://github.com/CurvineIO/curvine/issues/2014), [#2015](https://github.com/CurvineIO/curvine/issues/2015), [#2018](https://github.com/CurvineIO/curvine/issues/2018))
- Durable write watermark for worker staging blocks ([#1994](https://github.com/CurvineIO/curvine/issues/1994))
- Pipeline recovery on replica write failure instead of returning EIO ([#1965](https://github.com/CurvineIO/curvine/issues/1965))
- Fixes from FUSE-over-object-storage fault-injection testing ([#1810](https://github.com/CurvineIO/curvine/issues/1810))

### Write flow control

Keep clients and workers stable under heavy concurrent writes.

- Bounded FUSE write queue and client byte-level write budget ([#1993](https://github.com/CurvineIO/curvine/issues/1993))
- Client write throttling and write-path queue metrics ([#1992](https://github.com/CurvineIO/curvine/issues/1992))

### S3 gateway correctness

- Streamed PUT Object bodies with incremental SHA-256 verification ([#1990](https://github.com/CurvineIO/curvine/issues/1990))
- Validate CompleteMultipartUpload part lists before assembly ([#1989](https://github.com/CurvineIO/curvine/issues/1989))

## Next

### Metadata scalability

Support larger namespaces and higher metadata QPS for AI agent platforms.

- Shorter master write-lock critical paths ([#1321](https://github.com/CurvineIO/curvine/issues/1321), [#1931](https://github.com/CurvineIO/curvine/issues/1931))
- Lock-scope reduction, subtree sharding, and distributed masters ([#1725](https://github.com/CurvineIO/curvine/issues/1725))
- Follower masters serving whitelisted metadata reads ([#1617](https://github.com/CurvineIO/curvine/issues/1617))
- Faster master restart at 100M+ files ([#772](https://github.com/CurvineIO/curvine/issues/772))
- Asynchronous delete and bounded trash purge ([#1824](https://github.com/CurvineIO/curvine/issues/1824), [#1828](https://github.com/CurvineIO/curvine/issues/1828))

### POSIX compatibility

Close the remaining gaps found by LTP and xfstests.

- LTP 20210524 follow-ups: locking, xattr, mmap, inotify, and directory-entry semantics ([#808](https://github.com/CurvineIO/curvine/issues/808))
- Kernel dentry and readdir cache invalidation ([#840](https://github.com/CurvineIO/curvine/issues/840), [#1505](https://github.com/CurvineIO/curvine/issues/1505))
- POSIX permission checks on namespace-changing operations ([#1111](https://github.com/CurvineIO/curvine/issues/1111))

### Operations on Kubernetes

- Rolling cluster upgrades without downtime ([#869](https://github.com/CurvineIO/curvine/issues/869))
- etcd-based service discovery in the runtime ([#1784](https://github.com/CurvineIO/curvine/issues/1784))
- Cloud-native master deployment ([#647](https://github.com/CurvineIO/curvine/issues/647))
- End-to-end metrics across FUSE, master, and worker ([#897](https://github.com/CurvineIO/curvine/issues/897))

## Later

- Range-based (partial) object caching instead of whole-object caching ([#950](https://github.com/CurvineIO/curvine/issues/950))
- Stateless masters backed by an external distributed database ([#951](https://github.com/CurvineIO/curvine/issues/951))
- Migrate the internal RPC protocol to gRPC ([#1726](https://github.com/CurvineIO/curvine/issues/1726))
- io_uring as an async I/O backend on Linux ([#791](https://github.com/CurvineIO/curvine/issues/791))
- NFS protocol support ([#782](https://github.com/CurvineIO/curvine/issues/782))
- Peer-to-peer data transfer between clients ([#361](https://github.com/CurvineIO/curvine/issues/361))

## Ecosystem integrations

We want Curvine to work out of the box with the tools AI teams already use. We welcome contributions and design proposals for:

- LLM inference engines and KV cache layers (vLLM, LMCache, SGLang)
- Kubernetes data orchestration ([Fluid, #373](https://github.com/CurvineIO/curvine/issues/373))
- Training and data processing frameworks (PyTorch, Ray, Spark)

If you are working on one of these, open an issue so we can track it here.
