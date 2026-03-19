# Curvine Regression Test Server

Regression test server and Portal for Curvine (build-server). Test results are written under `curvine-tests/regression_result/` (gitignored).

## Local Run

From the **project root** (so that `curvine-tests/regression` and project code are visible):

```bash
cd /path/to/curvine
python3 curvine-tests/regression/build-server.py
```

Open the Portal: **http://localhost:5002/result**

| Option | Description |
|--------|-------------|
| `--port 5002` | Server port (default 5002) |
| `--project-path /path` | Override project root (default: current dir) |
| `--results-dir /path` | Override results dir (default: `curvine-tests/regression_result/`) |
| `--nextest-profile NAME` | Nextest profile for unittest (e.g. `ci-no-ufs`). See [Nextest profile](#nextest-profile) below. |
| `--update-code` | Run `git pull` on current branch before starting |
| `--run-once` | Trigger one dailytest, wait for it to finish, then exit |

#### Nextest profile

Unit tests are run with `cargo nextest`. You can choose which tests run by setting a **nextest profile**:

| How to set | Example | Use case |
|------------|---------|----------|
| `--nextest-profile NAME` | `--nextest-profile ci-no-ufs` | Use profile when starting build-server |
| Env `NEXTEST_PROFILE` | `NEXTEST_PROFILE=ci-no-ufs python3 build-server.py` | Same as above (also applies when running `daily_regression_test.sh` directly) |
| Env `NEXTEST_CI_NO_UFS=1` | `NEXTEST_CI_NO_UFS=1 python3 build-server.py` | Shortcut for profile `ci-no-ufs` (skip UFS-dependent tests, e.g. CI without MinIO) |

Profiles are defined in `curvine-tests/regression/nextest.toml`. Built-in:

- **default** – run all tests (requires UFS/MinIO for some tests).
- **ci-no-ufs** – skip UFS-dependent tests (`write_cache_test`, `ufs_test`, `fallback_read_test`), for CI without MinIO.

## Portal Dashboard

The dashboard shows all modules with **Run / Cancel** buttons.

| Module | What it does |
|--------|-------------|
| **Build (make all)** | Build the project with all UFS drivers |
| **Daily Test (Full)** | Run unittest → coverage → fio → fuse in sequence |
| **Regression (Unittest)** | Unit tests only (`cargo test`) |
| **Coverage** | Unit tests with llvm coverage report |
| **FUSE** | FUSE filesystem tests (needs a running cluster) |
| **FIO** | FIO performance tests (needs a running cluster) |
| **LTP** | POSIX compliance tests via LTP (needs `/opt/ltp` and cluster) |

> **Dailytest** does not include LTP by default. Trigger LTP separately via the Portal or `/ltp/run` API.  
> LTP requires LTP installed at `/opt/ltp` on the host or in the container.

Test results are saved under `curvine-tests/regression_result/<timestamp>/`.

## Docker

Image is based on `ghcr.io/curvineio/curvine-compile:rocky9`.

Build (from repo root):

```bash
docker build -f curvine-tests/Dockerfile -t curvine-tests .
```

Run with current project mounted (Portal keeps running):

```bash
docker run -v "$(pwd)":/workspace -p 5002:5002 curvine-tests
```

One-shot: trigger dailytest once and exit (e.g. for CI):

```bash
docker run -v "$(pwd)":/workspace curvine-tests --run-once
```

To use a nextest profile (e.g. skip UFS tests in CI): `--nextest-profile ci-no-ufs` or `-e NEXTEST_PROFILE=ci-no-ufs`.

**Optional: mount dependency caches** to avoid re-downloading on every build:

```bash
docker run \
  -v "$(pwd)":/workspace \
  -v "$HOME/.cargo/registry":/root/.cargo/registry \
  -v "$HOME/.m2":/root/.m2 \
  -p 5002:5002 \
  curvine-tests
```

| Mount | Purpose |
|-------|---------|
| `~/.cargo/registry` | Reuse downloaded Rust crates across builds |
| `~/.m2` | Reuse downloaded Maven JARs across builds |

**If running FUSE tests**, the container needs privileged mode to perform FUSE mounts:

```bash
docker run \
  -v "$(pwd)":/workspace \
  --privileged \
  --device /dev/fuse \
  -p 5002:5002 \
  curvine-tests
```

| Flag | Why it's needed |
|------|----------------|
| `--privileged` | Grants all capabilities required for `mount` (FUSE needs more than just `SYS_ADMIN` on some kernels) |
| `--device /dev/fuse` | Expose the host FUSE device to the container |

> Without `--privileged`, FUSE mount fails with `Operation not permitted (os error 1)` even if `--cap-add SYS_ADMIN` is set. This is a known limitation of running FUSE inside Docker.

**`CURVINE_MASTER_HOSTNAME`**: the hostname tests use to connect to the Curvine master. Defaults to `localhost`. Must match the `journal_addr` host in `curvine-cluster.toml`.

```bash
docker run \
  -v "$(pwd)":/workspace \
  -e CURVINE_MASTER_HOSTNAME=192.168.1.100 \
  -p 5002:5002 \
  curvine-tests
```
