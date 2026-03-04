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
| `--update-code` | Run `git pull` on current branch before starting |
| `--run-once` | Trigger one dailytest, wait for it to finish, then exit |

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
