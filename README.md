<div align=center>
<img src="https://raw.githubusercontent.com/CurvineIO/curvine-doc/refs/heads/main/static/img/curvine_logo.svg",  width="180" height="200">
</div>

![curvine-font-dark](https://raw.githubusercontent.com/CurvineIO/curvine-doc/refs/heads/main/static/img/curvine_font_dark.svg#gh-light-mode-only)
![curvine-font-light](https://raw.githubusercontent.com/CurvineIO/curvine-doc/refs/heads/main/static/img/curvine_font_white.svg#gh-dark-mode-only)

<p align="center">
  English | 
  <a href="https://github.com/CurvineIO/curvine/blob/main/README_zh.md">简体中文</a> |
  <a href="https://readme-i18n.com/CurvineIO/curvine?lang=de">Deutsch</a> |
  <a href="https://readme-i18n.com/CurvineIO/curvine?lang=es">Español</a> |
  <a href="https://readme-i18n.com/CurvineIO/curvine?lang=fr">français</a> |
  <a href="https://readme-i18n.com/CurvineIO/curvine?lang=ja">日本語</a> |
  <a href="https://readme-i18n.com/CurvineIO/curvine?lang=ko">한국어</a> |
  <a href="https://readme-i18n.com/CurvineIO/curvine?lang=pt">Português</a> |
  <a href="https://readme-i18n.com/CurvineIO/curvine?lang=ru">Русский</a>
</p>

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Rust](https://img.shields.io/badge/Rust-1.86%2B-orange)](https://www.rust-lang.org)

> **Curvine: AI-Native & Cloud-Native File System** — A high-performance POSIX file semantic layer built on top of cloud object storage, with an integrated multi-tier distributed cache, designed from the ground up for large-scale AI workloads and AI Agent platforms.

> **Name Origin** — "Curvine" is derived from *"Curvature Engine"*, the faster-than-light propulsion device in Liu Cixin's sci-fi novel *The Three-Body Problem*. It symbolizes the project's pursuit of extreme acceleration for data access.

---

## 📚 Documentation Resources

For more detailed information, please refer to:

- [Official Documentation](https://curvineio.github.io/docs/Overview/instroduction)
- [Quick Start](https://curvineio.github.io/docs/Deploy/quick-start)
- [User Manuals](https://curvineio.github.io/docs/category/user-manuals)
- [Detailed Usage Instructions](https://curvineio.github.io/blog/2025/09/28/user-guide/)
- [Benchmark](https://curvineio.github.io/docs/category/benchmark)
- [DeepWiki](https://deepwiki.com/CurvineIO/curvine)
- [Commit convention](COMMIT_CONVENTION.md)
- [Contribute guidelines](CONTRIBUTING.md)

## Roadmap 2026
![Evolution from Distributed Cache to AI Agent-Native Infrastructure ](https://github.com/CurvineIO/curvine/discussions/549)

## 🎯 Why Curvine

The AI infrastructure landscape is undergoing a fundamental architectural shift: from a centralized model where a single large model instance serves all requests, to a distributed model where **tens of thousands of Agent instances run independently**. Each Agent is not a stateless HTTP handler — it is a stateful process with its own working directory, persistent context files, `node_modules`, `.git` history, and a need for an isolated POSIX workspace.

This "massive small stateful instances" workload pattern is fundamentally different from traditional stateful applications, and it exposes the hard limits of existing storage options:

| Storage Option | Limitation for Agent-at-Scale |
|----------------|-------------------------------|
| Block storage (e.g. EBS) | Per-node volume attach limit (e.g. 28 on most Nitro instances), single-AZ binding, slow cross-AZ failover |
| Managed NFS (e.g. EFS) | Access Point creation is throttled by cloud API rate limits; large-scale parallel provisioning becomes a bottleneck |
| Object storage (e.g. S3) | No POSIX semantics — no in-place mutation, no atomic rename, no consistent directory listing |

**Curvine closes this gap.** It layers a distributed POSIX file system over cloud object storage, so that:

- Provisioning a PVC is just `mkdir` on a distributed file system — **millisecond-level, no cloud control-plane API calls, no rate limits**.
- Each Agent Pod gets an isolated file system view via the native CSI driver, with the same logical isolation as block storage but **without the per-node attach-count ceiling**.
- Pods can be scheduled across nodes and AZs freely; data stays reachable because it lives in the shared Curvine namespace, not a node-bound volume.

## 🤖 AI Agent Use Case

Curvine is purpose-built to back large-scale AI Agent platforms on Kubernetes. In a production validation on **Amazon EKS**, Curvine sustained **10,000 independent stateful Pods** with reliable persistent storage:

| Metric | Result |
|--------|--------|
| Provisioned PVCs | 10,000 — all `Bound`, zero `Pending`, zero `Failed` |
| Running Pods | 10,000 — all `Running`, zero `CrashLoopBackOff` |
| Storage cluster footprint | **1 Master + 3 Workers = 4 core Pods** serving 10,000 PVCs |
| Pod density per node | ~100 Agent Pods per `r6g.4xlarge` node (vs. ~28 with EBS) |
| Node resource utilization | CPU 88% / Memory 98% (compute, not storage, is the bound) |
| Provisioning latency | Milliseconds (local `mkdir`, no cloud API) |
| Durability | Data survives Pod restart and cross-node rescheduling |

> Read the full story: **[AI Agent 存储选型：Curvine 如何在 EKS 上支撑万级 Agent 运行](https://aws.amazon.com/cn/blogs/china/ai-agent-storage-curvine-how-to-eks-agent/)**


## 🚀 Core Features

- **AI-Native Positioning**: First-class support for AI training acceleration and AI Agent cloud-native storage as primary use cases, not an afterthought.
- **Multi-Cloud Object Storage Backend**: Compatible with object storage services from multiple cloud providers as the durable underlying layer, enabling transparent data migration across vendors.
- **Cloud-Native Kubernetes Integration**: Native CSI driver enables dynamic PVC provisioning, `Immediate` binding, volume expansion, and Helm-based cluster deployment.
- **Multi-Tier Cache**: Memory → SSD → HDD automatic tiering; hot data is transparently promoted to faster tiers.
- **Full POSIX Semantics via FUSE**: A high-performance FUSE layer presents distributed cached data as a local file system — `open`, `read`, `write`, `seek`, `rename`, `list` — enabling tools like Vite, `inotify`/`fswatch`, and `git` to work unmodified.
- **S3 & HDFS Protocol Compatibility**: Read/write through both S3 and HDFS interfaces for seamless integration with AI and big-data ecosystems.
- **Extreme Performance**: Rust core with Tokio async runtime, zero-copy data paths, and a GC-free memory model — ~100μs-class latency and 100K+ stable QPS.
- **Massive Metadata Capacity**: A single cluster supports **5 billion** small files, absorbing the aggregate metadata pressure of tens of thousands of Agents.
- **Metadata Independence**: Curvine's file metadata path maps **1:1** to the underlying S3 object path. Even if the Curvine service is unavailable, objects on S3 keep their original structure and remain independently accessible — fast and simple recovery.
- **Raft Consensus**: Master metadata is replicated via Raft for consistency and high availability.
- **Observability**: Built-in metrics system and Web UI for per-component performance monitoring.

## 📈 Use Cases

![use_case](https://raw.githubusercontent.com/CurvineIO/curvine-doc/refs/heads/main/docs/1-Overview/img/curvine-scene.png)

- **Case 1 — AI Agent Platform Storage**: Backing tens of thousands of stateful Agent Pods on Kubernetes with isolated POSIX workspaces, millisecond provisioning, and no per-node volume limits.
- **Case 2 — LLM Training Acceleration**: Caching training datasets and checkpoints close to GPU nodes to shorten training cycles.
- **Case 3 — LLM Model Distribution Acceleration**: Fast multi-region model artifact distribution through the distributed cache.
- **Case 4 — Multimodal Data Lake Access Acceleration**: POSIX access over multimodal lakes without copying data locally.
- **Case 5 — OLAP Query Acceleration**: Accelerating compute-storage separated OLAP engines with a hot data cache.
- **Case 6 — Multi-Cloud Data Caching**: A unified cache layer across multi-cloud object storage backends.

## 📦 System Requirements

- Rust 1.86+
- Linux or macOS (Limited support on Windows)
- FUSE library (for file system functionality)

**Officially Supported Linux Distributions**

| OS Distribution     | Kernel Requirement | Tested Version | Dependencies |
|---------------------|--------------------|----------------|--------------|
| ​**CentOS 7**​      | ≥3.10.0            | 7.6            | fuse2-2.9.2  |
| ​**CentOS 8**​      | ≥4.18.0            | 8.5            | fuse3-3.9.1  |
| ​**Rocky Linux 9**​ | ≥5.14.0            | 9.5            | fuse3-3.10.2 |
| ​**RHEL 9**​        | ≥5.14.0            | 9.5            | fuse3-3.10.2 |
| ​**Ubuntu 22**​      | ≥5.15.0            | 22.4           | fuse3-3.10.5 |

## 🛠 Build Instructions

This project requires the following dependencies. Please ensure they are installed before proceeding:

### 📋 Prerequisites

- ​**GCC**: version 10 or later ([Installation Guide](https://gcc.gnu.org/install/))
- ​**Rust**: version 1.86 or later ([Installation Guide](https://www.rust-lang.org/tools/install))
- ​**Protobuf**: version 3.x
- ​**Maven**: version 3.8 or later ([Install Guide](https://maven.apache.org/install.html))
- ​**LLVM**: version 12 or later ([Installation Guide](https://llvm.org/docs/GettingStarted.html))
- ​**FUSE**: libfuse2 or libfuse3 development packages
- ​**JDK**: version 1.8 or later (OpenJDK or Oracle JDK)
- ​**npm**: version 9 or later ([Node.js Installation](https://nodejs.org/))
- ​**Python**: version 3.7 or later ([Installation Guide](https://www.python.org/downloads/))

You can either:
1. Use the pre-configured `curvine-docker/compile/Dockerfile_rocky9` to build a compilation image
2. Reference this Dockerfile to create a compilation image for other operating system versions
3. We also supply `curvine/curvine-compile` image on dockerhub

### 🚀 Build Steps (Linux - Ubuntu/Debian example)
Using make to build:

```bash
# Build all modules
make all

# Build core modules only: server client cli
make build ARGS="-p core"

# Build fuse and core modules
make build ARGS="-p core -p fuse"
```

Using build.sh directly:

```bash
# Build all modules
sh build/build.sh 

# Display command help 
sh build/build.sh -h

# Build core modules only: server client cli
sh build/build.sh -p core

# Build fuse and core modules
sh build/build.sh -p core -p fuse
```

Building Docker images:

```bash
# or use curvine-compile:latest docker images to build
make docker-build

# or use curvine-compile:build-cached docker images to build, this image already cached most dependency crates
make docker-build-cached
```

After successful compilation, target file will be generated in the build/dist directory. This file is the Curvine installation package that can be used for deployment or building images.

### 🖥️  Start a single-node cluster
```bash
cd build/dist

# Start the master node
bin/curvine-master.sh start

# Start the worker node
bin/curvine-worker.sh start
```

Mount the file system
```bash
# The default mount point is /curvine-fuse
bin/curvine-fuse.sh start
```

View the cluster overview:
```bash
bin/cv report
```

Access the file system using compatible HDFS commands:
```bash
bin/cv fs mkdir /a
bin/cv fs ls /
```

Access Web UI：
```
http://your-hostname:9000
```

Curvine uses TOML - formatted configuration files. An example configuration is located at conf/curvine-cluster.toml. The main configuration items include:

- Network settings (ports, addresses, etc.)
- Storage policies (cache size, storage type)
- Cluster configuration (number of nodes, replication factor)
- Performance tuning parameters

Stop the cluster:

```bash
# Stop the FUSE mount
bin/curvine-fuse.sh stop

# Stop the worker and master nodes
bin/curvine-worker.sh stop
bin/curvine-master.sh stop
```

## 🏗️ Architecture Design

Curvine adopts a Master-Worker architecture:

- **Master Node**: Responsible for metadata management, worker node coordination, and load balancing. Uses the Raft consensus algorithm to guarantee metadata consistency and high availability.
- **Worker Node**: Responsible for data caching and service. Supports multi-tier cache (memory, SSD, HDD) with automatic hot-data promotion.
- **Client**: Communicates with the Master and Worker nodes via RPC; accesses data through FUSE (POSIX), S3, or HDFS-compatible interfaces.

The core idea: layer a distributed file system cache over cloud object storage, exposing full POSIX semantics upward and using object storage as the durable persistence layer downward. For Kubernetes workloads, the native CSI driver mounts the file system directly as a PVC — provisioning does not call any external cloud API, it simply creates a directory on the distributed file system, which completes in milliseconds.

## 📈 Performance

Curvine performs excellently in high-concurrency scenarios and supports:

- High-throughput data read and write
- ~100μs-class latency and 100K+ stable QPS
- Large-scale concurrent connections
- 5 billion small files per cluster

## Contributing
Please read Curvine [Contribute guidelines](CONTRIBUTING.md)

## 📜 License
Curvine is licensed under the ​**​[Apache License 2.0](LICENSE)​**.

## Star History

[![Star History Chart](https://api.star-history.com/svg?repos=CurvineIO/curvine&type=Date)](https://www.star-history.com/#CurvineIO/curvine&Date)
