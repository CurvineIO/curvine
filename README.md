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

- **Curvine** is a high-performance, concurrent distributed cache system written in Rust, designed for low-latency and high-throughput workloads.

- **Name Origin** The name "Curvine" is derived from the concatenation of the words "Curvature" and "Engine". It refers to an accelerator for spacecraft in the science fiction novel The Three-Body Problem, symbolizing extremely high performance.


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

## Use Case
![use_case](https://raw.githubusercontent.com/CurvineIO/curvine-doc/refs/heads/main/docs/1-Overview/img/curvine-scene.png)

- **Case1**: LLM Training acceleration Acceleration
- **Case2**: LLM Model distribution Acceleration
- **Case3**: Multimodal Data Lake Access Acceleration
- **Case4**: OLAP Engine Query Acceleration in Compute-Storage Separation Scenarios
- **Case5**: Multi-cloud data caching


## 🚀 Core Features

- **Multi-Cloud Support**: Curvine is compatible with object storage services from multiple cloud providers as its underlying storage layer, enabling transparent data migration across different vendors' object storage platforms.
- **Cloud-Native**: Curvine supports CSI-based cloud-native integration with Kubernetes, enabling deployment and management of Curvine clusters via Helm charts.
- **Multi-tir Cache**: Supports multi-tir cache strategies for memory, SSD, and HDD.
- **POSIX Semantic Support**: Curvine delivers comprehensive POSIX semantic compatibility, implementing a high-performance FUSE layer to facilitate the manipulation of distributed cached data as if it were local disk storage.
- **Compatibility with S3 and HDFS Protocols**: The system supports both S3 and HDFS read/write interfaces, facilitating seamless integration with artificial intelligence and big data technology ecosystems.
- **High Performance**: Curvine employs "zero-copy" techniques multiple times throughout its data read/write pipeline and leverages asynchronous operations. Additionally, its core engine is built with Rust, ensuring optimal performance is achieved.
- **Raft Consensus**: Uses the Raft algorithm to ensure the master's data consistency and high availability.
- **Monitoring and Metrics**: Curvine features a comprehensive built-in observability metrics system, facilitating detailed monitoring of the performance of each component.
- **Web Interface**: Provides a web management interface for convenient system monitoring and management.

## 📦 System Requirements

- Rust 1.86+
- Linux or macOS (Limited support on Windows)
- FUSE library (for file system functionality)


**Officially Supported Linux Distributions**​

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

### 🖥️  Start a single - node cluster
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
## 🏗️ cluster auto deploy
Introduction of Advantages
Automated Component Deployment: Automatically deploys Master, Worker, and Fuse services onto their respective functional nodes.<br>

Smart Disk Detection: Automatically detects both used and unused NVMe drives across every GPU server within your cluster.<br>

Automated Storage Initialization: Automatically formats unused disks and mounts them to your specified paths.<br>

Dynamic Configuration Management: Automatically adds specified paths and all other NVMe mount points (excluding the system partition) to the configuration file.<br>

Service-Oriented Management: Integrates Master, Worker, and Fuse services with systemd, allowing management via standard systemctl commands.<br>

Flexible Lifecycle Management: Supports idempotent deployments (repeated runs), deployment repairs, and incremental configuration updates. Provides automated batch management for service start, restart, and stop operations.<br>

Environment Automation: Automatically injects and activates required environment variables based on your specifications.<br>

Self-Correction Logic: Automatically detects configuration errors and applies corrections to ensure deployment integrity.<br>
```bash
apt install ansible
vi host.ini (config your master node ip address list 和worker ip address list，note：master node will start all three service，worker node only start worker service and fuse service )
cd ansible
chmod 777 *
./deploy_all.sh
```


## 🏗️ Architecture Design

Curvine adopts a master-slave architecture:

- **Master Node**: Responsible for metadata management, worker node coordination, and load balancing.
- **Worker Node**: Responsible for data storage and processing.
- **Client**: Communicates with the Master and Worker nodes via RPC.

The system uses the Raft consensus algorithm to ensure metadata consistency and supports multiple storage strategies (memory, SSD, HDD) to optimize performance and cost.

## 📈 Performance

Curvine performs excellently in high-concurrency scenarios and supports:

- High-throughput data read and write
- Low-latency operations
- Large-scale concurrent connections

## Contributing
Please read Curvine [Contribute guidelines](CONTRIBUTING.md)

## 📜 License
Curvine is licensed under the ​**​[Apache License 2.0](LICENSE)​**.

## Star History

[![Star History Chart](https://api.star-history.com/svg?repos=CurvineIO/curvine&type=Date)](https://www.star-history.com/#CurvineIO/curvine&Date)
