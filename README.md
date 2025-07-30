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
[![Rust](https://img.shields.io/badge/Rust-1.80%2B-orange)](https://www.rust-lang.org)

**Curvine** is a high-performance, concurrent distributed cache system written in Rust, designed for low-latency and high-throughput workloads.

**[Roadmap 2025](https://github.com/CurvineIO/curvine/issues/29)**

## 📚 Documentation Resources

For more detailed information, please refer to:

- [Official Documentation](https://curvineio.github.io/docs/Overview/instroduction)
- [Quick Start](https://curvineio.github.io/docs/Deploy/quick-start)
- [User Manuals](https://curvineio.github.io/docs/category/user-manuals)
- [Benchmark](https://curvineio.github.io/docs/category/benchmark)
- [DeepWiki](https://deepwiki.com/CurvineIO/curvine)
- [Commit convention](COMMIT_CONVENTION.md)

## Use Case
![use_case](https://raw.githubusercontent.com/CurvineIO/curvine-doc/refs/heads/main/docs/1-Overview/img/curvine-scene.jpg)

- **Case1**: Shuffle acceleration
- **Case2**: Hot table data acceleration
- **Case3**: Training acceleration
- **Case4**: Model distribution
- **Case5**: Multi-cloud data caching


## 🚀 Core Features

- **High-performance RPC Framework**: An asynchronous communication framework based on Tokio, supporting high-concurrency request processing.
- **Distributed Architecture**: A Master - Worker architecture design that supports horizontal scaling.
- **Multi-level Cache**: Supports multi - level cache strategies for memory, SSD, and HDD.
- **FUSE Interface**: Provides a FUSE file system interface for seamless integration into existing systems.
- **Underlying Storage Integration**: Supports integration with multiple underlying storage systems.
- **Raft Consensus**: Uses the Raft algorithm to ensure data consistency and high availability.
- **Monitoring and Metrics**: Built - in monitoring and performance metric collection.
- **Web Interface**: Provides a web management interface for convenient system monitoring and management.

## 🧩 Modular Architecture
Curvine adopts a modular design and is mainly composed of the following core components：

- **orpc**: A high-performance network communication framework that supports asynchronous RPC calls.
- **curvine-common**: A shared library containing protocol definitions, error handling, and common utilities.
- **curvine-server**: A server component that includes Master and Worker implementations.
- **curvine-client**: A client library that provides APIs for interacting with the server.
- **curvine-fuse**: A FUSE file system interface that allows Curvine to be mounted as a local file system.
- **curvine-libsdk**: An SDK library that supports multi - language access.
- **curvine-web**: A web management interface and API.
- **curvine-tests**: A testing framework and performance benchmarking tool.

## 📦 System Requirements

- Rust 1.80+
- Linux or macOS (Limited support on Windows)
- FUSE library (for file system functionality)

## 🗂️ Cached File System Access
### 🦀 Rust API (Recommended for Native Integration)
```
use curvine_common::conf::ClusterConf;
use curvine_common::fs::Path;
use std::sync::Arc;

let conf = ClusterConf::from(conf_path);
let rt = Arc::new(conf.client_rpc_conf().create_runtime());
let fs = CurvineFileSystem::with_rt(conf, rt)?;

let path = Path::from_str("/dir")?;
fs.mkdir(&path).await?;
```

### 📌 FUSE (Filesystem in Userspace)
```
ls /curvine-fuse
```

**Officially Supported Linux Distributions**​

| OS Distribution     | Kernel Requirement | Tested Version | Dependencies |
|---------------------|--------------------|----------------|--------------|
| ​**CentOS 7**​      | ≥3.10.0            | 7.6            | fuse2-2.9.2  |
| ​**CentOS 8**​      | ≥4.18.0            | 8.5            | fuse3-3.9.1  |
| ​**Rocky Linux 9**​ | ≥5.14.0            | 9.5            | fuse3-3.10.2 |
| ​**RHEL 9**​        | ≥5.14.0            | 9.5            | fuse3-3.10.2 |
| ​**Ubuntu 22**​      | ≥5.15.0            | 22.4           | fuse3-3.10.5 |

### 🐘 Hadoop Compatible API
```
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

Configuration conf = new Configuration();
conf.set("fs.cv.impl", "io.curvine.CurvineFileSystem");

FileSystem fs = FileSystem.get(URI.create("cv://master:8995"), conf);
FSDataInputStream in = fs.open(new Path("/user/test/file.txt"));
```

## 🛠 Build Instructions

This project requires the following dependencies. Please ensure they are installed before proceeding:

### 📋 Prerequisites

- ​**Rust**: version 1.80 or later ([Installation Guide](https://www.rust-lang.org/tools/install))
- ​**Protobuf**: version 2.x
- ​**Maven**: version 3.8 or later ([Install Guide](https://maven.apache.org/install.html))
- ​**LLVM**: version 12 or later ([Installation Guide](https://llvm.org/docs/GettingStarted.html))
- ​**FUSE**: libfuse2 or libfuse3 development packages
- ​**JDK**: version 1.8 or later (OpenJDK or Oracle JDK)
- ​**npm**: version 9 or later ([Node.js Installation](https://nodejs.org/))

You can either:
1. Use the pre-configured `curvine-docker/compile/Dockerfile_rocky9` to build a compilation image
2. Reference this Dockerfile to create a compilation image for other operating system versions

### 🚀 Build Steps (Linux - Ubuntu/Debian example)

```bash
# Compiled files are in build/dist
make all
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
bin/curvine report
```

Access the file system using compatible HDFS commands:
```bash
bin/curvine fs -mkdir /a
bin/curvine fs -ls /
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

## 📜 License
Curvine is licensed under the ​**​[Apache License 2.0](LICENSE)​**.

## Star History

[![Star History Chart](https://api.star-history.com/svg?repos=CurvineIO/curvine&type=Date)](https://www.star-history.com/#CurvineIO/curvine&Date)
