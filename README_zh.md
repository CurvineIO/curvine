<div align=center>
<img src="https://raw.githubusercontent.com/CurvineIO/curvine-doc/refs/heads/main/static/img/curvine_logo.svg",  width="180" height="200">
</div>

![curvine-font-dark](https://raw.githubusercontent.com/CurvineIO/curvine-doc/refs/heads/main/static/img/curvine_font_dark.svg#gh-light-mode-only)
![curvine-font-light](https://raw.githubusercontent.com/CurvineIO/curvine-doc/refs/heads/main/static/img/curvine_font_white.svg#gh-dark-mode-only)

<p align="center">
  <a href="https://github.com/CurvineIO/curvine/blob/main/README.md">English</a> ||
  简体中文 |
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

**Curvine** 是一个用 Rust 编写的高性能、并发分布式缓存系统，专为低延迟和高吞吐量工作负载设计。

## 📚 文档资源

更多详细信息，请参阅：

- [官方文档](https://curvineio.github.io/docs/Overview/instroduction)
- [快速入门](https://curvineio.github.io/docs/Deploy/quick-start)
- [用户手册](https://curvineio.github.io/docs/category/user-manuals)
- [性能基准](https://curvineio.github.io/docs/category/benchmark)
- [DeepWiki](https://deepwiki.com/CurvineIO/curvine)
- [提交规范](COMMIT_CONVENTION.md)
- [vLLM CPU 推理镜像 + Curvine 环境变量部署（本仓库）](doc/docker-vllm-curvine-cpu.md)
- [Native KV 端到端自测：真实 CurvineClient + 集群（本仓库）](doc/testing-native-kv-e2e.md)

## 应用场景

![use_case](https://raw.githubusercontent.com/CurvineIO/curvine-doc/refs/heads/main/docs/1-Overview/img/curvine-scene.png)

- **场景1**: 训练加速
- **场景2**: 模型分发
- **场景3**: 热表数据加速
- **场景4**: 大数据Shuffle加速
- **场景5**: 多云数据缓存

## 🚀 核心特性

- **高性能 RPC 框架**：基于 Tokio 的异步通信框架，支持高并发请求处理。
- **分布式架构**：采用 Master-Worker 架构设计，支持水平扩展。
- **多级缓存**：支持内存、SSD 和 HDD 的多级缓存策略。
- **FUSE 接口**：提供 FUSE 文件系统接口，可无缝集成到现有系统中。
- **底层存储集成**：支持与多种底层存储系统集成。
- **Raft 共识**：采用 Raft 算法确保数据一致性与高可用性。
- **监控与指标**：内置监控与性能指标收集功能。
- **Web 界面**：提供 Web 管理界面，便于系统监控与管理。

## 📦 系统要求

- Rust 1.80+
- Linux 或 macOS (Windows 支持有限)
- FUSE 库 (用于文件系统功能)

## 🗂️ 缓存文件系统访问

### 🦀 Rust API (原生集成推荐)

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

### 📌 FUSE (用户空间文件系统)

```
ls /curvine-fuse
```

**官方支持的 Linux 发行版**​

| 操作系统发行版     | 内核要求          | 测试版本       | 依赖项        |
|---------------------|-------------------|---------------|--------------|
| ​**CentOS 7**​      | ≥3.10.0           | 7.6           | fuse2-2.9.2  |
| ​**CentOS 8**​      | ≥4.18.0           | 8.5           | fuse3-3.9.1  |
| ​**Rocky Linux 9**​ | ≥5.14.0           | 9.5           | fuse3-3.10.2 |
| ​**RHEL 9**​        | ≥5.14.0           | 9.5           | fuse3-3.10.2 |
| ​**Ubuntu 22**​      | ≥5.15.0           | 22.4          | fuse3-3.10.5 |

### 🐘 Hadoop 兼容 API

```
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

Configuration conf = new Configuration();
conf.set("fs.cv.impl", "io.curvine.CurvineFileSystem");

FileSystem fs = FileSystem.get(URI.create("cv://master:8995"), conf);
FSDataInputStream in = fs.open(new Path("/user/test/file.txt"));
```

## 🛠 构建指南

本项目需要以下依赖项，请确保在继续之前已安装：

### 📋 先决条件

- ​**GCC**: 10 或更高版本 ([安装指南](https://gcc.gnu.org/install/))
- ​**Rust**: 1.86 或更高版本 ([安装指南](https://www.rust-lang.org/tools/install))
- ​**Protobuf**: 3.x 版本
- ​**Maven**: 3.8 或更高版本 ([安装指南](https://maven.apache.org/install.html))
- ​**LLVM**: 12 或更高版本 ([安装指南](https://llvm.org/docs/GettingStarted.html))
- ​**FUSE**: libfuse2 或 libfuse3 开发包
- ​**JDK**: 1.8 或更高版本 (OpenJDK 或 Oracle JDK)
- ​**npm**: 9 或更高版本 ([Node.js 安装](https://nodejs.org/))
- ​**Python**: 3.7 或更高版本 ([安装指南](https://www.python.org/downloads/))

您可以选择：

1. 使用预配置的 `curvine-docker/compile/Dockerfile_rocky9` 来构建编译镜像
2. 参考此 Dockerfile 为其他操作系统版本创建编译镜像
3. curvine在dockerhub上提供了`curvine/curvine-compile:latest` 镜像(基于rocky9)，可以使用此镜像进行编译

### 🚀 构建步骤 (Linux - Ubuntu/Debian 示例)

使用 make 编译:

```bash
# 编译所有模块
make all

# 只编译核心模块: server client cli
make build ARGS="-p core"

# 编译fuse和核心模块
make build ARGS="-p core -p fuse"
```


使用build.sh编译：

```bash
# 编译所有模块
sh build/build.sh 

# 输出命令帮助 
sh build/build.sh -h

# 只编译核心模块: server client cli
sh build/build.sh -p core

# 编译fuse和核心模块
sh build/build.sh -p core -p fuse
```

构建镜像：

```bash
# or use curvine-compile:latest docker images to build
make docker-build

# or use curvine-compile:build-cached docker images to build, this image already cached most dependency crates
make docker-build-cached
```

编译成功后，目标文件将生成在 build/dist 目录中。该文件是可用于部署或构建镜像的 Curvine 安装包。

### 🖥️  启动单节点集群

```bash
cd build/dist

# Start the master node
bin/curvine-master.sh start

# Start the worker node
bin/curvine-worker.sh start
```

挂载文件系统

```bash
# The default mount point is /curvine-fuse
bin/curvine-fuse.sh start
```

查看集群概览：

```bash
bin/cv report
```

执行文件系统命令：
```bash
bin/dfs fs mkdir /a
bin/dfs fs ls /
```

访问 Web 界面：

```
http://your-hostname:9000
```

Curvine 使用 TOML 格式的配置文件。示例配置位于 conf/curvine-cluster.toml，主要配置项包括：

- 网络设置（端口、地址等）
- 存储策略（缓存大小、存储类型）
- 集群配置（节点数量、副本因子）
- 性能调优参数

## 🏗️ 架构设计

Curvine 采用主从架构：

- **主节点**：负责元数据管理、工作节点协调和负载均衡
- **工作节点**：负责数据存储和处理
- **客户端**：通过 RPC 与主节点和工作节点通信

该系统使用 Raft 共识算法确保元数据一致性，并支持多种存储策略（内存、SSD、HDD）以优化性能和成本。

## 📈 性能表现

Curvine 在高并发场景下表现优异，支持：

- 高吞吐量数据读写
- 低延迟操作
- 大规模并发连接

## 📜 许可证

Curvine 采用 **[Apache License 2.0](LICENSE)** 开源协议授权。

## 星标历史

[![Star History Chart](https://api.star-history.com/svg?repos=CurvineIO/curvine&type=Date)](https://www.star-history.com/#CurvineIO/curvine&Date)
