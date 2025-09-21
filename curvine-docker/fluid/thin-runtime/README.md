# Curvine Fluid ThinRuntime Docker Image

<<<<<<< HEAD
This directory contains the Docker build configuration for Curvine Fluid ThinRuntime, providing a containerized solution for distributed file system management.

## Build Modes

The build system supports two modes:

### 1. Pre-built Binary Mode (Recommended)
- Uses pre-compiled binaries from the `build/` directory
- Faster build process
- Smaller final image size
- Uses single-stage Docker build

### 2. Local Workspace Build Mode
- Builds from complete source code in the `workspace/` directory
- Includes full development toolchain
- Uses multi-stage Docker build
- Longer build time but ensures latest code

## Quick Start

Run the interactive build script:

```bash
./build-image.sh
```

The script will present you with build options:
1. **Build from pre-built binaries (Recommended)** - Fast build using existing artifacts
2. **Build from local workspace** - Complete source build with full toolchain
3. **Exit** - Cancel the build process

## Build Optimization Features

### Alibaba Cloud Mirror
To improve package download speeds in China, both Docker images are configured to use Alibaba Cloud mirrors:
- Base images use `mirrors.aliyun.com` for Ubuntu packages
- Rust toolchain uses Alibaba Cloud Rustup mirror
- Maven uses Alibaba Cloud repository mirror

### Docker Build Context
The build process automatically optimizes the Docker build context:
- Only necessary files are included
- Large directories are excluded to reduce build time
- Different contexts for different build modes

## Files Structure

```
fluid/thin-runtime/
├── build-image.sh          # Interactive build script
├── Dockerfile              # Multi-stage build for workspace mode
├── Dockerfile.binary       # Single-stage build for binary mode
├── fluid-config-parse.py   # Configuration parser
├── README.md               # This documentation
├── build/                  # Pre-built binaries directory
├── workspace/              # Complete source code directory
└── compile/                # Build configuration files
    ├── config              # Rust cargo configuration
    └── settings.xml        # Maven settings
```

## Environment Variables

The Docker images support the following environment variables:

- `CURVINE_HOME`: Base directory for Curvine installation (default: `/opt/curvine`)
- `ORPC_BIND_HOSTNAME`: Hostname for RPC binding (default: `0.0.0.0`)
- `CURVINE_CONF_FILE`: Path to configuration file (default: `/opt/curvine/conf/curvine-cluster.toml`)
- `BUILD_FROM_SOURCE`: Build mode flag (true/false)

## Manual Build Commands

If you prefer to build manually without the interactive script:

### Pre-built Binary Mode
```bash
docker build -f Dockerfile.binary -t curvine/fluid-thin-runtime:binary .
```

### Local Workspace Mode
```bash
docker build --build-arg BUILD_FROM_SOURCE=true -t curvine/fluid-thin-runtime:workspace .
```

## Troubleshooting

### Slow Package Downloads
If you experience slow package downloads, the images are already configured with Alibaba Cloud mirrors. If issues persist:
1. Check your network connection
2. Consider using a VPN if accessing from outside China
3. Verify the mirror configuration is working

### Build Failures
1. Ensure you have the necessary directories (`build/` or `workspace/`)
2. Check that all required files are present
3. Verify Docker has sufficient resources
4. Check the build logs for specific error messages

### Permission Issues
If you encounter permission issues:
```bash
chmod +x build-image.sh
./build-image.sh
```

## Contributing

When contributing to this build system:
1. Test both build modes
2. Ensure the interactive script works correctly
3. Update documentation for any new features
4. Maintain compatibility with existing configurations

## License

This project follows the same license as the main Curvine project.
=======
这个目录包含用于构建 Curvine Fluid ThinRuntime Docker 镜像的文件。该镜像支持2阶段构建，可以从源码编译或使用预构建的二进制文件。

## 文件说明

- `Dockerfile` - 2阶段 Docker 构建文件（基于 Ubuntu 22.04）
- `build-image.sh` - 构建脚本，支持从源码构建或预构建二进制
- `fluid-config-parse.py` - Fluid 配置解析脚本
- `curvine-thinruntime.yaml` - Kubernetes 部署示例
- `test_pod.yaml` - 测试 Pod 示例

## 构建方法

### 方法1: 使用预构建的二进制文件（推荐）

```bash
# 确保已经构建了 Curvine 项目
cd /path/to/curvine/project/root
make build  # 或者 cargo build --release

# 构建 Docker 镜像
cd curvine-docker/fluid/thin-runtime
./build-image.sh
```

### 方法2: 从源码构建

```bash
# 从源码构建（包含完整编译过程）
cd curvine-docker/fluid/thin-runtime
BUILD_FROM_SOURCE=true ./build-image.sh
```

## 2阶段构建说明

### 构建阶段 (Builder Stage)
- 基于 Ubuntu 22.04
- 安装完整的编译工具链（Rust、Maven、protoc等）
- 参考 `Dockerfile_ubuntu22` 的编译环境配置
- 仅在 `BUILD_FROM_SOURCE=true` 时安装编译工具
- 支持从源码编译或直接使用预构建二进制

### 运行阶段 (Runtime Stage)
- 基于 Ubuntu 22.04
- 仅包含运行时依赖（Python3、FUSE3、JRE等）
- 显著减少最终镜像大小
- 包含 Fluid 配置解析和挂载脚本

## 使用示例

### 本地测试
```bash
# 运行容器进行测试
docker run --rm --privileged fluid-cloudnative/curvine-thinruntime:v1.0.0
```

### 在 Kubernetes 中部署
```bash
# 应用 ThinRuntime 配置
kubectl apply -f curvine-thinruntime.yaml

# 测试挂载
kubectl apply -f test_pod.yaml
```

## 环境变量配置

镜像支持以下环境变量作为配置回退：

- `CURVINE_MOUNT_POINT` - 挂载点（例如：curvine:///data）
- `CURVINE_TARGET_PATH` - 目标挂载路径（例如：/mnt/data）
- `CURVINE_MASTER_ENDPOINTS` - Master 端点（例如：master:9000）
- `CURVINE_MASTER_WEB_PORT` - Master Web 端口（默认：8080）
- `CURVINE_IO_THREADS` - IO 线程数（默认：32）
- `CURVINE_WORKER_THREADS` - 工作线程数（默认：56）

## 构建选项

### 镜像标签
可以通过修改 `build-image.sh` 中的变量来自定义镜像名称和标签：

```bash
IMAGE_NAME="fluid-cloudnative/curvine-thinruntime"
IMAGE_TAG="v1.0.0"
```

### 构建参数
- `BUILD_FROM_SOURCE=true` - 从源码构建
- `BUILD_FROM_SOURCE=false` - 使用预构建二进制（默认）

## 故障排除

1. **构建失败**：确保有足够的磁盘空间和内存
2. **运行时错误**：检查 FUSE 模块是否加载：`modprobe fuse`
3. **权限问题**：确保容器以 `--privileged` 模式运行
4. **网络问题**：检查 Master 端点是否可达

## 镜像大小优化

2阶段构建的优势：
- 构建阶段：包含完整开发工具链（~2GB）
- 运行阶段：仅包含运行时依赖（~500MB）
- 最终镜像大小显著减少，提高部署效率
>>>>>>> 368d183... feat: support two-stage build for thin-runtime
