# GitHub Actions 工作流说明

本项目使用 GitHub Actions 自动化构建和测试流程，包含以下工作流：

## 1. 构建 Docker 镜像 (build-docker-image.yml)

此工作流用于构建 Rust 编译环境的 Docker 镜像。

### 特点

- 手动触发执行
- 可选择是否推送镜像到 GitHub Container Registry
- 使用 Docker 缓存加速构建

### 使用方法

1. 在 GitHub 仓库页面，点击 "Actions" 标签
2. 从左侧列表选择 "Build Docker Image" 工作流
3. 点击 "Run workflow" 按钮
4. 选择是否推送镜像到 GitHub Container Registry
5. 点击 "Run workflow" 开始构建

## 2. Rust 构建与测试 (build.yml)

此工作流用于构建和测试 Rust 项目。

### 特点

- 自动触发：在所有分支的推送和拉取请求上执行
- 使用自定义 Docker 镜像作为构建环境
- 使用 Rust 缓存加速构建
- 执行代码格式检查和 Clippy 静态分析

### 前提条件

在使用此工作流之前，需要先运行 "Build Docker Image" 工作流并推送镜像到 GitHub Container Registry。

## 注意事项

1. 首次使用时，需要确保 GitHub 仓库有权限访问 GitHub Container Registry
2. 如果修改了 Docker 镜像的配置，需要重新构建并推送镜像
3. Clippy 检查级别设置为 "deny"，可以在工作流文件中修改

## 3. 触发 Helm Chart 发版 (trigger-helm-release.yml)

当 `curvine` 推送 `v*` tag 后，此工作流会:

1. 等待 runtime 和 CSI 镜像构建完成
2. 校验 GHCR 中对应 tag 的镜像已发布
3. 向 `CurvineIO/curvine-helm` 发送 `repository_dispatch` 事件 (`curvine-release`)

`curvine-helm` 收到事件后会自动在 `main` 上创建同名 tag，并触发 chart 打包。

### 前置配置

在 `CurvineIO/curvine` 仓库 Settings -> Secrets and variables -> Actions 中添加:

| Secret | 说明 |
| --- | --- |
| `CURVINE_HELM_DISPATCH_TOKEN` | 对 `curvine-helm` 有读写权限的 PAT，用于跨仓库 dispatch |

### 手动重试

Actions -> `Trigger Helm Release` -> Run workflow，填写 tag（如 `v0.3.0`）。