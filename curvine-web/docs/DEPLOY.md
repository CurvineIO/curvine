# Curvine Web 部署指南

> **适用范围**：`curvine-next` 仓库 **main** 分支（独立 `curvine-web` 管理控制台）。  
> 社区版 `curvine` 仓库中的 `curvine-web` 仅为库 crate，不含 `src/bin/curvine-web.rs`，不适用本文档。

## 1. 组件说明

| 组件 | 路径 | 说明 |
|------|------|------|
| 后端二进制 | `curvine-web/src/bin/curvine-web.rs` | 独立进程，Axum 提供 `/api/v1/*` 并托管静态资源 |
| 前端 | `curvine-web/webui-next/` | React 18 + Vite，构建产物在 `webui-next/dist/` |
| 集群配置 | `etc/curvine-cluster.toml` | 通过 `--conf` 传入，连接远端 Master/Worker |

### 端口分工

| 进程 | 配置项 | 默认端口 | 用途 |
|------|--------|----------|------|
| **curvine-web**（独立控制台） | `[web].port` | **9000** | 管理界面 + `/api/v1/*` |
| **curvine-master**（metrics） | `[master].web_port` | **9010** | Master `/metrics`（可观测性抓取） |
| Master RPC | `[master].rpc_port` | 8995 | 元数据 RPC |

> **注意**：旧配置曾将 Master 的 `web_port` 设为 9000。当前 main 分支已将 Master metrics 拆到 **9010**。  
> 若启用 `[web.observability]` 并配置 `client.master_web_addrs`，应指向各 Master 的 **metrics 端口（9010）**，而非独立控制台端口 9000。

---

## 2. 环境要求

| 工具 | 版本 | 用途 |
|------|------|------|
| Rust | 1.80+ | 编译后端 |
| Node.js | 18+ | 编译前端 |
| npm | 随 Node | 前端依赖 |
| JindoSDK | 按需 | 仅 OSS-HDFS 场景需要 |

若磁盘空间紧张，可将临时目录与编译输出指向空间充足的分区：

```bash
export TMPDIR=/var/tmp
export CARGO_TARGET_DIR=/opt/curvine/build/target
```

---

## 3. 获取源码

```bash
git clone <curvine-next-repo-url>
cd curvine-next
git checkout main
git pull --rebase origin main
```

确认版本：

```bash
git rev-parse HEAD
git log -1 --oneline
```

---

## 4. 构建

### 4.1 后端

**默认构建**（已内置 S3 opendal 支持，`curvine-web/Cargo.toml` 默认启用 `curvine-unified-fs/opendal-s3`）：

```bash
cd curvine-next
cargo build --release -p curvine-web
```

产物：`target/release/curvine-web`（或 `${CARGO_TARGET_DIR}/release/curvine-web`）

**启用 OSS-HDFS**（需预先安装 JindoSDK 并配置 `JINDOSDK_HOME`）：

```bash
export JINDOSDK_HOME=/opt/jindosdk
export LD_LIBRARY_PATH="${JINDOSDK_HOME}/lib/native:${JINDOSDK_HOME}/lib:${LD_LIBRARY_PATH:-}"

cargo build --release -p curvine-web --features curvine-unified-fs/oss-hdfs
```

### 4.2 前端

```bash
cd curvine-web/webui-next
npm ci --include=dev   # 首次
npm run build
```

> `vite` / `tailwindcss` 属于 devDependencies。若环境导出 `NODE_ENV=production`，
> 必须带 `--include=dev`，否则安装后缺少构建工具。

产物：`curvine-web/webui-next/dist/`

也可以直接用 `build/build.sh -p web`（或 `make build`）：它会构建本模块并把
`dist/` 移动到 **`build/dist/webui/`**，部署目录名仍为 `webui`，后端无需任何改动。

---

## 5. 配置

### 5.1 集群 TOML

以 `etc/curvine-cluster.toml` 或 `curvine-docker/deploy/example/conf/curvine-cluster.toml` 为模板，**至少**配置：

```toml
cluster_id = "curvine"

[client]
master_addrs = [
    { hostname = "cv-master-0.example.svc", port = 8995 },
]
# 可观测性：指向各 Master 的 /metrics 端口（默认为 9010）
master_web_addrs = [
    { hostname = "cv-master-0.example.svc", port = 9010 },
]

[journal]
journal_addrs = [
    { id = 1, hostname = "cv-master-0.example.svc", port = 8996 },
]

[web]
port = 9000

[web.observability]
enabled = true
mode = "builtin"
scrape_interval = "60s"
scrape_timeout = "3s"
retention = "24h"
```

`[journal].journal_addrs` 需与集群一致，用于 Overview 页 Master HA 可达性检查（Raft ping，默认端口 **8996**）。缺少该段时，核心 API 仍可能正常，但 HA 表格会显示「不可达」。

> **TOML 格式建议**：推荐使用 `[client]` 段内的内联数组（与 `curvine-docker/deploy/example/conf/curvine-cluster.toml` 一致）。  
> `[[client.master_addrs]]` 表数组写法在 curvine-next 中同样可解析，但编辑时容易误改 `port` 字段（例如批量替换时把 RPC 端口改成 `[web].port`），建议统一用内联数组。

完整字段说明见 `etc/curvine-cluster.toml` 内注释。

### 5.1.1 集群外部署（curvine-web 在 K8s 外）

当 `curvine-web` 运行在集群外、通过 **NodePort / LB IP** 连接 Master 时，推荐配置如下（`master_addrs` 用 K8s 短主机名，`master_web_addrs` 用可达 IP，并配合 `/etc/hosts`）：

```toml
[client]
master_addrs = [
    { hostname = "cv-curvine-master-0", port = 8995 },
]
master_web_addrs = [
    { hostname = "10.0.0.1", port = 9010 },
]

[journal]
journal_addrs = [
    { id = 1, hostname = "cv-curvine-master-0.<namespace>.svc.cluster.local", port = 8996 },
]
```

在 `curvine-web` 所在机器配置 `/etc/hosts`：

```text
10.0.0.1 cv-curvine-master-0 cv-curvine-master-0.<namespace>.svc.cluster.local
```

说明：

- **RPC / Overview**：`master_addrs` 中的短主机名经 hosts 解析到 Master IP。
- **可观测性 metrics**：程序可能抓取 `http://cv-curvine-master-0:9010/metrics`，同样依赖 hosts；若未配置会出现 `active-master: error sending request for url (...)`。
- **HA 可达性**：依赖 `journal_addrs` 与 `master_addrs` 主机名可关联；仅写 IP 且无 `journal_addrs` 时会报 `cannot be mapped to a raft node`。
- 若 `master_addrs` / `master_web_addrs` 使用从 `curvine-web` 所在网络可解析的 **FQDN**，可省略 hosts。
- 暂不需要可观测性时，可设 `[web.observability] enabled = false`。

验证：

```bash
curl -s -o /dev/null -w '%{http_code}\n' http://<master-ip>:9010/metrics
curl -s -o /dev/null -w '%{http_code}\n' http://cv-curvine-master-0:9010/metrics   # 需 hosts
```

### 5.2 认证环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `CURVINE_WEB_USERNAME` | `admin` | 登录用户名 |
| `CURVINE_WEB_PASSWORD` | — | 直接指定密码（K8s Secret 常用） |
| `CURVINE_WEB_PASSWORD_FILE` | `data/web-password` | 密码文件路径（首行内容为密码） |
| `CURVINE_WEBUI_DIR` | 工作目录下 `webui/` | 前端静态资源目录 |
| `CURVINE_WEB_SESSION_TTL_SECS` | `43200`（12h） | Session 有效期 |

### 5.3 启动命令

```bash
curvine-web start \
  --conf /path/to/curvine-cluster.toml \
  --hostname 0.0.0.0 \
  --port 9000
```

CLI 参数会覆盖 TOML 中 `[web]` 的 hostname/port。

---

## 6. 部署目录布局

推荐将运行目录与源码分离：

```
/opt/curvine-web/
├── bin/start-web.sh
├── conf/curvine-env.sh
├── data/web-password          # chmod 600
├── lib/curvine-web
├── logs/web.out
├── webui/                     # dist 内容
└── build-version              # 记录 git commit（可选）
```

安装产物：

```bash
DEPLOY_ROOT=/opt/curvine-web
mkdir -p "${DEPLOY_ROOT}"/{bin,conf,data,lib,logs,webui}

cp target/release/curvine-web "${DEPLOY_ROOT}/lib/"
chmod +x "${DEPLOY_ROOT}/lib/curvine-web"
cp -r build/dist/webui/* "${DEPLOY_ROOT}/webui/"
echo "your-initial-password" > "${DEPLOY_ROOT}/data/web-password"
chmod 600 "${DEPLOY_ROOT}/data/web-password"
```

### curvine-env.sh

```bash
#!/bin/bash
export CURVINE_HOME="$(cd "$(dirname "$0")"/..; pwd)"
export ORPC_BIND_HOSTNAME=0.0.0.0
export CURVINE_CONF_FILE=/path/to/curvine-cluster.toml
export JINDOSDK_HOME=/opt/jindosdk
export LD_LIBRARY_PATH="${JINDOSDK_HOME}/lib/native:${JINDOSDK_HOME}/lib:${LD_LIBRARY_PATH:-}"
export CURVINE_WEBUI_DIR="${CURVINE_HOME}/webui"
export CURVINE_WEB_PASSWORD_FILE="${CURVINE_HOME}/data/web-password"
export RUST_BACKTRACE="${RUST_BACKTRACE:-1}"
```

未使用 OSS-HDFS 时可省略 `JINDOSDK_HOME` 与 `LD_LIBRARY_PATH`。

### start-web.sh

```bash
#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
source "${ROOT}/conf/curvine-env.sh"
exec "${ROOT}/lib/curvine-web" start \
  --conf "${CURVINE_CONF_FILE}" \
  --hostname 0.0.0.0 \
  --port 9000
```

```bash
chmod +x "${DEPLOY_ROOT}/bin/start-web.sh" "${DEPLOY_ROOT}/conf/curvine-env.sh"
```

---

## 7. systemd 持久化

### 7.1 用户级服务（开发/测试）

服务文件：`~/.config/systemd/user/curvine-web.service`

```ini
[Unit]
Description=Curvine Web Admin Console
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/opt/curvine-web
ExecStart=/opt/curvine-web/bin/start-web.sh
Restart=always
RestartSec=5
StandardOutput=append:/opt/curvine-web/logs/web.out
StandardError=append:/opt/curvine-web/logs/web.out

[Install]
WantedBy=default.target
```

```bash
systemctl --user daemon-reload
systemctl --user enable --now curvine-web.service
```

### 7.2 系统级服务（生产）

将单元文件放到 `/etc/systemd/system/curvine-web.service`，内容同上，然后：

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now curvine-web.service
```

### 7.3 更新流程

```bash
# 1. 拉取并编译
cd curvine-next
git pull --rebase origin main
cargo build --release -p curvine-web
cd curvine-web/webui-next && npm ci --include=dev && npm run build

# 2. 停服替换（避免 Text file busy）
systemctl --user stop curvine-web.service   # 或 sudo systemctl stop curvine-web.service
cp target/release/curvine-web /opt/curvine-web/lib/
rm -rf /opt/curvine-web/webui/*
cp -r curvine-web/webui-next/dist/* /opt/curvine-web/webui/
echo "$(git rev-parse HEAD)" > /opt/curvine-web/build-version

# 3. 启动
systemctl --user start curvine-web.service
```

日常更新**仅替换**二进制与 `webui/` 静态资源，一般不修改集群 TOML、密码文件与环境脚本。

---

## 8. Kubernetes 部署

仓库自带示例：`curvine-docker/deploy/example/curvine-web.yaml`

要点：

- 镜像内二进制路径：`/app/curvine/lib/curvine-web`
- 通过 `CURVINE_WEB_USERNAME` / `CURVINE_WEB_PASSWORD` 注入认证
- Readiness：`GET /api/v1/auth/session` on port 9000

---

## 9. 验证

```bash
# 服务状态
systemctl status curvine-web.service
ss -tlnp | grep ':9000 '

# 页面可达
curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:9000/overview

# 登录 API
PASS=$(cat /opt/curvine-web/data/web-password)
curl -s -c /tmp/curvine-web.cookie -X POST http://127.0.0.1:9000/api/v1/auth/login \
  -H 'Content-Type: application/json' \
  -d "{\"username\":\"admin\",\"password\":\"$PASS\"}"

# 集群 API（需登录 cookie）
curl -s -b /tmp/curvine-web.cookie http://127.0.0.1:9000/api/v1/overview
curl -s -b /tmp/curvine-web.cookie http://127.0.0.1:9000/api/v1/workers

# Master metrics 网络（从 curvine-web 所在机器）
curl -s -o /dev/null -w '%{http_code}\n' http://<master-ip>:9010/metrics

# 可观测性 targets（检查 active-master 是否 scrape 成功）
curl -s -b /tmp/curvine-web.cookie http://127.0.0.1:9000/api/v1/observability/targets

# 版本
cat /opt/curvine-web/build-version
grep 'git version' /opt/curvine-web/logs/web.out | tail -1
```

期望结果：

| 检查项 | 期望 |
|--------|------|
| `/overview` | HTTP 200 |
| `/api/v1/auth/login` | `success: true` |
| `/api/v1/overview` | 返回集群容量、worker 数量等 |
| `http://<master-ip>:9010/metrics` | HTTP 200 |
| `master:active-master` target | `last_error` 为空；若不为空见 §5.1.1 |
| `/api/v1/master/ha` 中 `reachable` | `true`；若为 `false` 检查 `journal_addrs` 与 hosts（§5.1） |

浏览器访问：`http://<host>:9000/overview`

---

## 10. 常见问题

| 问题 | 处理 |
|------|------|
| 编译 feature 报错 | 使用 `cargo build -p curvine-web`；OSS-HDFS 加 `--features curvine-unified-fs/oss-hdfs` |
| `libjindosdk_c.so` 找不到 | 设置 `JINDOSDK_HOME` 和 `LD_LIBRARY_PATH` |
| RPC 连错端口 / `Connection refused` | 检查 `client.master_addrs` 的 **8995** 是否被误改为 `[web].port`；推荐用 `[client]` 内联数组并逐项核对端口 |
| `active-master: error sending request for url (...)` | 配置 `/etc/hosts` 映射短主机名，或关闭 observability（§5.1.1） |
| Overview HA「不可达」/ `cannot be mapped to a raft node` | 按 §5.1 补全 `journal_addrs`，`master_addrs` 使用可与 journal 关联的主机名 |
| 可观测性抓不到 Master 指标 | 确认 `master_web_addrs` 指向 **9010**；从 `curvine-web` 主机 `curl http://<ip>:9010/metrics` 应返回 200 |
| 页面空白 | 确认 `CURVINE_WEBUI_DIR` 含 `index.html` |
| `Text file busy` | 先 stop 服务再替换二进制 |
| 无法连集群 | 检查 `client.master_addrs` 网络可达性 |
| 根分区空间不足 | 将 `TMPDIR`、`CARGO_TARGET_DIR` 指向空间充足的分区 |

---

## 11. 相关文件

| 文件 | 说明 |
|------|------|
| `etc/curvine-cluster.toml` | 配置模板（含 `[web]` / observability） |
| `curvine-docker/deploy/example/curvine-web.yaml` | K8s 部署示例 |