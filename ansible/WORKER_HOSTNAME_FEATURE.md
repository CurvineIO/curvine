# Worker节点主机名环境变量功能说明

## 功能概述

为每个 Worker 节点的 `/etc/profile` 添加环境变量 `CURVINE_WORKER_HOSTNAME`，该变量的值等于该 Worker 在 `hosts.ini` 中定义的 IP 地址。

## 自动配置

### 1. 部署时自动设置

在运行部署脚本时，会自动为所有 Worker 节点设置环境变量：

```bash
ansible-playbook deploy_curvine.yml
```

部署脚本会自动：
- 在 `/etc/profile` 中添加 `export CURVINE_WORKER_HOSTNAME={该worker的IP}`
- 在 systemd 服务文件中添加相应的环境变量
- 确保每个 Worker 的 IP 地址与 `hosts.ini` 中的定义一致

### 2. 启动服务时自动检查

在运行启动服务脚本时，会自动检查并修复环境变量：

```bash
ansible-playbook start_services.yml
```

启动脚本会在启动服务前：
- 检查 `CURVINE_WORKER_HOSTNAME` 是否已设置
- 验证设置的值是否与 `hosts.ini` 中的 IP 一致
- 如果不正确，自动修复并重新设置

## 独立设置/更新

如果需要单独设置或更新 Worker 节点的环境变量，可以使用专用脚本：

```bash
ansible-playbook setup_worker_hostname.yml
```

### 适用场景

- 已部署的环境需要添加此环境变量
- 环境变量被意外修改或删除
- Worker 节点 IP 发生变更后需要更新
- 验证所有 Worker 的环境变量设置是否正确

## 配置详情

### 环境变量

每个 Worker 节点的 `/etc/profile` 中会添加：

```bash
export CURVINE_WORKER_HOSTNAME=<该worker的IP>
```

例如，对于 IP 为 `10.128.131.15` 的 Worker：

```bash
export CURVINE_WORKER_HOSTNAME=10.128.131.15
```

### Systemd 服务配置

Worker 节点的服务文件也会包含此环境变量：

**curvine-worker.service:**
```ini
[Service]
Environment="CURVINE_MASTER_HOSTNAME=localhost"
Environment="CURVINE_WORKER_HOSTNAME=10.128.131.15"
ExecStart=/root/dist/bin/curvine-worker.sh start
```

**curvine-fuse.service:**
```ini
[Service]
Environment="CURVINE_MASTER_HOSTNAME=localhost"
Environment="CURVINE_WORKER_HOSTNAME=10.128.131.15"
ExecStart=/root/dist/bin/curvine-fuse.sh start
```

## 验证设置

### 检查所有 Worker 的环境变量

```bash
ansible worker -m shell -a "grep CURVINE_WORKER_HOSTNAME /etc/profile"
```

### 检查运行中的服务环境变量

```bash
ansible worker -m shell -a "systemctl show curvine-worker -p Environment"
```

### 验证 IP 对应关系

```bash
ansible worker -m shell -a "echo \$CURVINE_WORKER_HOSTNAME"
```

输出应该显示每个 Worker 返回其在 `hosts.ini` 中定义的 IP 地址。

## 注意事项

1. **IP 地址来源**：环境变量的值直接来自 `hosts.ini` 文件中定义的 Worker 节点 IP
2. **自动生效**：新的终端会话会自动加载 `/etc/profile` 中的环境变量
3. **服务重启**：修改环境变量后，建议重启相关服务以确保立即生效
4. **备份**：修改 `/etc/profile` 时会自动创建备份文件

## 重启服务使环境变量生效

如果修改了环境变量，建议重启 Worker 服务：

```bash
# 重启所有服务
ansible-playbook restart_services.yml

# 或者只重启 Worker 节点的服务
ansible worker -m systemd -a "name=curvine-worker state=restarted"
ansible worker -m systemd -a "name=curvine-fuse state=restarted"
```

## 故障排查

### 环境变量未设置

**问题**：`grep CURVINE_WORKER_HOSTNAME /etc/profile` 没有输出

**解决**：
```bash
ansible-playbook setup_worker_hostname.yml
```

### 环境变量值不正确

**问题**：环境变量的值与 hosts.ini 中的 IP 不一致

**解决**：
```bash
# 自动修复
ansible-playbook setup_worker_hostname.yml

# 或手动修复
ansible worker -m lineinfile -a "path=/etc/profile regexp='.*CURVINE_WORKER_HOSTNAME.*' state=absent"
ansible-playbook setup_worker_hostname.yml
```

### 服务启动失败

**问题**：Worker 服务启动失败，日志显示找不到主机名

**检查**：
```bash
# 检查环境变量
ansible worker -m shell -a "grep CURVINE_WORKER_HOSTNAME /etc/profile"

# 检查服务配置
ansible worker -m shell -a "grep CURVINE_WORKER_HOSTNAME /etc/systemd/system/curvine-worker.service"
```

**解决**：
```bash
ansible-playbook setup_worker_hostname.yml
ansible-playbook restart_services.yml
```

## 更新历史

- **2024-12**: 添加 CURVINE_WORKER_HOSTNAME 环境变量功能
  - 自动在部署时设置
  - 启动服务前自动检查和修复
  - 提供独立的设置脚本

## 相关文件

- `deploy_curvine.yml`: 主部署脚本，包含环境变量设置
- `start_services.yml`: 服务启动脚本，包含环境变量检查
- `setup_worker_hostname.yml`: 独立的环境变量设置脚本
- `hosts.ini`: Worker 节点 IP 定义文件

