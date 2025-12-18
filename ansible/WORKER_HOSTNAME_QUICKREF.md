# Worker节点主机名环境变量 - 快速参考

## 快速开始

### 新部署（自动配置）

```bash
# 正常部署，会自动为所有 Worker 设置 CURVINE_WORKER_HOSTNAME
ansible-playbook deploy_curvine.yml
ansible-playbook start_services.yml
```

### 已部署环境（添加环境变量）

```bash
# 为所有 Worker 节点添加 CURVINE_WORKER_HOSTNAME 环境变量
ansible-playbook setup_worker_hostname.yml

# 重启服务使其生效
ansible-playbook restart_services.yml
```

## 快速验证

```bash
# 验证所有 Worker 的环境变量设置
ansible-playbook verify_worker_hostname.yml

# 或快速检查
ansible worker -m shell -a "grep CURVINE_WORKER_HOSTNAME /etc/profile"

# 预期输出（每个 Worker 返回其自己的 IP）：
# 10.128.131.15 | SUCCESS | rc=0 >>
# export CURVINE_WORKER_HOSTNAME=10.128.131.15
#
# 10.128.131.16 | SUCCESS | rc=0 >>
# export CURVINE_WORKER_HOSTNAME=10.128.131.16
```

## 环境变量说明

### 变量名
`CURVINE_WORKER_HOSTNAME`

### 变量值
等于该 Worker 节点在 `hosts.ini` 中定义的 IP 地址

### 设置位置
1. `/etc/profile` - 系统级环境变量
2. `/etc/systemd/system/curvine-worker.service` - Worker 服务
3. `/etc/systemd/system/curvine-fuse.service` - FUSE 服务

### 示例

对于 `hosts.ini` 中的 Worker：
```ini
[worker]
10.128.131.15
10.128.131.16
10.128.131.39
```

每个节点的环境变量：
```bash
# 节点 10.128.131.15
export CURVINE_WORKER_HOSTNAME=10.128.131.15

# 节点 10.128.131.16
export CURVINE_WORKER_HOSTNAME=10.128.131.16

# 节点 10.128.131.39
export CURVINE_WORKER_HOSTNAME=10.128.131.39
```

## 常用命令

```bash
# 1. 查看所有 Worker 的环境变量
ansible worker -m shell -a "grep CURVINE_WORKER_HOSTNAME /etc/profile"

# 2. 验证服务配置中的环境变量
ansible worker -m shell -a "grep CURVINE_WORKER_HOSTNAME /etc/systemd/system/curvine-worker.service"

# 3. 检查运行中的服务环境变量
ansible worker -m shell -a "systemctl show curvine-worker -p Environment | grep CURVINE_WORKER_HOSTNAME"

# 4. 在 Worker 节点上手动验证
ssh root@10.128.131.15 "echo \$CURVINE_WORKER_HOSTNAME"
# 应该输出：10.128.131.15

# 5. 设置/更新环境变量（单个节点）
ansible-playbook setup_worker_hostname.yml --limit 10.128.131.15

# 6. 完整验证报告
ansible-playbook verify_worker_hostname.yml
```

## 故障排查

### 问题1：环境变量未设置

**症状**：
```bash
$ ansible worker -m shell -a "grep CURVINE_WORKER_HOSTNAME /etc/profile"
# 没有输出或报错
```

**解决**：
```bash
ansible-playbook setup_worker_hostname.yml
```

### 问题2：环境变量值不正确

**症状**：
```bash
$ ansible worker -m shell -a "grep CURVINE_WORKER_HOSTNAME /etc/profile"
10.128.131.15 | SUCCESS | rc=0 >>
export CURVINE_WORKER_HOSTNAME=wrong_value
```

**解决**：
```bash
ansible-playbook setup_worker_hostname.yml
```

### 问题3：服务启动失败

**症状**：
```bash
$ systemctl status curvine-worker
# 显示失败，日志中提示找不到主机名
```

**检查**：
```bash
# 检查环境变量
ansible worker -m shell -a "grep CURVINE_WORKER_HOSTNAME /etc/profile"

# 检查服务配置
ansible worker -m shell -a "grep CURVINE_WORKER_HOSTNAME /etc/systemd/system/curvine-worker.service"
```

**解决**：
```bash
# 修复环境变量
ansible-playbook setup_worker_hostname.yml

# 重启服务
ansible-playbook restart_services.yml
```

## 自动化流程

### deploy_curvine.yml（部署时）

```
1. 检查 CURVINE_WORKER_HOSTNAME 是否存在
2. 如果不存在，添加到 /etc/profile
3. 在 systemd 服务文件中添加环境变量
4. 显示设置状态
```

### start_services.yml（启动前）

```
1. 读取当前 CURVINE_WORKER_HOSTNAME 值
2. 与 hosts.ini 中的 IP 对比
3. 如果不一致，自动修复
4. 重新加载 /etc/profile
5. 启动服务
```

### setup_worker_hostname.yml（独立使用）

```
1. 检查所有 Worker 的环境变量
2. 修复 /etc/profile
3. 修复 systemd 服务文件
4. 重新加载 systemd
5. 显示详细状态
```

## 相关文档

- **WORKER_HOSTNAME_FEATURE.md** - 完整的功能说明文档
- **README.md** - 主文档（已更新）
- **CHANGELOG.md** - 更新日志（v1.12）

## 注意事项

1. ✅ **自动生效**：新的终端会话会自动加载环境变量
2. ✅ **服务重启**：修改环境变量后建议重启服务
3. ✅ **备份**：修改 `/etc/profile` 时会自动创建备份
4. ✅ **幂等性**：多次运行不会产生副作用
5. ✅ **验证**：每次修改后建议验证设置是否正确

