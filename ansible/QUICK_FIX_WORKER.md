# Worker节点快速修复指南

## 🚨 问题：文件解压到了错误的目录

**症状**：
```
root@worker:~# ls /root/dist
(空目录或不存在)

root@worker:~# ls /root/dist1
bin/  conf/  lib/  ...
```

**原因**：dist1.tar.gz解压后，文件在 `/root/dist1/`，但systemd服务配置指向 `/root/dist/`

## ⚡ 快速修复（推荐）

```bash
# 修复特定Worker节点
ansible-playbook fix_dist_path.yml --limit 10.200.3.15

# 修复所有Worker节点
ansible-playbook fix_dist_path.yml
```

**修复工具会自动**：
1. ✅ 停止服务
2. ✅ 备份现有的 `/root/dist`（如果存在）
3. ✅ 将 `/root/dist1` 移动到 `/root/dist`
4. ✅ 设置正确的权限
5. ✅ 重新启动服务
6. ✅ 验证服务状态

## 🔧 手动修复

如果需要手动修复，在Worker节点上执行：

```bash
# 1. 停止服务
systemctl stop curvine-worker curvine-fuse

# 2. 备份旧的dist（如果存在）
if [ -d /root/dist ]; then
    mv /root/dist /root/dist.backup.$(date +%Y%m%d_%H%M%S)
fi

# 3. 移动dist1到dist
mv /root/dist1 /root/dist

# 4. 验证
ls -la /root/dist/bin/
ls -la /root/dist/lib/

# 5. 设置权限
chmod -R 755 /root/dist/bin/
chmod -R 755 /root/dist/lib/

# 6. 启动服务
systemctl start curvine-worker
systemctl start curvine-fuse

# 7. 检查状态
systemctl status curvine-worker
systemctl status curvine-fuse
```

## 🔍 验证修复

```bash
# 检查目录结构
ansible worker -m shell -a "ls -la /root/dist/bin/" --limit 10.200.3.15

# 检查服务状态
ansible-playbook status_services.yml --limit 10.200.3.15

# 查看服务日志
ansible worker -m shell -a "journalctl -u curvine-worker -n 20 --no-pager" --limit 10.200.3.15
```

## 🛡️ 预防此问题

### 方法1：修正压缩包结构

确保 `dist1.tar.gz` 直接包含文件，而不是包含 `dist1/` 顶层目录：

```bash
# 错误的打包方式（会导致问题）
tar -czf dist1.tar.gz dist1/

# 正确的打包方式
cd dist1
tar -czf ../dist1.tar.gz .

# 或者重命名后打包
mv dist1 dist
tar -czf dist1.tar.gz dist/
```

### 方法2：使用相同的压缩包

Worker节点和Master节点可以使用相同的 `dist.tar.gz`：

```bash
# 在控制节点上
cp /root/dist.tar.gz /root/dist1.tar.gz

# 重新部署
ansible-playbook deploy_curvine.yml --limit worker
```

### 方法3：使用修复后的部署脚本

新版本的 `deploy_curvine.yml` 和 `fix_worker.yml` 已经自动处理这个问题：
- 自动检测是否解压到了 `dist1/`
- 自动移动到 `dist/`
- 验证目录存在

## 📊 检查当前状态

```bash
# 检查Worker节点的目录情况
ansible worker -m shell -a "ls -la /root/ | grep dist"

# 检查哪些节点有问题
ansible worker -m shell -a "[ -d /root/dist1 ] && echo 'dist1存在' || echo 'dist1不存在'"
ansible worker -m shell -a "[ -d /root/dist ] && [ -f /root/dist/bin/curvine-worker.sh ] && echo 'dist正确' || echo 'dist有问题'"
```

## 🔄 完整的重新部署流程

如果要彻底重新部署Worker节点：

```bash
# 1. 停止服务
ansible-playbook stop_services.yml --limit worker

# 2. 清理旧文件
ansible worker -m shell -a "rm -rf /root/dist /root/dist1 /root/dist*.tar.gz"

# 3. 重新部署
ansible-playbook deploy_curvine.yml --limit worker

# 4. 启动服务
ansible-playbook start_services.yml --limit worker

# 5. 检查状态
ansible-playbook status_services.yml --limit worker
```

## 🆘 如果还是有问题

1. **运行诊断**：
```bash
ansible-playbook diagnose_worker.yml --limit 10.200.3.15
```

2. **完全重新部署**：
```bash
ansible-playbook fix_worker.yml --limit 10.200.3.15
```

3. **查看详细日志**：
```bash
ansible worker -m shell -a "journalctl -u curvine-worker -n 50 --no-pager" --limit 10.200.3.15
```

4. **检查systemd配置**：
```bash
ansible worker -m shell -a "cat /etc/systemd/system/curvine-worker.service" --limit 10.200.3.15
```

确认 `WorkingDirectory` 和 `ExecStart` 路径都指向 `/root/dist/`

## 📝 总结

**最快的解决方案**：
```bash
ansible-playbook fix_dist_path.yml --limit <worker-ip>
```

**最彻底的解决方案**：
```bash
ansible-playbook fix_worker.yml --limit <worker-ip>
```

修复后应该看到：
```
✓ /root/dist 目录存在
✓ /root/dist/bin/curvine-worker.sh 可执行
✓ /root/dist/lib/curvine-server 可执行
✓ curvine-worker 服务running
✓ curvine-fuse 服务running
```

