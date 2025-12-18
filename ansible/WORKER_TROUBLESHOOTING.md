# Worker节点故障排查指南

## 🚨 常见问题

### Worker服务启动失败（状态码203/EXEC）

**症状**：
```
Process: 54417 ExecStart=/root/dist/bin/curvine-worker.sh start (code=exited, status=203/EXEC)
```

**可能原因**：
1. `dist1.tar.gz` 包结构不正确，缺少必要文件
2. 启动脚本没有执行权限
3. 启动脚本的shebang行有问题
4. 依赖的二进制文件不存在或不可执行

## 🔍 诊断步骤

### 步骤1：运行诊断工具

```bash
# 诊断特定Worker节点
ansible-playbook diagnose_worker.yml --limit 10.200.3.15

# 诊断所有Worker节点
ansible-playbook diagnose_worker.yml
```

**诊断工具会检查**：
- ✅ 安装目录是否存在
- ✅ bin目录是否存在
- ✅ 启动脚本是否存在和可执行
- ✅ 二进制文件是否存在
- ✅ 配置文件是否存在
- ✅ 手动执行启动脚本的结果
- ✅ systemd日志

### 步骤2：查看诊断结果

重点关注：
```
Worker启动脚本状态:
- 存在: True/False
- 可执行: True/False
- 权限: 0755

curvine-server二进制文件状态:
- 存在: True/False
- 可执行: True/False
```

### 步骤3：检查dist1.tar.gz内容

在控制节点上：
```bash
# 查看压缩包内容
tar -tzf /root/dist1.tar.gz | head -20

# 检查是否包含必要文件
tar -tzf /root/dist1.tar.gz | grep -E "(bin/curvine-worker.sh|lib/curvine-server)"
```

**必须包含的文件**：
- `dist/bin/curvine-worker.sh`
- `dist/bin/curvine-fuse.sh`
- `dist/lib/curvine-server`
- `dist/conf/curvine-cluster.toml` (可选，如果Worker需要)

## 🔧 修复方法

### 方法1：使用自动修复工具（推荐）

```bash
# 修复特定Worker节点
ansible-playbook fix_worker.yml --limit 10.200.3.15

# 修复所有Worker节点
ansible-playbook fix_worker.yml
```

**修复工具会执行**：
1. 停止所有服务
2. 备份旧安装目录
3. 重新拷贝和解压 `dist1.tar.gz`
4. 设置正确的文件权限（bin和lib目录）
5. 重新启动服务
6. 显示详细的诊断信息和最终状态

### 方法2：手动修复

在Worker节点上：

```bash
# 1. 停止服务
systemctl stop curvine-worker curvine-fuse

# 2. 备份旧安装
mv /root/dist /root/dist.backup.$(date +%Y%m%d)

# 3. 重新解压
tar -xzf /root/dist1.tar.gz -C /root/

# 4. 设置权限
chmod -R 755 /root/dist/bin/
chmod -R 755 /root/dist/lib/

# 5. 验证文件
ls -la /root/dist/bin/curvine-worker.sh
ls -la /root/dist/lib/curvine-server
file /root/dist/bin/curvine-worker.sh

# 6. 手动测试启动
bash -x /root/dist/bin/curvine-worker.sh start

# 7. 启动服务
systemctl start curvine-worker
systemctl start curvine-fuse

# 8. 检查状态
systemctl status curvine-worker
systemctl status curvine-fuse
```

### 方法3：检查dist1.tar.gz是否正确

如果 `dist1.tar.gz` 内容不完整：

**选项A：使用完整的dist.tar.gz**
```bash
# 在控制节点上
cp /root/dist.tar.gz /root/dist1.tar.gz

# 重新部署
ansible-playbook fix_worker.yml --limit worker
```

**选项B：重新制作Worker安装包**
```bash
# 在有完整安装的节点上
cd /root
tar -czf dist1.tar.gz dist/

# 拷贝到控制节点
scp dist1.tar.gz root@<control-node>:/root/
```

## 📋 验证修复

修复后验证：

```bash
# 1. 检查服务状态
ansible-playbook status_services.yml --limit 10.200.3.15

# 2. 查看服务日志
ansible worker -m shell -a "journalctl -u curvine-worker -n 30 --no-pager" --limit 10.200.3.15

# 3. 检查进程
ansible worker -m shell -a "ps aux | grep curvine" --limit 10.200.3.15

# 4. 测试FUSE挂载
ansible worker -m shell -a "ls -la /curvine-fuse" --limit 10.200.3.15
```

## 🔬 深度诊断

### 检查启动脚本

```bash
# 在Worker节点上
cat /root/dist/bin/curvine-worker.sh

# 检查shebang行
head -n 1 /root/dist/bin/curvine-worker.sh

# 检查脚本语法
bash -n /root/dist/bin/curvine-worker.sh
```

### 检查二进制文件

```bash
# 检查文件类型
file /root/dist/lib/curvine-server

# 检查依赖库
ldd /root/dist/lib/curvine-server

# 尝试直接运行
/root/dist/lib/curvine-server --help
```

### 检查systemd服务配置

```bash
# 查看服务文件
cat /etc/systemd/system/curvine-worker.service

# 检查配置语法
systemd-analyze verify curvine-worker.service

# 查看详细日志
journalctl -u curvine-worker -xe
```

## 🆘 如果问题仍未解决

### 收集诊断信息

```bash
# 在Worker节点上运行
mkdir -p /tmp/curvine-debug
cd /tmp/curvine-debug

# 收集文件列表
find /root/dist -type f -ls > files.txt

# 收集权限信息
ls -laR /root/dist > permissions.txt

# 收集服务日志
journalctl -u curvine-worker --no-pager > worker.log
journalctl -u curvine-fuse --no-pager > fuse.log

# 收集systemd配置
cp /etc/systemd/system/curvine-*.service .

# 收集配置文件
cp /root/dist/conf/curvine-cluster.toml . 2>/dev/null || echo "No config"

# 打包
tar -czf /tmp/curvine-debug.tar.gz .
```

### 常见解决方案

1. **dist1.tar.gz和dist.tar.gz应该相同**
   - Worker节点和Master节点通常使用相同的安装包
   - 只是启动不同的服务而已

2. **环境变量CURVINE_MASTER_HOSTNAME**
   - Master节点：设置为自己的IP
   - Worker节点：设置为`localhost`或Master节点的IP
   - 检查：`grep CURVINE /etc/profile`

3. **数据目录权限**
   - 确保 `/data/data` 目录存在且可写
   - `mkdir -p /data/data && chmod 755 /data/data`

4. **网络连通性**
   - Worker需要能连接到Master节点的8995端口
   - 测试：`telnet <master-ip> 8995`

## 📞 获取帮助

如果问题仍未解决，请提供：
1. `diagnose_worker.yml` 的完整输出
2. `/tmp/curvine-debug.tar.gz` 文件
3. dist1.tar.gz 的来源和创建方法
4. 集群架构和网络拓扑

参考Curvine官方文档：https://curvineio.github.io

