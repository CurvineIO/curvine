# 故障排查和常见问题

## ❗ 重要提示

### SSH免密登录必须配置所有节点

**问题症状**：部署脚本显示"所有节点连接正常"，但实际上worker节点没有配置SSH免密登录，导致部署失败。

**原因**：您只配置了master节点的SSH免密登录，worker节点还未配置。

**解决方案**：

```bash
# 方法1：使用交互式脚本配置所有节点
bash setup_ssh_batch.sh

# 方法2：使用ansible playbook分别配置
# 先配置master节点
ansible-playbook setup_ssh.yml --limit master
# 再配置worker节点  
ansible-playbook setup_ssh.yml --limit worker

# 方法3：如果master和worker密码不同
ansible-playbook setup_ssh.yml --limit master --extra-vars "ansible_password=master_password"
ansible-playbook setup_ssh.yml --limit worker --extra-vars "ansible_password=worker_password"
```

**验证连接**：

```bash
# 测试所有节点
ansible all -m ping

# 分别测试
ansible master -m ping
ansible worker -m ping

# 查看节点列表
ansible all --list-hosts
ansible master --list-hosts
ansible worker --list-hosts
```

---

## 🔍 常见问题

### 1. Worker节点配置文件不存在

**错误信息**：
```
fatal: [10.200.3.15]: FAILED! => {"changed": false, "msg": "file not found: /root/dist/conf/curvine-cluster.toml"}
```

**原因**：
- dist1.tar.gz（worker节点安装包）的目录结构与dist.tar.gz不同
- worker节点可能不需要配置文件，或配置文件路径不同

**解决方案**：

脚本已经更新，会自动检查配置文件是否存在，如果不存在则跳过配置更新步骤。

如果worker节点确实需要配置文件，请检查：

```bash
# 在worker节点上检查解压后的目录结构
ssh root@<worker-ip>
cd /root/dist
ls -la
find . -name "*.toml"
```

如果配置文件在不同位置，需要手动调整 `group_vars/all.yml` 中的 `curvine_config_file` 路径。

---

### 2. 部署脚本连接测试改进

新版本的 `deploy_all.sh` 会分别显示master和worker节点的连接状态：

```
检查Master节点...
  ✓ Master节点 (3 个) 连接正常
检查Worker节点...
  ✗ Worker节点连接失败
```

这样可以清楚地知道哪些节点有问题。

---

### 3. 网卡检测优先级

脚本会按以下优先级自动检测Master节点的IP：

1. **bond0** - 首选
2. **eth0** - 备选
3. **默认网卡** - 最后备选

部署时会显示：
```
Master节点IP: 10.200.3.14 (来源: eth0)
```

查看网卡信息：
```bash
# 检查所有master节点的网卡
ansible master -m shell -a "ip addr show | grep 'inet '"

# 检查特定网卡
ansible master -m shell -a "ip addr show bond0 || ip addr show eth0"
```

---

### 4. 环境变量配置说明

**Master节点**：
```bash
export CURVINE_MASTER_HOSTNAME=<本机bond0或eth0的IP>
```

**Worker节点**：
```bash
export CURVINE_MASTER_HOSTNAME=localhost
```

验证环境变量：
```bash
# 查看所有节点的环境变量
ansible all -m shell -a "grep CURVINE /etc/profile"

# 查看当前生效的环境变量
ansible all -m shell -a "source /etc/profile && echo \$CURVINE_MASTER_HOSTNAME"
```

---

### 5. dist1.tar.gz 和 dist.tar.gz 的区别

**dist.tar.gz** (Master节点)：
- 包含完整的master、worker、fuse组件
- 包含完整的配置文件
- 包含Web界面

**dist1.tar.gz** (Worker节点)：
- 可能只包含worker、fuse组件
- 可能不包含配置文件（从master节点获取配置）
- 不包含Web界面

如果不确定包的内容，可以先解压查看：

```bash
# 在控制节点上
mkdir -p /tmp/check-dist
tar -tzf /root/dist.tar.gz | head -20
tar -tzf /root/dist1.tar.gz | head -20

# 或完全解压查看
cd /tmp/check-dist
tar -xzf /root/dist1.tar.gz
ls -la dist/
```

---

### 6. 服务启动顺序

正确的启动顺序（脚本已自动处理）：

1. **Master节点**：
   - curvine-master (首先启动)
   - curvine-worker
   - curvine-fuse

2. **Worker节点**：
   - 等待master启动完成
   - curvine-worker
   - curvine-fuse

如果手动启动，请遵循此顺序。

---

### 7. 重新部署

如果部署失败需要重新部署：

```bash
# 方法1：完全卸载后重新部署
ansible-playbook uninstall.yml
bash deploy_all.sh

# 方法2：只重新配置失败的节点
ansible-playbook deploy_curvine.yml --limit <failed-host-ip>

# 方法3：只重新配置worker节点
ansible-playbook deploy_curvine.yml --limit worker
```

---

### 8. 查看详细日志

```bash
# 部署时显示详细输出
ansible-playbook deploy_curvine.yml -v   # 一般详细
ansible-playbook deploy_curvine.yml -vv  # 更详细
ansible-playbook deploy_curvine.yml -vvv # 最详细（调试）

# 查看服务日志
ansible all -m shell -a "journalctl -u curvine-master -n 50 --no-pager"
ansible all -m shell -a "journalctl -u curvine-worker -n 50 --no-pager"
ansible all -m shell -a "journalctl -u curvine-fuse -n 50 --no-pager"

# 实时查看日志（在目标节点上）
ssh root@<node-ip>
journalctl -u curvine-worker -f
```

---

### 9. 检查部署状态

```bash
# 检查所有节点的目录结构
ansible all -m shell -a "ls -la /root/dist/"

# 检查服务文件是否存在
ansible all -m shell -a "ls -la /etc/systemd/system/curvine-*.service"

# 检查服务状态
ansible-playbook status_services.yml

# 或手动检查
ansible all -m shell -a "systemctl status curvine-worker --no-pager"
```

---

### 10. hosts.ini 配置示例

确保hosts.ini正确配置：

```ini
[master]
10.200.3.3
10.200.3.8
10.200.3.14

[worker]
10.200.3.15
10.200.3.16

[all:vars]
ansible_user=root
ansible_port=22
# 如果需要，可以为每个节点单独设置密码
# 或者在这里设置统一密码（不推荐提交到版本控制）
# ansible_password=your_password
```

为不同节点设置不同密码：

```ini
[master]
10.200.3.3 ansible_password=pass1
10.200.3.8 ansible_password=pass2
10.200.3.14 ansible_password=pass3

[worker]
10.200.3.15 ansible_password=pass4
10.200.3.16 ansible_password=pass5
```

---

## 📞 获取帮助

如果问题仍未解决：

1. **收集信息**：
```bash
# 保存所有输出到文件
ansible all -m setup > ansible-facts.txt
ansible all -m shell -a "systemctl status curvine-* --no-pager" > service-status.txt
ansible all -m shell -a "journalctl -u curvine-* -n 100 --no-pager" > service-logs.txt
```

2. **检查网络**：
```bash
# 检查节点间网络连通性
ansible all -m shell -a "ping -c 3 <master-ip>"
```

3. **检查防火墙**：
```bash
# 检查防火墙状态
ansible all -m shell -a "systemctl status firewalld"
ansible all -m shell -a "iptables -L -n | head -20"
```

4. 查阅文档：
   - README.md - 完整功能说明
   - USAGE.md - 详细使用指南
   - QUICKSTART.md - 快速开始

5. 参考Curvine官方文档：https://curvineio.github.io

---

## ✅ 部署检查清单

在部署前，请确认：

- [ ] 所有节点在 hosts.ini 中正确配置
- [ ] **所有节点（master和worker）都配置了SSH免密登录**
- [ ] /root/dist.tar.gz 存在且完整
- [ ] /root/dist1.tar.gz 存在且完整
- [ ] 已安装Ansible (>= 2.9)
- [ ] 所有节点网络互通
- [ ] 所有节点有足够的磁盘空间
- [ ] 目标节点已安装FUSE库
- [ ] 防火墙规则允许必要的端口通信

部署后验证：

- [ ] ansible all -m ping 所有节点都返回SUCCESS
- [ ] systemctl status curvine-* 显示服务运行正常
- [ ] 可以访问 http://<master-ip>:9000
- [ ] /root/dist/bin/cv report 显示集群状态正常

