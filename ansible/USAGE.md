# Curvine集群部署使用指南

## 📋 部署前检查清单

- [ ] 已安装Ansible（版本 >= 2.9）
- [ ] 准备好master节点安装包 `/root/dist.tar.gz`
- [ ] 准备好worker节点安装包 `/root/dist1.tar.gz`
- [ ] 所有目标节点的IP地址和SSH访问信息
- [ ] 确认目标节点的bond0网卡或默认网卡配置
- [ ] 确认数据存储目录路径

## 🚀 快速部署流程

### 方式1：使用一键部署脚本（推荐）

```bash
# 1. 修改hosts.ini，填写节点IP
vim hosts.ini

# 2. 运行一键部署脚本
bash deploy_all.sh
```

该脚本会自动：
- 检查SSH连接
- 提示配置免密登录（如需要）
- 执行完整部署
- 询问是否启动服务

### 方式2：分步执行

#### 步骤1：配置主机清单

编辑 `hosts.ini` 文件：

```ini
[master]
192.168.1.10
192.168.1.11

[worker]
192.168.1.20
192.168.1.21

[all:vars]
ansible_user=root
ansible_port=22
```

#### 步骤2：配置SSH免密登录

**选项A：所有节点使用相同密码**

```bash
ansible-playbook setup_ssh.yml
# 根据提示输入密码
```

**选项B：不同节点组使用不同密码**

```bash
bash setup_ssh_batch.sh
# 选择选项2，分别输入master和worker密码
```

**选项C：每个节点使用不同密码**

在 `hosts.ini` 中为每个节点指定密码：

```ini
[master]
192.168.1.10 ansible_password=password1
192.168.1.11 ansible_password=password2

[worker]
192.168.1.20 ansible_password=password3
```

然后运行：

```bash
ansible-playbook setup_ssh.yml
```

#### 步骤3：测试连接

```bash
ansible all -m ping
```

期望输出：
```
192.168.1.10 | SUCCESS => {
    "changed": false,
    "ping": "pong"
}
...
```

#### 步骤4：配置数据目录（可选）

如需自定义数据目录，编辑 `group_vars/all.yml`：

```yaml
data_dirs:
  - "[SSD]/data/data1"
  - "[SSD]/data/data2"
  - "[HDD]/data/data3"
```

#### 步骤5：执行部署

```bash
ansible-playbook deploy_curvine.yml
```

部署过程会：
1. 检测并配置环境变量
2. 拷贝和解压安装包
3. 配置data_dir
4. 创建systemd服务
5. 启用服务开机自启动

#### 步骤6：启动服务

```bash
ansible-playbook start_services.yml
```

#### 步骤7：验证部署

```bash
# 查看服务状态
ansible-playbook status_services.yml

# 或者在任一master节点上执行
ssh root@<master-ip>
/root/dist/bin/cv report
```

## 🔧 日常运维操作

### 服务管理

```bash
# 启动所有服务
ansible-playbook start_services.yml

# 停止所有服务
ansible-playbook stop_services.yml

# 重启所有服务
ansible-playbook restart_services.yml

# 查看服务状态
ansible-playbook status_services.yml
```

### 针对特定节点组操作

```bash
# 只操作master节点
ansible-playbook start_services.yml --limit master

# 只操作worker节点
ansible-playbook restart_services.yml --limit worker

# 操作特定IP的节点
ansible-playbook status_services.yml --limit 192.168.1.10

# 操作多个特定节点
ansible-playbook start_services.yml --limit "192.168.1.10,192.168.1.20"
```

### 配置更新

**更新数据目录配置：**

```bash
# 方式1：修改group_vars/all.yml后重新部署
vim group_vars/all.yml
ansible-playbook update_config.yml
ansible-playbook restart_services.yml

# 方式2：命令行直接指定
ansible-playbook update_config.yml -e 'data_dirs=["[SSD]/data1","[SSD]/data2","[HDD]/data3"]'
ansible-playbook restart_services.yml
```

### 查看日志

```bash
# 查看master服务日志
ansible master -m shell -a "journalctl -u curvine-master -n 100 --no-pager"

# 查看worker服务日志
ansible all -m shell -a "journalctl -u curvine-worker -n 100 --no-pager"

# 查看fuse服务日志
ansible all -m shell -a "journalctl -u curvine-fuse -n 100 --no-pager"

# 实时查看日志（在目标节点上）
ssh root@<node-ip>
journalctl -u curvine-master -f
```

### 配置文件管理

```bash
# 备份所有节点的配置文件
ansible all -m fetch -a "src=/root/dist/conf/curvine-cluster.toml dest=./backup/ flat=no"

# 查看当前配置
ansible all -m shell -a "cat /root/dist/conf/curvine-cluster.toml"

# 查看环境变量
ansible all -m shell -a "grep CURVINE /etc/profile"
```

## 🔍 故障排查

### 问题1：SSH连接失败

```bash
# 测试网络连通性
ansible all -m ping

# 如果失败，尝试：
# 1. 检查防火墙
ansible all -m shell -a "systemctl status firewalld"

# 2. 检查SSH服务
ansible all -m shell -a "systemctl status sshd"

# 3. 手动SSH测试
ssh root@<target-ip>
```

### 问题2：服务启动失败

```bash
# 查看详细状态
ansible all -m shell -a "systemctl status curvine-master curvine-worker curvine-fuse"

# 查看错误日志
ansible all -m shell -a "journalctl -xe | tail -100"

# 检查端口占用
ansible all -m shell -a "netstat -tulpn | grep curvine"

# 检查进程
ansible all -m shell -a "ps aux | grep curvine"
```

### 问题3：网卡检测

脚本会自动按照优先级检测Master节点的IP地址：
1. **bond0网卡** - 首选
2. **eth0网卡** - 如果bond0不存在
3. **默认网卡** - 如果bond0和eth0都不存在

查看检测到的网卡信息：
```bash
ansible master -m shell -a "ip addr show bond0 || ip addr show eth0 || ip addr"
```

如需手动指定IP，编辑 `deploy_curvine.yml`：

```yaml
- name: Manually set master IP
  set_fact:
    bond0_ip: "192.168.1.10"  # 使用实际的master IP
  when: "'master' in group_names"
```

### 问题4：磁盘空间不足

```bash
# 检查磁盘空间
ansible all -m shell -a "df -h"

# 清理旧的安装包
ansible all -m shell -a "rm -f /root/dist*.tar.gz"

# 清理日志
ansible all -m shell -a "journalctl --vacuum-time=7d"
```

### 问题5：配置文件格式错误

```bash
# 查看配置文件备份
ansible all -m shell -a "ls -la /root/dist/conf/curvine-cluster.toml*"

# 恢复配置文件
ansible all -m shell -a "cp /root/dist/conf/curvine-cluster.toml.backup /root/dist/conf/curvine-cluster.toml"
```

## 📊 监控和检查

### 集群状态检查

```bash
# 在master节点上执行
ssh root@<master-ip>
/root/dist/bin/cv report

# 查看文件系统
ls -la /curvine-fuse

# 测试文件操作
/root/dist/bin/cv fs ls /
/root/dist/bin/cv fs mkdir /test
/root/dist/bin/cv fs ls /
```

### 性能监控

```bash
# 查看系统资源使用
ansible all -m shell -a "top -bn1 | head -20"

# 查看网络连接
ansible all -m shell -a "ss -tunlp"

# 查看IO状态
ansible all -m shell -a "iostat -x 1 3"
```

### Web界面访问

访问任一master节点的Web界面：

```
http://<master-ip>:9000
```

## 🔄 升级和更新

### 升级Curvine版本

```bash
# 1. 准备新版本的安装包
# 将新的dist.tar.gz和dist1.tar.gz放到/root/目录

# 2. 停止服务
ansible-playbook stop_services.yml

# 3. 备份当前版本
ansible all -m shell -a "mv /root/dist /root/dist.backup.$(date +%Y%m%d)"

# 4. 重新部署
ansible-playbook deploy_curvine.yml

# 5. 启动服务
ansible-playbook start_services.yml
```

### 回滚到旧版本

```bash
# 1. 停止服务
ansible-playbook stop_services.yml

# 2. 恢复旧版本
ansible all -m shell -a "rm -rf /root/dist && mv /root/dist.backup.* /root/dist"

# 3. 启动服务
ansible-playbook start_services.yml
```

## 🗑️ 卸载

### 完全卸载Curvine集群

```bash
# 执行卸载脚本
ansible-playbook uninstall.yml
```

该操作会：
- 停止所有服务
- 删除systemd服务文件
- 删除安装目录
- 清理环境变量

**注意：数据目录不会被自动删除，需要手动清理**

```bash
# 手动清理数据目录（谨慎操作！）
ansible all -m shell -a "rm -rf /data/data"
```

## 📝 最佳实践

### 1. 节点规划

- Master节点：建议3个或5个（奇数，用于Raft共识）
- Worker节点：根据实际需求配置
- 所有节点网络互通

### 2. 存储规划

```yaml
# 推荐的数据目录配置
data_dirs:
  - "[SSD]/data/cache1"      # 高速缓存
  - "[SSD]/data/cache2"      # 高速缓存
  - "[HDD]/data/storage1"    # 大容量存储
  - "[HDD]/data/storage2"    # 大容量存储
```

### 3. 服务启动顺序

正确的启动顺序：
1. Master节点的curvine-master
2. Master节点的curvine-worker
3. Worker节点的curvine-worker
4. 所有节点的curvine-fuse

（ansible playbook已经自动处理了启动顺序）

### 4. 定期备份

```bash
# 备份配置文件
ansible all -m fetch -a "src=/root/dist/conf/curvine-cluster.toml dest=./backup/$(date +%Y%m%d)/ flat=no"

# 备份元数据（在master节点上）
ssh root@<master-ip>
tar czf /root/curvine-metadata-backup-$(date +%Y%m%d).tar.gz /root/dist/data/
```

### 5. 监控告警

建议配置监控系统监控以下指标：
- 服务状态（systemctl status）
- CPU和内存使用率
- 磁盘空间
- 网络连接状态
- 日志错误信息

## 🆘 获取帮助

- Curvine官方文档: https://curvineio.github.io
- GitHub仓库: https://github.com/CurvineIO/curvine
- Ansible文档: https://docs.ansible.com/

## 📞 技术支持

如遇到无法解决的问题：

1. 收集以下信息：
   - 错误日志（journalctl输出）
   - 配置文件（curvine-cluster.toml）
   - 系统环境（uname -a, free -h, df -h）
   - Ansible版本（ansible --version）

2. 查看Curvine官方文档和GitHub Issues

3. 联系技术支持团队

