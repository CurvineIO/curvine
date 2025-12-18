# Curvine 集群自动化部署工具

基于Ansible的Curvine分布式缓存系统自动化部署和服务化工具。

## 📋 功能特性

- ✅ SSH免密登录配置（支持统一密码或多个密码）
- ✅ 自动检测并配置master节点的bond0网卡IP
- ✅ 环境变量自动配置（/etc/profile）
- ✅ Worker节点主机名环境变量自动配置（CURVINE_WORKER_HOSTNAME）🆕
- ✅ 自动分发和解压安装包
- ✅ 配置文件自动更新（支持多个data_dir）
- ✅ systemd服务化（curvine-master、curvine-worker、curvine-fuse）
- ✅ 服务启动、停止、重启、状态查看

## 📁 文件结构

```
.
├── ansible.cfg              # Ansible配置文件
├── hosts.ini                # 主机清单（需要手动填写IP）
├── setup_ssh.yml            # SSH免密登录配置
├── deploy_curvine.yml       # 主部署脚本
├── start_services.yml       # 启动服务
├── stop_services.yml        # 停止服务
├── restart_services.yml     # 重启服务
├── status_services.yml      # 查看服务状态
├── update_config.yml        # 更新配置文件
├── setup_worker_hostname.yml # 设置Worker主机名环境变量
├── uninstall.yml            # 卸载集群
├── README.md                # 本文件
├── QUICKSTART.md            # 快速开始指南
├── USAGE.md                 # 详细使用手册
├── TROUBLESHOOTING.md       # 故障排查指南⭐
└── CHANGELOG.md             # 更新日志
```

**⚠️ 重要文档**：
- 📖 **TROUBLESHOOTING.md** - 遇到问题必看！包含常见问题和解决方案
- 📖 **DISK_FORMAT_FEATURE.md** - 磁盘格式化功能说明 🆕
- 📖 **NODE_INFO_FEATURE.md** - 节点信息收集功能说明 🆕
- 📖 **WORKER_HOSTNAME_FEATURE.md** - Worker主机名环境变量功能说明 🆕
- 📖 **QUICKSTART.md** - 5分钟快速部署指南
- 📖 **USAGE.md** - 详细使用手册和最佳实践

## 🚀 快速开始

### 1. 准备工作

#### 安装Ansible

**CentOS/RHEL:**
```bash
yum install -y ansible
```

**Ubuntu/Debian:**
```bash
apt-get update
apt-get install -y ansible
```

#### 准备安装包

确保以下文件存在：
- `/root/dist.tar.gz` - Master节点安装包
- `/root/dist1.tar.gz` - Worker节点安装包

### 2. 配置主机清单

编辑 `hosts.ini` 文件，填写节点IP地址：

```ini
[master]
192.168.1.10
192.168.1.11

[worker]
192.168.1.20
192.168.1.21
192.168.1.22

[all:vars]
ansible_user=root
ansible_port=22
```

### 3. 配置SSH免密登录

**所有节点使用相同密码：**
```bash
ansible-playbook setup_ssh.yml
# 根据提示输入统一密码
```

**不同节点使用不同密码：**
```bash
# 方法1：为特定主机组设置
ansible-playbook setup_ssh.yml --extra-vars "ansible_password=password1" --limit master
ansible-playbook setup_ssh.yml --extra-vars "ansible_password=password2" --limit worker

# 方法2：在hosts.ini中为每个主机单独设置
# 在hosts.ini中添加：
# 192.168.1.10 ansible_password=password1
# 192.168.1.20 ansible_password=password2
```

### 4. 部署Curvine集群

```bash
ansible-playbook deploy_curvine.yml
```

此步骤将完成：
- ✅ 配置环境变量（CURVINE_MASTER_HOSTNAME、CURVINE_WORKER_HOSTNAME）
- ✅ 拷贝并解压安装包
- ✅ 配置data_dir
- ✅ 创建systemd服务
- ✅ 启用服务开机自启动

### 5. 启动服务

```bash
ansible-playbook start_services.yml
```

### 6. 验证部署

```bash
# 查看所有节点服务状态
ansible-playbook status_services.yml

# 访问Web界面
# 浏览器打开: http://<master-ip>:9000
```

## ⚙️ 高级配置

### 自定义数据目录

**方法1：在部署时指定**

编辑 `deploy_curvine.yml`，修改 `data_dirs` 变量：

```yaml
vars:
  data_dirs:
    - "[SSD]/data/data1"
    - "[SSD]/data/data2"
    - "[HDD]/data/data3"
```

**方法2：使用update_config.yml单独更新**

```bash
ansible-playbook update_config.yml -e 'data_dirs=["[SSD]/data/data1","[HDD]/data/data2"]'
```

更新配置后需要重启服务：
```bash
ansible-playbook restart_services.yml
```

### 自定义安装目录

编辑playbook文件，修改 `curvine_install_dir` 变量：

```yaml
vars:
  curvine_install_dir: /opt/curvine  # 默认是 /root/dist
```

### 自定义安装包路径

编辑 `deploy_curvine.yml`，修改以下变量：

```yaml
vars:
  master_dist_file: /path/to/your/dist.tar.gz
  worker_dist_file: /path/to/your/dist1.tar.gz
```

## 🔧 常用命令

### 环境变量管理

```bash
# 设置或更新Worker节点主机名环境变量
ansible-playbook setup_worker_hostname.yml

# 验证Worker节点主机名环境变量
ansible worker -m shell -a "grep CURVINE_WORKER_HOSTNAME /etc/profile"
```

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

### 单独管理某个节点组

```bash
# 只启动master节点服务
ansible-playbook start_services.yml --limit master

# 只重启worker节点服务
ansible-playbook restart_services.yml --limit worker

# 只查看特定IP节点的状态
ansible-playbook status_services.yml --limit 192.168.1.10
```

### 直接使用systemctl管理（在目标节点上）

```bash
# Master节点
systemctl start curvine-master
systemctl start curvine-worker
systemctl start curvine-fuse

# Worker节点
systemctl start curvine-worker
systemctl start curvine-fuse

# 查看状态
systemctl status curvine-master
systemctl status curvine-worker
systemctl status curvine-fuse
```

## 📊 服务说明

### curvine-master（仅Master节点）
- **功能**: 元数据管理、worker节点协调、负载均衡
- **启动脚本**: `/root/dist/bin/curvine-master.sh start`
- **环境变量**: `CURVINE_MASTER_HOSTNAME=<bond0_ip>`

### curvine-worker（所有节点）
- **功能**: 数据存储和处理
- **启动脚本**: `/root/dist/bin/curvine-worker.sh start`
- **环境变量**: 
  - Master节点: `CURVINE_MASTER_HOSTNAME=<bond0_ip>`
  - Worker节点: 
    - `CURVINE_MASTER_HOSTNAME=localhost`
    - `CURVINE_WORKER_HOSTNAME=<worker_ip>` (自hosts.ini)

### curvine-fuse（所有节点）
- **功能**: POSIX文件系统接口
- **启动脚本**: `/root/dist/bin/curvine-fuse.sh start`
- **挂载点**: `/curvine-fuse`（默认）

## 🐛 故障排查

### 1. SSH连接失败

```bash
# 测试连接
ansible all -m ping

# 如果失败，检查：
# - 网络连通性: ping <target_ip>
# - SSH服务: ssh root@<target_ip>
# - 防火墙规则
```

### 2. 网卡检测

脚本会按照以下优先级自动检测Master节点的IP地址：
1. **bond0网卡** - 首选
2. **eth0网卡** - 如果bond0不存在
3. **默认网卡** - 如果bond0和eth0都不存在

部署时会显示使用的网卡来源。如需手动指定IP，可以编辑 `deploy_curvine.yml`：
```yaml
- name: Manually set master IP
  set_fact:
    bond0_ip: "192.168.1.10"  # 替换为实际IP
  when: "'master' in group_names"
```

### 3. 服务启动失败

```bash
# 查看详细日志
ansible all -m shell -a "journalctl -u curvine-master -n 50 --no-pager"
ansible all -m shell -a "journalctl -u curvine-worker -n 50 --no-pager"
ansible all -m shell -a "journalctl -u curvine-fuse -n 50 --no-pager"

# 检查配置文件
ansible all -m shell -a "cat /root/dist/conf/curvine-cluster.toml"

# 检查环境变量
ansible all -m shell -a "env | grep CURVINE"

# 验证Worker节点主机名环境变量
ansible worker -m shell -a "grep CURVINE_WORKER_HOSTNAME /etc/profile"
```

### 4. 文件传输失败

```bash
# 检查安装包是否存在
ls -lh /root/dist.tar.gz
ls -lh /root/dist1.tar.gz

# 检查目标节点磁盘空间
ansible all -m shell -a "df -h /root"
```

### 5. 配置文件更新失败

```bash
# 检查配置文件备份
ansible all -m shell -a "ls -lt /root/dist/conf/curvine-cluster.toml*"

# 手动验证配置
ansible all -m shell -a "grep data_dir /root/dist/conf/curvine-cluster.toml"
```

## 📝 注意事项

1. **环境变量配置**：脚本会在 `/etc/profile` 末尾追加环境变量，不会影响现有配置

2. **配置文件备份**：每次修改配置文件都会自动创建备份，备份文件包含时间戳

3. **服务启动顺序**：
   - Master节点：master → worker → fuse
   - Worker节点：worker → fuse
   
4. **网络要求**：
   - 控制节点需要能SSH访问所有目标节点
   - Master节点之间需要网络互通（Raft共识）
   - Worker节点需要能访问Master节点

5. **权限要求**：所有操作需要root权限

## 🔗 参考资源

- [Curvine官方文档](https://curvineio.github.io)
- [Curvine GitHub仓库](https://github.com/CurvineIO/curvine)
- [Ansible文档](https://docs.ansible.com/)

## 📧 支持

如有问题，请参考Curvine官方文档或提交Issue。

---

**License**: Apache-2.0

