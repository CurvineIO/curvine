# Curvine 快速开始指南

## ⚡ 5分钟快速部署

### 前提条件

```bash
# 确保已安装Ansible
ansible --version

# 确保安装包存在
ls -lh /root/dist.tar.gz /root/dist1.tar.gz
```

### 部署步骤

```bash
# 1. 编辑主机清单
vim hosts.ini
# 填写master和worker节点的IP地址

# 2. 一键部署
bash deploy_all.sh
# 按照提示操作即可
```

## 📝 手动部署（分步）

```bash
# 1. 配置SSH免密登录
ansible-playbook setup_ssh.yml
# 输入密码

# 2. 测试连接
ansible all -m ping

# 3. 执行部署
ansible-playbook deploy_curvine.yml

# 4. 启动服务
ansible-playbook start_services.yml

# 5. 查看状态
ansible-playbook status_services.yml
```

## 🎯 常用命令速查

### 服务管理
```bash
ansible-playbook start_services.yml      # 启动
ansible-playbook stop_services.yml       # 停止
ansible-playbook restart_services.yml    # 重启
ansible-playbook status_services.yml     # 状态
```

### 配置管理
```bash
# 更新数据目录
ansible-playbook update_config.yml -e 'data_dirs=["[SSD]/data1","[HDD]/data2"]'

# 重启使配置生效
ansible-playbook restart_services.yml
```

### 针对特定节点
```bash
ansible-playbook start_services.yml --limit master         # 只操作master
ansible-playbook restart_services.yml --limit worker       # 只操作worker
ansible-playbook status_services.yml --limit 192.168.1.10  # 特定IP
```

### 日志查看
```bash
# Master日志
ansible master -m shell -a "journalctl -u curvine-master -n 50"

# Worker日志
ansible all -m shell -a "journalctl -u curvine-worker -n 50"

# FUSE日志
ansible all -m shell -a "journalctl -u curvine-fuse -n 50"
```

### 集群检查
```bash
# 在master节点上
ssh root@<master-ip>
/root/dist/bin/cv report
/root/dist/bin/cv fs ls /
```

## 🔧 配置文件位置

| 文件 | 用途 |
|------|------|
| `hosts.ini` | 主机清单 |
| `group_vars/all.yml` | 全局变量配置 |
| `ansible.cfg` | Ansible配置 |

## 📊 Web界面

```
http://<master-ip>:9000
```

## 🐛 快速故障排查

### SSH连接失败
```bash
ansible all -m ping
ssh root@<target-ip>  # 手动测试
```

### 服务无法启动
```bash
ansible all -m shell -a "systemctl status curvine-master"
ansible all -m shell -a "journalctl -xe | tail -50"
```

### 检查环境变量
```bash
ansible all -m shell -a "grep CURVINE /etc/profile"
ansible all -m shell -a "env | grep CURVINE"
```

### 检查网卡（Master节点）
```bash
# 脚本自动检测优先级：bond0 → eth0 → 默认网卡
ansible master -m shell -a "ip addr show bond0 || ip addr show eth0 || ip addr"
```

### 检查配置
```bash
ansible all -m shell -a "cat /root/dist/conf/curvine-cluster.toml | grep data_dir"
```

## 🗑️ 卸载

```bash
ansible-playbook uninstall.yml
```

## 📚 完整文档

详细信息请查看：
- `README.md` - 完整功能说明
- `USAGE.md` - 详细使用指南

## 🆘 需要帮助？

1. 查看日志文件
2. 检查配置文件
3. 参考完整文档
4. 访问官方文档: https://curvineio.github.io

