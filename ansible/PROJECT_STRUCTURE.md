# 项目结构说明

```
curvine-ansible-deployment/
│
├── ansible.cfg                    # Ansible主配置文件
├── hosts.ini                      # 主机清单（需手动填写IP）
│
├── group_vars/                    # 组变量目录
│   └── all.yml                    # 全局变量配置（数据目录、安装路径等）
│
├── setup_ssh.yml                  # SSH免密登录配置playbook
├── setup_ssh_batch.sh             # SSH免密登录配置辅助脚本
├── passwords.example.yml          # 密码配置示例文件
│
├── deploy_curvine.yml             # 主部署playbook
├── deploy_all.sh                  # 一键部署脚本（推荐使用）
│
├── start_services.yml             # 启动服务playbook
├── stop_services.yml              # 停止服务playbook
├── restart_services.yml           # 重启服务playbook
├── status_services.yml            # 查看服务状态playbook
│
├── update_config.yml              # 更新配置文件playbook
├── uninstall.yml                  # 卸载Curvine集群playbook
│
├── README.md                      # 项目说明文档
├── QUICKSTART.md                  # 快速开始指南
├── USAGE.md                       # 详细使用指南
├── PROJECT_STRUCTURE.md           # 本文件
│
└── .gitignore                     # Git忽略文件配置
```

## 文件说明

### 核心配置文件

#### `ansible.cfg`
Ansible主配置文件，包含：
- inventory路径
- SSH连接配置
- 性能优化选项
- 权限提升配置

#### `hosts.ini`
主机清单文件，定义：
- master组：Master节点IP列表
- worker组：Worker节点IP列表
- 全局变量：SSH用户、端口等

#### `group_vars/all.yml`
全局变量配置，包含：
- 安装目录路径
- 安装包路径
- 数据目录配置
- 服务列表

### 部署相关

#### `setup_ssh.yml`
配置SSH免密登录的playbook，支持：
- 自动生成SSH密钥对
- 分发公钥到所有节点
- 支持统一密码或不同密码

#### `setup_ssh_batch.sh`
SSH配置辅助脚本，提供交互式菜单：
- 所有节点使用相同密码
- Master和Worker使用不同密码
- 每个节点使用不同密码

#### `deploy_curvine.yml`
主部署playbook，执行任务：
1. 检测bond0网卡IP
2. 配置环境变量（/etc/profile）
3. 拷贝和解压安装包
4. 配置data_dir
5. 创建systemd服务
6. 启用服务自启动

#### `deploy_all.sh`
一键部署脚本，自动化流程：
- 检查必要文件
- 测试SSH连接
- 执行部署
- 启动服务（可选）

### 服务管理

#### `start_services.yml`
启动服务playbook：
- Master节点：master → worker → fuse
- Worker节点：worker → fuse
- 包含服务就绪等待

#### `stop_services.yml`
停止服务playbook：
- 按正确顺序停止服务
- Worker节点优先停止

#### `restart_services.yml`
重启服务playbook：
- 优雅重启所有服务
- 包含稳定性等待

#### `status_services.yml`
查看服务状态playbook：
- 显示所有服务的systemctl状态
- 分组显示master和worker节点

### 配置管理

#### `update_config.yml`
更新配置文件playbook：
- 更新data_dir配置
- 自动备份原配置
- 支持命令行参数

### 卸载工具

#### `uninstall.yml`
卸载Curvine集群playbook：
- 停止所有服务
- 删除systemd服务文件
- 删除安装目录
- 清理环境变量
- 保留数据目录（需手动清理）

### 文档

#### `README.md`
项目主文档，包含：
- 功能特性
- 系统要求
- 构建和安装说明
- 配置说明
- 常见问题

#### `QUICKSTART.md`
快速开始指南：
- 5分钟快速部署
- 常用命令速查
- 快速故障排查

#### `USAGE.md`
详细使用指南：
- 完整部署流程
- 日常运维操作
- 故障排查
- 最佳实践

### 其他文件

#### `passwords.example.yml`
密码配置示例文件：
- 展示如何配置多密码
- 不同主机组使用不同密码的方法

#### `.gitignore`
Git忽略文件配置：
- 排除敏感信息（密码文件）
- 排除临时文件
- 排除Ansible缓存

## 使用流程

### 初次部署

```
1. 编辑 hosts.ini
   ↓
2. 运行 deploy_all.sh 或者：
   - setup_ssh.yml
   - deploy_curvine.yml
   - start_services.yml
   ↓
3. 验证: status_services.yml
```

### 日常运维

```
配置更新
├─ update_config.yml
└─ restart_services.yml

服务管理
├─ start_services.yml
├─ stop_services.yml
├─ restart_services.yml
└─ status_services.yml
```

### 卸载

```
uninstall.yml
```

## 依赖关系

```
deploy_curvine.yml
├─ 依赖: SSH免密登录已配置
├─ 依赖: /root/dist.tar.gz (master)
├─ 依赖: /root/dist1.tar.gz (worker)
└─ 输出: systemd服务已创建

start_services.yml
├─ 依赖: deploy_curvine.yml 已执行
└─ 输出: 服务已启动

update_config.yml
├─ 依赖: deploy_curvine.yml 已执行
└─ 需要: restart_services.yml 使配置生效
```

## 配置层级

```
命令行参数 (-e)
    ↓ (覆盖)
group_vars/all.yml
    ↓ (覆盖)
hosts.ini [all:vars]
    ↓ (覆盖)
playbook vars
    ↓
默认值
```

## 备份和恢复

### 自动备份

- 配置文件修改时自动备份
- 备份文件命名格式：
  - `curvine-cluster.toml.backup`
  - `curvine-cluster.toml.<timestamp>.backup`

### 手动备份

```bash
# 备份所有配置
ansible all -m fetch -a "src=/root/dist/conf/curvine-cluster.toml dest=./backup/ flat=no"
```

## 日志位置

### Ansible日志

- 执行输出：控制台
- 事实缓存：`/tmp/ansible_fact_cache/`

### Curvine服务日志

- systemd journal：`journalctl -u curvine-<service>`
- 应用日志：`/root/dist/logs/` (如果存在)

## 安全注意事项

1. **密码文件**
   - `passwords.yml` 已加入 `.gitignore`
   - 不要提交到版本控制系统

2. **SSH密钥**
   - 妥善保管生成的SSH私钥
   - 建议使用密钥密码保护

3. **权限管理**
   - 所有操作使用root权限
   - 建议在生产环境使用sudo配置

4. **防火墙**
   - 确保必要端口已开放
   - Master节点：9000 (Web), Raft端口
   - Worker节点：数据传输端口

## 扩展性

### 添加新节点

1. 在 `hosts.ini` 添加节点IP
2. 运行 `setup_ssh.yml --limit <new-host>`
3. 运行 `deploy_curvine.yml --limit <new-host>`
4. 运行 `start_services.yml --limit <new-host>`

### 自定义变量

在 `group_vars/` 目录下添加：
- `master.yml` - Master组特定变量
- `worker.yml` - Worker组特定变量

### 自定义任务

在playbook中添加：
- `pre_tasks` - 部署前任务
- `post_tasks` - 部署后任务
- `handlers` - 事件处理器

## 维护建议

1. **定期检查**
   - 每周运行 `status_services.yml`
   - 监控磁盘空间和日志大小

2. **配置备份**
   - 每次修改配置后备份
   - 使用版本控制管理配置

3. **文档更新**
   - 记录自定义修改
   - 更新部署文档

4. **测试环境**
   - 先在测试环境验证
   - 再应用到生产环境

## 贡献指南

如需修改或扩展此项目：

1. 保持代码结构清晰
2. 添加适当的注释
3. 更新相关文档
4. 测试所有变更

## 版本信息

- 项目版本：1.0
- Ansible要求：>= 2.9
- Curvine兼容：v0.1.0+

## 参考资源

- Ansible最佳实践：https://docs.ansible.com/ansible/latest/user_guide/playbooks_best_practices.html
- Systemd服务管理：https://www.freedesktop.org/software/systemd/man/systemd.service.html
- Curvine文档：https://curvineio.github.io

