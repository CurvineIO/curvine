# 更新日志

## [1.12] - 2025-12-18

### ✨ 新增Worker节点主机名环境变量功能

#### 功能概述
为每个 Worker 节点的 `/etc/profile` 添加环境变量 `CURVINE_WORKER_HOSTNAME`，该变量的值等于该 Worker 在 `hosts.ini` 中定义的 IP 地址。

#### 实现细节

**自动配置时机**：
1. **部署时自动设置** - `deploy_curvine.yml`
   - 在配置 Worker 节点时自动添加环境变量
   - 确保每个 Worker 的 IP 地址与 `hosts.ini` 中的定义一致
   
2. **启动服务前自动检查** - `start_services.yml`
   - 在启动服务前检查环境变量是否正确
   - 如果不正确，自动修复为正确的值
   - 确保服务启动时环境变量已正确设置

**配置内容**：
- `/etc/profile` 中添加：`export CURVINE_WORKER_HOSTNAME=<worker_ip>`
- systemd 服务文件中添加：`Environment="CURVINE_WORKER_HOSTNAME=<worker_ip>"`
- 每个 Worker 的值自动从 `hosts.ini` 的 `inventory_hostname` 获取

**示例**：
```bash
# Worker节点 10.128.131.15
export CURVINE_WORKER_HOSTNAME=10.128.131.15

# Worker节点 10.128.131.16
export CURVINE_WORKER_HOSTNAME=10.128.131.16
```

#### 新增文件

1. **setup_worker_hostname.yml** - 独立的环境变量设置脚本
   - 可单独运行，用于设置或更新 Worker 节点的环境变量
   - 自动检查并修复不正确的设置
   - 更新 `/etc/profile` 和 systemd 服务文件
   - 显示详细的设置状态

2. **verify_worker_hostname.yml** - 环境变量验证脚本
   - 验证所有 Worker 节点的环境变量是否正确
   - 检查 `/etc/profile` 和 systemd 服务文件
   - 生成详细的验证报告
   - 显示通过/失败的节点统计

3. **WORKER_HOSTNAME_FEATURE.md** - 功能说明文档
   - 完整的功能说明和使用指南
   - 验证方法和故障排查
   - 适用场景和注意事项

#### 使用方法

**自动使用**（推荐）：
```bash
# 部署时自动设置
ansible-playbook deploy_curvine.yml

# 启动服务时自动检查和修复
ansible-playbook start_services.yml
```

**独立使用**：
```bash
# 设置或更新环境变量
ansible-playbook setup_worker_hostname.yml

# 验证环境变量设置
ansible-playbook verify_worker_hostname.yml

# 验证特定节点
ansible worker -m shell -a "grep CURVINE_WORKER_HOSTNAME /etc/profile"
```

#### 修改的文件

1. **deploy_curvine.yml**
   - Worker 节点部署时添加 `CURVINE_WORKER_HOSTNAME` 环境变量
   - 在 `/etc/profile` 中添加 export 语句
   - 在 systemd 服务文件中添加 Environment 变量
   - 显示设置状态

2. **start_services.yml**
   - 启动服务前检查 Worker 节点的环境变量
   - 自动修复不正确的设置
   - 显示检查和修复状态
   - 确保环境变量正确后才启动服务

3. **README.md**
   - 添加 Worker 主机名环境变量功能说明
   - 更新功能特性列表
   - 添加环境变量管理命令
   - 更新服务说明部分

#### 应用场景

- ✅ 每个 Worker 需要知道自己的 IP 地址
- ✅ 服务启动时需要使用 Worker 的 IP 地址
- ✅ 分布式系统中节点身份识别
- ✅ 配置文件中需要引用 Worker 的 IP
- ✅ 日志记录和监控中标识 Worker 节点

#### 验证示例

```bash
# 验证所有 Worker
$ ansible-playbook verify_worker_hostname.yml

========================================
Worker节点: 10.128.131.15
----------------------------------------
预期IP (hosts.ini): 10.128.131.15

/etc/profile设置:
  值: 10.128.131.15
  状态: ✓ 正确

systemd服务设置:
  值: 10.128.131.15
  状态: ✓ 正确

总体状态: ✓ 通过验证
========================================

验证摘要
========================================
总Worker节点数: 19
通过验证: 19
验证失败: 0

✓ 所有Worker节点的CURVINE_WORKER_HOSTNAME环境变量设置正确！
========================================
```

## [1.11] - 2025-12-17

### ✨ 新增配置与实际挂载点同步功能

#### 问题场景
- 之前已经格式化并挂载了多块磁盘（/datads1, /datads2, ...）
- 但配置文件可能未正确更新或被覆盖
- 导致配置文件中的data_dir与实际挂载不一致
- 服务启动后无法使用已挂载的磁盘

#### 解决方案
启动服务前，自动检查并同步配置文件与实际挂载点：

**检查步骤**：
1. 从df -h获取所有/datads*挂载点
2. 检查配置文件中的data_dir是否包含这些路径
3. 如果缺少，添加到data_dir中
4. 对于Master节点，同时检查meta_dir和journal_dir：
   - 如果已经是/datads1/*，保持不变
   - 如果还是/data/*，更新为/datads1/*

**执行时机**：
- Worker目录结构修复后
- 启动服务前

**示例**：
```
节点 10.128.131.15 (worker):
  实际挂载的datads: /datads1,/datads2,/datads3
  配置文件中现有的data_dir: ["[SSD]/data/data"]
  添加缺失的路径: /datads1/data
  添加缺失的路径: /datads2/data
  添加缺失的路径: /datads3/data
  新的data_dir: data_dir = ["[SSD]/data/data", "/datads1/data", "/datads2/data", "/datads3/data"]
  ✓ 配置已同步
```

#### 新增文件
- `sync_datads_config.py` - 配置同步脚本

#### 适用场景
- 历史遗留问题修复
- 配置文件被意外覆盖
- 手动挂载了磁盘但未更新配置
- 确保配置文件与实际系统状态一致

## [1.10] - 2025-12-17

### 🐛 Bug修复：磁盘检测遗漏LVM和RAID设备

**问题**：
- nvme3n1有LVM子设备（data-data）
- nvme4n1等有RAID子设备（md1）
- 子设备名称不是以父磁盘名开头（如md0、md1、data-data）
- `grep "^${disk}"` 无法匹配到这些子设备
- 导致被误判为未格式化磁盘

**修复**：
- 使用 `lsblk -no NAME "/dev/${disk}"` 列出磁盘的完整设备树
- 包括所有子设备，不管命名规则
- 使用 `lsblk -no MOUNTPOINT "/dev/${disk}"` 检查所有挂载点
- 支持所有类型：part、lvm、raid、crypt等

**修复效果**：
```
# nvme4n1的lsblk输出
nvme4n1      3.5T disk
`-md1         21T raid5
  `-md1p1     21T part  /cce

# 检测逻辑
lsblk -no NAME "/dev/nvme4n1"  # 输出：nvme4n1, md1, md1p1
has_children = 3 - 1 = 2  # 有子设备
lsblk -no MOUNTPOINT "/dev/nvme4n1"  # 找到挂载点 /cce
结论：不是未格式化磁盘 ✅
```

**测试场景**：
- ✅ 普通分区：sda1, vda1, nvme0n1p1
- ✅ LVM卷：data-data, vg-lv
- ✅ RAID阵列：md0, md1, md127
- ✅ 加密卷：luks-xxx
- ✅ 复杂嵌套：RAID → LVM → 分区

### ✨ 新增环境变量自动检查和修复

#### 问题背景
- Worker节点可能被改为Master节点
- 导致/etc/profile中CURVINE_MASTER_HOSTNAME设置错误
- Master节点设置为localhost会导致服务启动失败
- Worker节点设置为IP而非localhost会导致连接问题

#### 解决方案
在启动/重启服务前，自动检查并修复所有节点的环境变量：

**Master节点检查**：
- 读取当前CURVINE_MASTER_HOSTNAME值
- 获取节点的eth0或bond0 IP
- 对比是否一致
- 不一致则自动修复为正确的IP

**Worker节点检查**：
- 读取当前CURVINE_MASTER_HOSTNAME值
- 检查是否为"localhost"
- 不是则自动修复为"localhost"

#### 执行时机
- `./deploy_all.sh` - 启动服务前自动检查
- `ansible-playbook start_services.yml` - 启动前自动检查
- `ansible-playbook restart_services.yml` - 重启前自动检查

#### 显示信息
```
节点 10.200.3.17 (Master):
  当前CURVINE_MASTER_HOSTNAME: localhost
  应该设置为: 10.200.3.17
  ⚠️ 不正确，正在修复...
  ✓ 已修复为: 10.200.3.17
```

### ✨ 新增master_addrs和journal_addrs自动更新

#### 问题
- 配置文件中的master_addrs和journal_addrs可能与hosts.ini不一致
- 导致Master节点服务启动失败或无法互相发现
- 手动修改容易出错且繁琐

#### 解决方案
在启动服务前，自动检查并更新所有节点的配置文件：

**更新内容**：
- **master_addrs**: 从hosts.ini的master组自动生成
  ```toml
  master_addrs = [
      { hostname = "10.200.3.3", port = 8995 },
      { hostname = "10.200.3.14", port = 8995 },
      { hostname = "10.200.3.8", port = 8995 }
  ]
  ```

- **journal_addrs**: 从hosts.ini的master组自动生成（带ID）
  ```toml
  journal_addrs = [
      {id = 1, hostname = "10.200.3.3", port = 8996},
      {id = 2, hostname = "10.200.3.14", port = 8996},
      {id = 3, hostname = "10.200.3.8", port = 8996}
  ]
  ```

#### 执行时机
- 部署完成后
- 格式化磁盘配置确认后
- **启动服务前**

#### 适用范围
- 所有Master节点
- 所有Worker节点
- 首次部署和增量部署都会检查更新

#### 新增文件
- `update_master_addrs.yml` - 独立的更新playbook（可单独使用）

#### 使用示例
```bash
# 集成在deploy_all.sh中（自动）
./deploy_all.sh

# 或单独使用
ansible-playbook update_master_addrs.yml
```

## [1.9.2] - 2025-12-17

### 🐛 严重Bug修复
- **修复deploy_curvine.yml覆盖已修改配置的问题**
  - 问题：每次部署时，deploy_curvine.yml都会用默认data_dirs覆盖配置文件
  - 结果：格式化磁盘后的配置修改被覆盖，丢失/datads1、/datads2等路径
  - 原因：`when: data_dirs is defined` 总是为true（group_vars中有默认值）
  - 解决：
    - Master节点：只在 `not install_dir_check.stat.exists` 时更新（首次部署）
    - Worker节点：只在 `not worker_install_dir_check.stat.exists` 时更新（首次部署）
    - 如果安装目录已存在，跳过data_dir更新，保留已修改的配置

- **添加配置更新的调试输出**
  - 显示Python脚本执行前的data_dir值
  - 显示Python脚本执行后的data_dir值
  - 便于追踪配置是否被正确修改

### 修复效果
**之前**（错误）：
```
第一次部署：data_dir = ["/datads1/data", "/datads2/data"]  ← 格式化后设置
第二次部署：data_dir = ["[SSD]/data/data"]                ← 被覆盖回默认值！
```

**现在**（正确）：
```
第一次部署：data_dir = ["/datads1/data", "/datads2/data"]  ← 格式化后设置
第二次部署：data_dir = ["/datads1/data", "/datads2/data", "/datads3/data"]  ← 追加新路径 ✅
```

## [1.9.1] - 2025-12-17

### ⚡ 性能优化：跳过已有安装目录

- **优化**：检查/root/dist目录是否已存在
  - 如果存在：跳过拷贝和解压，直接使用现有安装
  - 如果不存在：执行完整的拷贝和解压流程
  - 加快重复部署速度（增量部署、配置更新等场景）

- **适用场景**：
  - 只更新配置文件
  - 增加新磁盘并更新配置
  - 重新创建systemd服务
  - 修复配置问题

- **节省时间**：
  - 跳过大文件拷贝（dist.tar.gz可能几百MB）
  - 跳过解压操作
  - 大幅加快重复部署速度

### 使用示例
```bash
# 第一次部署：完整拷贝和解压
./deploy_all.sh
# Master节点拷贝dist.tar.gz并解压

# 第二次部署（增加磁盘）：跳过拷贝和解压
./deploy_all.sh
# 检测到/root/dist已存在，跳过拷贝和解压
# 只更新配置文件和挂载新磁盘
```

## [1.9] - 2025-12-17

### 🔄 重构配置文件更新（使用Python）
- **问题**：Shell脚本更新toml文件不可靠
  - 引号嵌套问题
  - 特殊字符处理困难
  - 单行和多行格式难以统一处理
  - 无法正确读取`["[SSD]/data/data"]`格式

- **解决**：使用Python脚本更新配置
  - 新增 `update_toml_config.py`
  - 使用正则表达式可靠提取和更新
  - 支持任意格式的路径（`/xxx`, `[SSD]/xxx`, `[HDD]/xxx`等）
  - 支持单行和多行数组格式
  - 自动去重，避免重复路径
  - 完整的调试输出

### ✨ 配置更新逻辑（追加模式）
- **问题**：之前配置更新是覆盖模式，会丢失已有配置
- **现在**：改为追加模式，保留已有配置并追加新路径

#### Master节点配置更新逻辑
- **meta_dir**: 只在原来是 `/data/*` 时更新为第一个新挂载点
  - 如果已经是 `/datads1/meta`，不会修改
- **journal_dir**: 只在原来是 `/data/*` 时更新为第一个新挂载点
  - 如果已经是 `/datads1/journal`，不会修改
- **data_dir**: 读取现有路径 + 追加新路径
  - 原来：`["/datads1/data"]`
  - 新增：`/datads2`
  - 结果：`["/datads1/data", "/datads2/data"]` ✅

#### Worker节点配置更新逻辑
- **data_dir**: 读取现有路径 + 追加新路径
  - 自动去重，避免重复路径
  - 保留所有历史挂载点

#### 更新示例
```toml
# 第一次部署（vdb -> /datads1）
data_dir = ["/datads1/data"]

# 第二次部署（新增vdc -> /datads2）
data_dir = ["/datads1/data", "/datads2/data"]  # 追加，不是覆盖

# 第三次部署（新增vdd -> /datads3）
data_dir = ["/datads1/data", "/datads2/data", "/datads3/data"]  # 继续追加
```

## [1.8.2] - 2025-12-17

### ✨ 改进挂载点命名逻辑
- **支持增量部署场景**
  - 问题：多次部署时，新磁盘可能覆盖已存在的挂载点
  - 场景：第一次vdb→/datads1，后来加了vdc，应该→/datads2
  - 解决：
    - 格式化前检查df -h中已存在的/datads*挂载点
    - 找到最大序号（如已有/datads1、/datads2，最大为2）
    - 新磁盘从(最大序号+1)开始命名
    - 避免挂载点冲突

- **智能序号分配**
  ```bash
  # 第一次部署
  vdb -> /datads1
  
  # 第二次部署（增加了vdc）
  检测到已有/datads1
  vdc -> /datads2  # 自动跳过1，使用2
  
  # 第三次部署（增加了vdd、vde）
  检测到已有/datads1、/datads2
  vdd -> /datads3  # 自动从3开始
  vde -> /datads4
  ```

### 🐛 Bug修复
- **修复配置文件更新失败**
  - 问题：ansible参数解析错误 "Unable to parse argument string"
  - 原因：data_dir数组包含特殊字符，无法直接在-m shell -a中使用
  - 解决：
    - 使用脚本文件方式
    - 在控制节点生成更新脚本
    - 用sed替换变量占位符
    - 拷贝到远程节点执行
    - 使用sed多行匹配替换整个data_dir数组

## [1.8.1] - 2025-12-17

### 🐛 严重Bug修复
- **修复磁盘检测错误导致的误操作风险**
  - 问题：shell语法错误 "[: too many arguments"
  - 危险：错误信息被当作磁盘名（/tmp、line、too、many等）
  - 后果：可能格式化系统关键目录（已阻止）
  - 解决：
    - 完全重写检测逻辑，使用简单可靠的命令
    - 严格的磁盘名正则过滤：`^(sd[a-z]+|vd[a-z]+|nvme[0-9]+n[0-9]+|xvd[a-z]+)$`
    - 只输出有效的块设备名
    - stderr重定向避免错误信息混入

### ✨ 新增配置文件自动更新功能（已完成）
- 格式化磁盘后，记录每个节点的所有挂载点
- 部署完成后、启动服务前自动更新配置文件
- 显示修改后的配置给用户确认
- 用户输入Y后再启动服务

#### 配置更新逻辑
**Master节点**：
- meta_dir：使用第一个挂载点
- journal_dir：使用第一个挂载点
- data_dir：使用所有挂载点

**Worker节点**：
- data_dir：使用所有挂载点

**示例**（10.200.3.12有vdb格式化到/datads1）：
```toml
# Worker节点配置更新：
data_dir = ["/datads1/data"]  # 原来是 ["/data/data"]
```

## [1.8] - 2025-12-17

### ✨ 新增配置文件自动更新功能

格式化磁盘后，部署完成时自动更新配置文件以使用新磁盘：

#### Master节点配置更新
- **meta_dir**: 将 `/data/meta` 替换为 `{第一个挂载点}/meta`
  - 例如：`/datads1/meta`
- **journal_dir**: 将 `/data/journal` 替换为 `{第一个挂载点}/journal`
  - 例如：`/datads1/journal`
- **data_dir**: 替换为所有新挂载点的数组
  - 例如：`["/datads1/data", "/datads2/data"]`

#### Worker节点配置更新
- **data_dir**: 替换为所有新挂载点的数组
  - 例如：`["/datads1/data"]`

#### 用户确认机制
- 配置更新后显示每个节点的修改结果
- Master节点显示：meta_dir、journal_dir、data_dir
- Worker节点显示：data_dir
- 用户输入Y确认后才启动服务

#### 自动备份
- 修改前自动备份配置文件为 `.bak`
- 确保可以回滚

### 🐛 Bug修复
- **修复磁盘检测的严重错误**
  - 问题：shell语法错误"[: too many arguments"
  - 问题：错误信息被当作磁盘名（/tmp、line、26、too等）
  - 危险：可能导致系统关键目录被误操作
  - 解决：
    - 简化检测脚本，避免复杂的引号嵌套
    - 添加严格的磁盘名正则过滤：`^(sd[a-z]+|vd[a-z]+|nvme[0-9]+n[0-9]+)$`
    - 重定向stderr避免错误信息混入结果
    - 只输出有效的磁盘设备名

### 使用示例
```bash
./deploy_all.sh

# 1. 检测到未格式化磁盘
节点 10.200.3.12:
  - /dev/vdb -> 将挂载到 /datads1

# 2. 用户确认Y，格式化和挂载

# 3. 部署完成后，自动更新配置
节点: 10.200.3.12 (Worker)
================
data_dir = ["/datads1/data"]

# 4. 用户确认Y，启动服务
```

## [1.7] - 2025-12-17

### ✨ 新增磁盘格式化和挂载功能

#### 智能磁盘检测
在节点信息确认后、部署前，自动检测所有节点的未格式化磁盘：

**检测条件**（需同时满足）：
1. ✅ lsblk中TYPE为disk
2. ✅ lsblk中MOUNTPOINT为空
3. ✅ 没有子分区（part）
4. ✅ df -h中看不到该磁盘

**显示示例**：
```
发现未格式化的磁盘：

节点 10.200.3.14:
  - /dev/vdc -> 将挂载到 /datads1
  - /dev/vdd -> 将挂载到 /datads2

节点 10.200.3.15:
  - /dev/vdc -> 将挂载到 /datads1
```

#### 用户确认和自动格式化
- 显示所有未格式化磁盘后，等待用户输入Y确认
- 确认后自动执行：
  1. 使用 `mkfs.ext4 -F /dev/{磁盘}` 格式化
  2. 创建挂载点目录 `/datads1`, `/datads2`, `/datads3`...
  3. 挂载磁盘到对应目录
  4. 添加到 `/etc/fstab` 实现开机自动挂载

#### 格式化后验证
- 显示每个节点格式化后的lsblk和df -h信息
- 等待用户输入Y确认后进入部署阶段

### 文件说明
- `detect_unformatted_disks.yml` - 检测未格式化磁盘的playbook（备用）
- `format_and_mount_disks.yml` - 格式化和挂载磁盘的playbook（备用）
- `unformatted_disks_info.txt` - 未格式化磁盘列表（自动生成）

### 使用流程
```bash
./deploy_all.sh

# 1. 检测节点连接
# 2. SSH免密配置
# 3. 收集节点信息 -> 用户确认Y
# 4. 检测未格式化磁盘 -> 显示 -> 用户确认Y -> 格式化和挂载 -> 显示结果 -> 用户确认Y
# 5. 开始部署
```

## [1.6.3] - 2025-12-17

### 🐛 Bug修复
- **修复网络信息为空的问题**
  - 问题：节点信息中网络信息显示为空
  - 原因：ansible输出被过度过滤，包含网络信息的行也被过滤掉
  - 解决：
    - 使用变量存储ansible命令输出
    - 只过滤掉ansible元数据行（主机名、SUCCESS等）
    - 保留包含冒号(:)的网络信息行
    - 改进grep过滤条件

- **添加df -h表头行**
  - 问题：磁盘使用情况没有表头，不易阅读
  - 解决：在df命令中先获取表头（`df -h | head -1`），再获取数据行
  - 显示格式更清晰

## [1.6.2] - 2025-12-17

### 🐛 Bug修复
- **修复节点信息文件保存路径问题**
  - 问题：node_info_summary.txt文件保存位置不确定，脚本找不到
  - 解决：改用shell脚本直接收集和保存节点信息
    - 不再依赖ansible playbook的模板系统
    - 直接在deploy_all.sh中使用ansible命令收集信息
    - 使用shell重定向直接写入文件
    - 确保在$SCRIPT_DIR目录保存文件

- **改进信息收集逻辑**
  - 逐个节点收集信息，实时写入文件
  - 每个节点收集：网络、lsblk、df -h
  - 使用ansible shell模块执行远程命令
  - 过滤掉ansible输出中的节点名，只保留命令结果

## [1.6.1] - 2025-12-17

### 🐛 Bug修复  
- **修复节点信息文件保存路径问题（第一次尝试）**
  - 使用 `local_action` 确保在ansible执行目录保存
  - deploy_all.sh使用 `$SCRIPT_DIR` 明确文件路径
  - 增加文件生成等待时间（2秒）
  - 添加文件存在性验证

## [1.6] - 2025-12-17

### ✨ 新增功能

#### 1. 节点信息收集和确认
- **collect_node_info.yml** - 收集所有节点的详细信息
  - 网络信息：自动识别eth、en、bond开头的网卡IP地址
  - 磁盘信息：使用lsblk显示磁盘布局
  - 磁盘使用：df -h显示/dev开头的磁盘使用情况
  - 分组显示：Master和Worker节点分别显示
  - 保存到文件：node_info_summary.txt

- **部署前确认**
  - 在SSH配置成功后，部署前显示所有节点信息
  - 用户需要输入Y确认信息无误后才能继续部署
  - 可以在确认前仔细检查网络和磁盘配置

#### 2. 完整的日志记录系统
- **deploy.log** - 所有部署日志保存到当前目录
  - 追加模式：不会覆盖之前的日志
  - 时间戳：每条日志带有时间戳
  - 完整记录：包含所有命令执行和输出
  - 用户输入：记录所有用户确认操作
  - ansible输出：所有ansible-playbook的输出都记录

- **日志函数**
  - `log()` - 记录普通日志
  - `log_cmd()` - 记录并执行命令，输出同时显示和保存

### 使用示例
```bash
# 运行部署脚本
./deploy_all.sh

# 会看到节点信息收集和确认步骤
# 输入Y确认后继续部署
# 所有日志保存到deploy.log

# 查看日志
cat deploy.log
tail -f deploy.log  # 实时查看
```

### 文件说明
- `collect_node_info.yml` - 节点信息收集playbook
- `node_info_summary.txt` - 节点信息汇总文件（自动生成）
- `deploy.log` - 部署日志文件（自动生成，追加模式）

## [1.5.1] - 2025-12-17

### 🐛 Bug修复
- **修复Worker节点目录错位问题**
  - 问题：dist1.tar.gz解压后文件在 `/root/dist1/` 目录，但systemd配置指向 `/root/dist/`
  - 原因：压缩包包含顶层目录名 `dist1/`，导致解压位置错误
  - 解决：
    - 解压后自动检测是否在 `dist1/` 目录
    - 如果是，自动移动到 `dist/` 目录
    - 添加验证确保 `dist/` 目录存在
  - 修改文件：`deploy_curvine.yml`, `fix_worker.yml`

## [1.5] - 2025-12-17

### 🔧 改进Worker节点部署和诊断
- **新增诊断工具**
  - `diagnose_worker.yml` - 全面诊断Worker节点问题
    - 检查安装目录、bin、lib目录是否存在
    - 检查启动脚本和二进制文件权限
    - 检查配置文件
    - 尝试手动执行启动脚本
    - 显示systemd日志
  - `fix_worker.yml` - 自动修复Worker节点问题
    - 重新拷贝和解压安装包
    - 备份旧安装目录
    - 设置正确的文件权限
    - 重新启动服务
    - 显示详细的诊断信息

- **改进deploy_curvine.yml Worker部署**
  - 添加bin目录存在性检查
  - 检查关键文件（curvine-worker.sh, curvine-fuse.sh, curvine-server）
  - 显示每个文件的状态
  - lib目录也设置可执行权限

- **改进start_services.yml Worker启动**
  - 启动前检查启动脚本是否存在
  - 服务启动失败时显示详细日志
  - 更好的错误提示和诊断建议
  - 使用ignore_errors继续执行

### 使用方法
```bash
# 诊断Worker节点问题
ansible-playbook diagnose_worker.yml --limit 10.200.3.15

# 修复Worker节点
ansible-playbook fix_worker.yml --limit 10.200.3.15

# 重新启动服务
ansible-playbook start_services.yml --limit 10.200.3.15
```

## [1.4.3] - 2025-12-17

### 🐛 Bug修复
- **修复set -e导致脚本意外退出**
  - 问题：ansible-playbook有unreachable主机时返回非零值，触发set -e导致脚本退出
  - 解决：所有ansible-playbook命令添加`|| true`
  - 影响命令：setup_ssh.yml, deploy_curvine.yml, start_services.yml, status_services.yml
  - 现在即使部分节点失败，脚本也会继续执行

## [1.4.2] - 2025-12-17

### 🐛 Bug修复
- **修复节点统计错误**
  - 过滤掉"hosts (X):"这样的ansible输出行
  - 只统计真实的IP地址
  - Master和Worker节点数量统计现在正确

- **修复SSH配置失败逻辑错误**
  - 之前：有任何节点unreachable就退出
  - 现在：只要有节点配置成功就继续部署
  - unreachable节点自动跳过，不影响其他节点
  - 只有所有节点都失败才真正退出

## [1.4.1] - 2025-12-17

### 🐛 Bug修复
- **修复脚本直接退出的问题**
  - 简化节点检测逻辑，逐个检测每个节点
  - 移除复杂的grep匹配，改为直接ansible ping
  - 修复在hosts.ini配置正确时脚本直接退出的问题
  - 增加详细的错误提示信息

## [1.4] - 2025-12-17

### ⚡ 性能优化
- **大幅缩短连接检测时间**
  - SSH连接超时从30秒降至3秒
  - SSH ConnectTimeout设置为3秒
  - SSH retries从2次降至1次
  - 逐个节点快速检测，不再批量处理
  - Ping 3次不通自动跳过，不再长时间等待

- **智能节点管理**
  - 快速检测所有节点，立即显示哪些可连接
  - 连接失败的节点自动跳过，不影响其他节点部署
  - 使用 `--limit` 参数只部署连接成功的节点
  - 部署完成后汇总显示成功/失败节点列表

### 改进
- **ansible.cfg**
  - timeout: 30 → 3秒
  - ConnectTimeout: 3秒
  - SSH retries: 1次
  - forks: 10 (并发连接)

- **deploy_all.sh**
  - 分别检测master和worker节点
  - 显示每组的成功/失败数量
  - 记录失败节点，最后汇总显示
  - 只部署连接成功的节点
  - 清理临时文件

- **所有playbook**
  - 添加 `any_errors_fatal: false` - 允许部分节点失败
  - 添加 `ignore_unreachable: yes` - 忽略不可达节点
  - 适用于: deploy_curvine.yml, start_services.yml, stop_services.yml, restart_services.yml, status_services.yml

### 修改的文件
- `ansible.cfg` - 优化超时和连接参数
- `deploy_all.sh` - 快速检测和智能跳过
- `deploy_curvine.yml` - 允许部分节点失败
- `start_services.yml` - 允许部分节点失败
- `stop_services.yml` - 允许部分节点失败
- `restart_services.yml` - 允许部分节点失败
- `status_services.yml` - 允许部分节点失败

---

## [1.3] - 2025-12-17

### 🐛 关键修复
- **修复SSH免密登录未真正执行的问题**
  - 之前只检测连接状态，不会真正配置SSH密钥
  - 现在会调用 `ansible-playbook setup_ssh.yml --ask-pass` 真正配置免密登录
  - 使用ansible的authorized_key模块自动分发SSH公钥

### 改进
- **简化 setup_ssh.yml**
  - 移除vars_prompt，避免与--ask-pass冲突
  - 直接使用--ask-pass参数输入密码
  - 自动生成SSH密钥对（如果不存在）
  - 自动分发公钥到所有目标节点
  
- **改进 setup_ssh_batch.sh**
  - 简化交互流程
  - 直接调用ansible-playbook而不是自己处理密码
  - 配置后自动测试连接
  
- **新增 setup_ssh_quick.sh**
  - 快速配置SSH免密登录的简化脚本
  - 适合快速部署场景

### 修改的文件
- `setup_ssh.yml` - 简化逻辑，使用--ask-pass
- `deploy_all.sh` - 改进SSH配置调用
- `setup_ssh_batch.sh` - 简化流程
- `setup_ssh_quick.sh` - 新增

---

## [1.2] - 2025-12-17

### 🐛 Bug修复
- **修复SSH连接测试问题**：改进 `deploy_all.sh` 中的连接测试逻辑
  - 之前使用 `&> /dev/null` 静默执行，可能误判部分节点失败为成功
  - 现在分别测试master和worker节点，显示各组连接状态
  - 显示节点数量，更容易发现遗漏的节点
  
- **修复Worker节点配置文件问题**
  - Worker节点的dist1.tar.gz可能不包含配置文件
  - 添加配置文件存在性检查，不存在则跳过配置更新
  - 避免因配置文件不存在而导致部署失败

### 新增
- 新增 `TROUBLESHOOTING.md` - 详细的故障排查指南
  - SSH免密登录配置说明
  - Worker节点配置文件问题解决方案
  - 完整的部署检查清单
  - 常见问题和解决方法

### 改进
- **deploy_all.sh** 连接测试改进：
  ```bash
  检查Master节点...
    ✓ Master节点 (3 个) 连接正常
  检查Worker节点...
    ✗ Worker节点连接失败
  ```
  
- **deploy_curvine.yml** Worker节点配置改进：
  - 自动检查配置文件是否存在
  - 显示配置文件状态信息
  - 只在配置文件存在时更新配置

### 修改的文件
- `deploy_all.sh` - 改进连接测试逻辑
- `deploy_curvine.yml` - 增加worker节点配置文件检查
- `TROUBLESHOOTING.md` - 新增故障排查文档

---

## [1.1] - 2025-12-17

### 新增
- 增加对eth0网卡的支持作为bond0的备选方案

### 改进
- **网卡检测优先级**：
  1. 首先尝试检测 `bond0` 网卡
  2. 如果bond0不存在，尝试检测 `eth0` 网卡
  3. 如果bond0和eth0都不存在，使用默认网卡
- 部署时显示使用的网卡来源，方便排查问题

### 修改的文件
- `deploy_curvine.yml` - 增加eth0网卡检测逻辑
- `README.md` - 更新网卡检测说明
- `USAGE.md` - 更新故障排查部分
- `QUICKSTART.md` - 增加网卡检查命令
- `SUMMARY.txt` - 更新问题3说明

### 使用说明
部署时，脚本会自动按优先级检测Master节点的网卡：
```yaml
# 1. bond0 (首选)
ansible_facts['bond0']['ipv4']['address']

# 2. eth0 (备选)
ansible_facts['eth0']['ipv4']['address']

# 3. 默认网卡 (最后备选)
ansible_default_ipv4.address
```

部署时会输出检测到的网卡信息：
```
Master节点IP: 192.168.1.10 (来源: eth0)
```

---

## [1.0] - 2025-12-17

### 初始版本
- 完整的Ansible自动化部署工具
- SSH免密登录配置
- 环境变量自动配置
- 安装包分发和解压
- systemd服务化
- 服务管理（启动/停止/重启/状态）
- 配置文件管理
- 完整的文档

