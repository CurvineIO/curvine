# Curvine Daily回归测试脚本

本目录包含了 Curvine 项目的Daily回归测试脚本，用于启动测试集群、运行测试并生成HTML报告。

## 脚本说明

### 1. 主测试脚本

#### `daily_regression_test.sh` - 每日回归测试
专门用于每日自动化测试，生成详细的HTML报告。

```bash
# 运行每日回归测试
./scripts/daily_regression_test.sh
```

### 2. 组件脚本


#### `check_cluster_status.sh` - 集群状态检查
检查 Curvine 测试集群是否正常运行，根据 "Cluster is ready, active Worker count: 3" 日志判断状态。

```bash
./scripts/check_cluster_status.sh <log_file> [max_wait] [pid]
```

### 3. 工具脚本

#### `colors.sh` - 颜色定义
所有脚本共享的颜色定义文件。

```bash
source scripts/colors.sh
```

#### `pre-test.sh` - 预测试脚本
用于代码更新和构建的脚本。

```bash
./scripts/pre-test.sh
```

## 使用示例

### 快速开始
```bash
# 进入项目目录
cd /path/to/curvine-hot-fix

# 运行每日回归测试
./scripts/daily_regression_test.sh
```

### 查看测试结果
```bash
# 查看生成的报告
ls daily_test_results/
open daily_test_results/YYYYMMDD_HHMMSS/daily_test_report.html
```

## 测试分类

脚本会自动运行以下测试分类：

- **文件系统测试** (`fs_test`)
- **块存储测试** (`block_test`) 
- **集群测试** (`cluster_test`)
- **挂载测试** (`mount_test`)
- **复制测试** (`replication_test`)
- **TTL测试** (`ttl_test`)
- **超时测试** (`timeout_test`)
- **负载测试** (`load_client_test`)
- **统一测试** (`unified_test`)

## 输出文件

测试完成后会在 `daily_test_results/YYYYMMDD_HHMMSS/` 目录下生成：

- `daily_test_report.html` - 美观的HTML测试报告
- `test_summary.json` - 测试结果JSON数据
- `daily_test.log` - 主测试日志
- `cluster.log` - 集群启动日志
- `*.log` - 各测试分类的详细日志

## 集群状态检查

### 集群就绪判断
脚本会根据以下日志信息判断集群是否就绪：

1. **完全就绪**: `"Cluster is ready, active Worker count: 3"`
2. **部分就绪**: `"Cluster is ready"` (但Worker数量不足3个)

### 检查逻辑
- 等待最多120秒让集群完全启动
- 如果完全就绪，立即继续
- 如果部分就绪，额外等待30秒
- 如果超时，报告错误并退出

## 环境要求

- Rust 和 Cargo
- jq (用于JSON处理)
- 项目配置文件 `etc/curvine-cluster.toml`
