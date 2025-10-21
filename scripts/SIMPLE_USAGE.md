# Curvine Daily回归测试 - 简化使用指南

## 🎯 核心功能

你只需要一个脚本：**`daily_regression_test.sh`**

## 🚀 使用方法

### 1. 直接运行每日回归测试
```bash
# 进入项目目录
cd /path/to/curvine-hot-fix

# 运行每日回归测试
./scripts/daily_regression_test.sh
```

### 2. 查看测试结果
```bash
# 测试完成后，查看HTML报告
ls daily_test_results/
open daily_test_results/YYYYMMDD_HHMMSS/daily_test_report.html
```

## 📁 输出文件

测试完成后会生成：
```
daily_test_results/
└── 20241201_143022/           # 时间戳目录
    ├── daily_test_report.html # HTML测试报告
    ├── test_summary.json      # 测试结果摘要
    ├── cluster.log           # 集群启动日志
    └── *.log                 # 各测试分类日志
```

## 🔧 脚本功能

`daily_regression_test.sh` 会自动执行：

1. **代码更新** - 执行 `pre-test.sh` 脚本
2. **启动测试集群** - 等待集群就绪
3. **运行测试套件** - 10个测试分类
4. **生成HTML报告** - 美观的测试报告
5. **清理资源** - 自动清理测试环境

## ❌ 不需要的脚本

以下脚本你可以忽略：
- `run_tests.sh` - 仅运行测试（功能重复）
- `run_full_test.sh` - 完整流程（功能重复）
- `test_server.py` - Web服务器（如果你不需要Web界面）
- `test_client.py` - 命令行客户端（如果你不需要Web界面）

## 🎯 总结

**你只需要关注一个脚本：`daily_regression_test.sh`**

这个脚本包含了完整的每日回归测试功能，满足你的所有需求！
