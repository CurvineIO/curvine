#!/bin/bash
# 批量设置SSH免密登录

set -e

echo "========================================"
echo "Curvine SSH免密登录批量配置工具"
echo "========================================"
echo ""

# 检查ansible是否安装
if ! command -v ansible &> /dev/null; then
    echo "错误: 未找到ansible命令，请先安装ansible"
    echo "CentOS/RHEL: yum install -y ansible"
    echo "Ubuntu/Debian: apt-get install -y ansible"
    exit 1
fi

# 检查hosts.ini是否存在
if [ ! -f "hosts.ini" ]; then
    echo "错误: 未找到hosts.ini文件"
    exit 1
fi

echo "即将为以下节点配置SSH免密登录："
echo ""
ansible all --list-hosts 2>/dev/null || true
echo ""

echo "请选择配置方式："
echo "1) 所有节点使用相同密码 (推荐)"
echo "2) Master和Worker使用不同密码"
echo "3) 退出"
read -p "请输入选项 (1-3): " choice

case $choice in
    1)
        echo ""
        echo "配置所有节点使用统一密码..."
        echo "请在提示时输入SSH密码"
        echo ""
        ansible-playbook setup_ssh.yml --ask-pass
        ;;
    2)
        echo ""
        echo "配置Master节点..."
        echo "请输入Master节点的SSH密码"
        ansible-playbook setup_ssh.yml --limit master --ask-pass
        echo ""
        echo "配置Worker节点..."
        echo "请输入Worker节点的SSH密码"
        ansible-playbook setup_ssh.yml --limit worker --ask-pass
        ;;
    3)
        echo "已退出"
        exit 0
        ;;
    *)
        echo "无效选项"
        exit 1
        ;;
esac

echo ""
echo "========================================"
echo "SSH免密登录配置完成！"
echo "========================================"
echo ""
echo "测试连接："
ansible all -m ping

echo ""
if ansible all -m ping > /dev/null 2>&1; then
    echo "✓ 所有节点连接成功"
    echo ""
    echo "继续部署："
    echo "bash deploy_all.sh"
else
    echo "✗ 部分节点连接失败，请检查配置"
fi
echo ""

