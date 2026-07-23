#!/bin/bash
# SSH免密登录快速配置脚本

set -e

echo "========================================"
echo "SSH免密登录配置"
echo "========================================"
echo ""

# 检查hosts.ini
if [ ! -f "hosts.ini" ]; then
    echo "错误: 未找到hosts.ini文件"
    exit 1
fi

echo "即将为以下节点配置SSH免密登录："
echo ""
ansible all --list-hosts
echo ""

read -p "请确认节点列表正确 (y/n): " confirm
if [ "$confirm" != "y" ]; then
    echo "已取消"
    exit 0
fi

echo ""
echo "开始配置SSH免密登录..."
echo "请在提示时输入SSH密码"
echo "========================================"
echo ""

# 执行SSH免密登录配置
ansible-playbook setup_ssh.yml --ask-pass

echo ""
echo "========================================"
echo "配置完成！"
echo "========================================"
echo ""
echo "测试连接："
ansible all -m ping

echo ""
echo "如果所有节点都返回SUCCESS，则配置成功"
echo "现在可以运行: bash deploy_all.sh"
echo ""

