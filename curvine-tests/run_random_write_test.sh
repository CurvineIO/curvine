#!/bin/bash

# Curvine 随机写测试运行脚本
# Copyright 2025 OPPO.

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 默认配置
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${PROJECT_ROOT}/build"
DIST_DIR="${BUILD_DIR}/dist"
CONF_DIR="${DIST_DIR}/conf"
LOG_DIR="${DIST_DIR}/logs"

# 功能函数
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

# 检查环境
check_environment() {
    log_info "检查运行环境..."
    
    # 检查 Rust 环境
    if ! command -v cargo &> /dev/null; then
        log_error "未找到 cargo 命令，请先安装 Rust 环境"
        exit 1
    fi
    
    # 检查项目根目录
    if [ ! -f "${PROJECT_ROOT}/Cargo.toml" ]; then
        log_error "未在项目根目录中找到 Cargo.toml 文件"
        exit 1
    fi
    
    # 检查配置文件
    if [ ! -f "${CONF_DIR}/curvine-cluster.toml" ]; then
        log_error "未找到集群配置文件: ${CONF_DIR}/curvine-cluster.toml"
        log_warn "请确保已构建项目并配置了集群"
        exit 1
    fi
    
    log_success "环境检查通过"
}

# 确保日志目录存在
prepare_logs() {
    log_info "准备日志目录..."
    mkdir -p "${LOG_DIR}"
    log_success "日志目录准备完成: ${LOG_DIR}"
}

# 编译测试程序
build_test() {
    local use_robust=${1:-false}
    log_info "编译随机写测试程序..."
    cd "${PROJECT_ROOT}/curvine-tests"
    
    # 编译智能重叠处理测试版本
    local example_name="random_write_test"
    log_info "编译智能重叠处理随机写测试"
    
    if cargo build --example "${example_name}" --release; then
        log_success "测试程序编译成功"
    else
        log_error "测试程序编译失败"
        exit 1
    fi
}

# 检查集群状态
check_cluster() {
    log_info "检查集群状态..."
    
    # 检查 master 进程
    if [ -f "${DIST_DIR}/master.pid" ]; then
        local master_pid=$(cat "${DIST_DIR}/master.pid")
        if kill -0 "$master_pid" 2>/dev/null; then
            log_success "Master 进程运行中 (PID: $master_pid)"
        else
            log_warn "Master PID 文件存在但进程未运行，将清理 PID 文件"
            rm -f "${DIST_DIR}/master.pid"
        fi
    else
        log_warn "未找到 Master PID 文件，集群可能未启动"
    fi
    
    # 检查 worker 进程
    if [ -f "${DIST_DIR}/worker.pid" ]; then
        local worker_pid=$(cat "${DIST_DIR}/worker.pid")
        if kill -0 "$worker_pid" 2>/dev/null; then
            log_success "Worker 进程运行中 (PID: $worker_pid)"
        else
            log_warn "Worker PID 文件存在但进程未运行，将清理 PID 文件"
            rm -f "${DIST_DIR}/worker.pid"
        fi
    else
        log_warn "未找到 Worker PID 文件，集群可能未启动"
    fi
    
    # 检查 FUSE 进程
    if [ -f "${DIST_DIR}/fuse.pid" ]; then
        local fuse_pid=$(cat "${DIST_DIR}/fuse.pid")
        if kill -0 "$fuse_pid" 2>/dev/null; then
            log_success "FUSE 进程运行中 (PID: $fuse_pid)"
        else
            log_warn "FUSE PID 文件存在但进程未运行，将清理 PID 文件"
            rm -f "${DIST_DIR}/fuse.pid"
        fi
    else
        log_info "FUSE 进程未启动（非必须）"
    fi
}

# 启动集群（如果需要）
start_cluster() {
    log_info "检查是否需要启动集群..."
    
    # 简单检查：如果没有运行的进程，提示用户启动
    if [ ! -f "${DIST_DIR}/master.pid" ] || [ ! -f "${DIST_DIR}/worker.pid" ]; then
        log_warn "检测到集群可能未完全启动"
        echo
        echo "请执行以下命令启动集群："
        echo "  cd ${BUILD_DIR}"
        echo "  ./bin/local-cluster.sh"
        echo
        read -p "是否继续运行测试？[y/N]: " -r
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            exit 0
        fi
    fi
}

# 运行随机写测试
run_test() {
    log_info "开始运行随机写测试..."
    
    local timestamp=$(date +"%Y%m%d_%H%M%S")
    local log_file="${LOG_DIR}/random_write_test_${timestamp}.log"
    
    log_info "测试日志将保存到: ${log_file}"
    
    cd "${PROJECT_ROOT}/curvine-tests"
    
    # 运行测试并输出到控制台和日志文件
    # 运行智能重叠处理测试版本
    local example_name="random_write_test"
    if [[ "${verbose}" == "true" ]]; then
        log_info "运行智能重叠处理测试（详细模式）"
        export CURVINE_TEST_VERBOSE=true
    else
        log_info "运行智能重叠处理随机写测试"
    fi
    
    if cargo run --example "${example_name}" --release 2>&1 | tee "${log_file}"; then
        log_success "随机写测试完成"
        log_info "详细日志已保存到: ${log_file}"
    else
        log_error "随机写测试失败"
        log_info "错误日志已保存到: ${log_file}"
        exit 1
    fi
}

# 显示使用帮助
show_help() {
    cat << EOF
Curvine 随机写测试运行脚本

用法: $0 [选项]

选项:
    -h, --help          显示帮助信息
    --no-build          跳过编译步骤
    --no-cluster-check  跳过集群状态检查
    --verbose           显示详细的重叠处理日志
    --force             强制运行测试（忽略所有检查）

示例:
    $0                  # 完整运行测试（推荐）
    $0 --no-build       # 跳过编译直接运行
    $0 --verbose        # 显示详细的重叠处理过程
    $0 --force          # 强制运行测试

环境要求:
    - Rust 环境 (cargo)
    - 已构建的 Curvine 项目
    - 配置的集群环境

EOF
}

# 主函数
main() {
    local no_build=false
    local no_cluster_check=false
    local force=false
    local verbose=false
    
    # 解析命令行参数
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help)
                show_help
                exit 0
                ;;
            --no-build)
                no_build=true
                shift
                ;;
            --no-cluster-check)
                no_cluster_check=true
                shift
                ;;
            --verbose)
                verbose=true
                shift
                ;;
            --force)
                force=true
                no_build=true
                no_cluster_check=true
                shift
                ;;
            *)
                log_error "未知参数: $1"
                show_help
                exit 1
                ;;
        esac
    done
    
    # 打印标题
    echo
    echo -e "${BLUE}================================"
    echo "  Curvine 随机写测试运行器"  
    echo -e "================================${NC}"
    echo
    
    # 执行检查和运行步骤
    if [ "$force" = false ]; then
        check_environment
        prepare_logs
        
        if [ "$no_build" = false ]; then
            build_test
        fi
        
        if [ "$no_cluster_check" = false ]; then
            check_cluster
            start_cluster
        fi
    else
        log_warn "强制模式：跳过所有检查"
        prepare_logs
    fi
    
    # 运行测试
    run_test
    
    echo
    log_success "测试运行完成！"
    echo
}

# 脚本入口
main "$@"
