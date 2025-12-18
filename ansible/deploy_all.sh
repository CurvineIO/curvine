#!/bin/bash
# 一键部署Curvine集群脚本

set -e

# 设置日志文件
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="${SCRIPT_DIR}/deploy.log"

# 日志函数
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

log_cmd() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] 执行: $*" >> "$LOG_FILE"
    "$@" 2>&1 | tee -a "$LOG_FILE"
    return ${PIPESTATUS[0]}
}

log "========================================"
log "Curvine集群一键部署工具"
log "日志文件: $LOG_FILE"
log "========================================"
log ""

# 检查必要的文件
log "检查必要文件..."
if [ ! -f "hosts.ini" ]; then
    log "错误: 未找到hosts.ini文件"
    exit 1
fi

if [ ! -f "/root/dist.tar.gz" ]; then
    log "警告: 未找到/root/dist.tar.gz文件"
    read -p "是否继续？ (y/n): " continue
    if [ "$continue" != "y" ]; then
        exit 1
    fi
fi

if [ ! -f "/root/dist1.tar.gz" ]; then
    log "警告: 未找到/root/dist1.tar.gz文件"
    read -p "是否继续？ (y/n): " continue
    if [ "$continue" != "y" ]; then
        exit 1
    fi
fi

# 测试连接
log ""
log "快速检测节点连接状态 (超时3秒)..."
log ""

# 创建临时文件存储结果
failed_hosts=$(mktemp)
success_hosts=""
master_ok_count=0
master_fail_count=0
worker_ok_count=0
worker_fail_count=0

# 检查Master节点
log "检查Master节点..."
if ansible master --list-hosts > /dev/null 2>&1; then
    while IFS= read -r host; do
        host=$(echo "$host" | xargs)  # 去除空格
        # 跳过"hosts (X):"这样的行
        if [[ "$host" =~ ^hosts ]]; then
            continue
        fi
        if ansible "$host" -m ping -o > /dev/null 2>&1; then
            master_ok_count=$((master_ok_count + 1))
            success_hosts="$success_hosts,$host"
            echo "  ✓ $host"
        else
            master_fail_count=$((master_fail_count + 1))
            echo "  ✗ $host (连接超时，将跳过)"
            echo "$host" >> "$failed_hosts"
        fi
    done < <(ansible master --list-hosts 2>/dev/null | grep '^ ' | sed 's/^ *//')
fi

echo ""
echo "检查Worker节点..."
if ansible worker --list-hosts > /dev/null 2>&1; then
    while IFS= read -r host; do
        host=$(echo "$host" | xargs)  # 去除空格
        # 跳过"hosts (X):"这样的行
        if [[ "$host" =~ ^hosts ]]; then
            continue
        fi
        if ansible "$host" -m ping -o > /dev/null 2>&1; then
            worker_ok_count=$((worker_ok_count + 1))
            success_hosts="$success_hosts,$host"
            echo "  ✓ $host"
        else
            worker_fail_count=$((worker_fail_count + 1))
            echo "  ✗ $host (连接超时，将跳过)"
            echo "$host" >> "$failed_hosts"
        fi
    done < <(ansible worker --list-hosts 2>/dev/null | grep '^ ' | sed 's/^ *//')
fi

echo ""
echo "连接检测完成："
echo "  Master节点: $master_ok_count 成功, $master_fail_count 失败"
echo "  Worker节点: $worker_ok_count 成功, $worker_fail_count 失败"

# 移除开头的逗号
success_hosts=$(echo "$success_hosts" | sed 's/^,//')

# 计算总数
total_ok=$((master_ok_count + worker_ok_count))
total_fail=$((master_fail_count + worker_fail_count))

# 如果有失败的节点
if [ $total_fail -gt 0 ]; then
    echo ""
    echo "⚠️  以下节点连接失败，将被跳过："
    cat "$failed_hosts" 2>/dev/null || true
fi

echo ""

# 检查是否有可用节点
if [ $total_ok -eq 0 ]; then
    echo "✗ 所有节点都无法连接！"
    echo ""
    echo "请检查："
    echo "1. hosts.ini 文件是否正确配置"
    echo "2. 节点网络是否可达"
    echo "3. SSH服务是否运行"
    echo "4. 是否已配置SSH免密登录"
    echo ""
    rm -f "$failed_hosts"
    exit 1
fi

master_ok=true
worker_ok=true
[ $master_ok_count -eq 0 ] && master_ok=false
[ $worker_ok_count -eq 0 ] && worker_ok=false

# 处理失败节点
if [ $total_fail -gt 0 ]; then
    echo "⚠️  有 $total_fail 个节点连接失败"
    echo ""
    read -p "是否为失败节点配置SSH免密登录？(y/n，或按s跳过失败节点继续部署): " setup_choice
    
    if [ "$setup_choice" == "y" ]; then
        echo ""
        echo "开始配置SSH免密登录..."
        echo "========================================"
        
        # 执行SSH配置，即使有节点失败也继续
        ansible-playbook setup_ssh.yml --ask-pass || true
        
        echo ""
        echo "SSH配置完成，重新检测所有节点..."
        
        # 重新检测 - 重新统计所有节点
        rm -f "$failed_hosts"
        failed_hosts=$(mktemp)
        success_hosts=""
        master_ok_count=0
        master_fail_count=0
        worker_ok_count=0
        worker_fail_count=0
        
        # 重新检测Master节点
        if ansible master --list-hosts > /dev/null 2>&1; then
            while IFS= read -r host; do
                host=$(echo "$host" | xargs)
                if [[ "$host" =~ ^hosts ]]; then
                    continue
                fi
                if ansible "$host" -m ping -o > /dev/null 2>&1; then
                    master_ok_count=$((master_ok_count + 1))
                    success_hosts="$success_hosts,$host"
                    echo "  ✓ Master: $host"
                else
                    master_fail_count=$((master_fail_count + 1))
                    echo "  ✗ Master: $host"
                    echo "$host" >> "$failed_hosts"
                fi
            done < <(ansible master --list-hosts 2>/dev/null | grep '^ ' | sed 's/^ *//')
        fi
        
        # 重新检测Worker节点
        if ansible worker --list-hosts > /dev/null 2>&1; then
            while IFS= read -r host; do
                host=$(echo "$host" | xargs)
                if [[ "$host" =~ ^hosts ]]; then
                    continue
                fi
                if ansible "$host" -m ping -o > /dev/null 2>&1; then
                    worker_ok_count=$((worker_ok_count + 1))
                    success_hosts="$success_hosts,$host"
                    echo "  ✓ Worker: $host"
                else
                    worker_fail_count=$((worker_fail_count + 1))
                    echo "  ✗ Worker: $host"
                    echo "$host" >> "$failed_hosts"
                fi
            done < <(ansible worker --list-hosts 2>/dev/null | grep '^ ' | sed 's/^ *//')
        fi
        
        success_hosts=$(echo "$success_hosts" | sed 's/^,//')
        total_ok=$((master_ok_count + worker_ok_count))
        total_fail=$((master_fail_count + worker_fail_count))
        
        echo ""
        echo "重新检测完成："
        echo "  Master节点: $master_ok_count 成功, $master_fail_count 失败"
        echo "  Worker节点: $worker_ok_count 成功, $worker_fail_count 失败"
        
        if [ $total_ok -eq 0 ]; then
            echo ""
            echo "✗ 所有节点SSH配置都失败"
            rm -f "$failed_hosts"
            exit 1
        fi
        
        if [ $total_fail -gt 0 ]; then
            echo ""
            echo "⚠️  仍有 $total_fail 个节点无法连接，将跳过这些节点继续部署"
            cat "$failed_hosts"
        else
            echo ""
            echo "✓ 所有节点连接成功"
        fi
    elif [ "$setup_choice" != "s" ]; then
        echo "已取消部署"
        rm -f "$failed_hosts"
        exit 0
    fi
else
    log "✓ 所有节点连接正常"
fi

# 收集节点信息
log ""
log "========================================"
log "收集节点信息..."
log "========================================"

NODE_INFO_FILE="${SCRIPT_DIR}/node_info_summary.txt"

# 删除旧的节点信息文件
rm -f "$NODE_INFO_FILE"

# 添加文件头
cat > "$NODE_INFO_FILE" << EOF
================================================================================
Curvine集群节点信息汇总
收集时间: $(date '+%Y-%m-%d %H:%M:%S')
================================================================================

EOF

# 逐个收集节点信息
if [ -n "$success_hosts" ]; then
    IFS=',' read -ra HOSTS <<< "$success_hosts"
    for host in "${HOSTS[@]}"; do
        log "收集节点 $host 的信息..."
        
        # 判断节点类型
        node_type="Unknown"
        if ansible master --list-hosts 2>/dev/null | grep -q "$host"; then
            node_type="Master"
        elif ansible worker --list-hosts 2>/dev/null | grep -q "$host"; then
            node_type="Worker"
        fi
        
        # 写入节点信息到文件
        {
            echo ""
            echo "================================================================================"
            echo "节点: $host ($node_type)"
            echo "================================================================================"
            echo ""
            echo "网络信息:"
        } >> "$NODE_INFO_FILE"
        
        # 获取网络信息
        net_info=$(ansible "$host" -m shell -a "ip addr show | grep -E '^[0-9]+: (eth|en|bond)' -A 5 | awk '/^[0-9]+:/{iface=\$2; gsub(/:/, \"\", iface)} /inet /{print iface\":\"\$2}' | sed 's|/[0-9]*||'" 2>/dev/null | grep -v "^$host" | grep -v "SUCCESS" | grep ":" | sed 's/^/  /')
        if [ -n "$net_info" ]; then
            echo "$net_info" >> "$NODE_INFO_FILE"
        else
            echo "  无网络信息" >> "$NODE_INFO_FILE"
        fi
        
        # 获取lsblk信息
        {
            echo ""
            echo "磁盘信息 (lsblk):"
        } >> "$NODE_INFO_FILE"
        lsblk_info=$(ansible "$host" -m shell -a "lsblk -o NAME,SIZE,TYPE,MOUNTPOINT" 2>/dev/null | grep -v "^$host" | grep -v "SUCCESS" | grep -v "^$")
        if [ -n "$lsblk_info" ]; then
            echo "$lsblk_info" | sed 's/^/  /' >> "$NODE_INFO_FILE"
        else
            echo "  无法获取lsblk信息" >> "$NODE_INFO_FILE"
        fi
        
        # 获取df信息（包含表头）
        {
            echo ""
            echo "磁盘使用情况 (df -h):"
        } >> "$NODE_INFO_FILE"
        df_info=$(ansible "$host" -m shell -a "df -h | head -1; df -h | grep '^/dev'" 2>/dev/null | grep -v "^$host" | grep -v "SUCCESS" | grep -v "^$")
        if [ -n "$df_info" ]; then
            echo "$df_info" | sed 's/^/  /' >> "$NODE_INFO_FILE"
        else
            echo "  无法获取磁盘使用信息" >> "$NODE_INFO_FILE"
        fi
        
        echo "" >> "$NODE_INFO_FILE"
    done
fi

log ""
log "========================================"
log "节点信息收集完成"
log "详细信息保存位置: $NODE_INFO_FILE"
log "========================================"

# 显示节点信息摘要并确认
log ""
log "即将部署以下节点的详细信息："
log ""

# 显示node_info_summary.txt的内容
if [ -f "$NODE_INFO_FILE" ]; then
    cat "$NODE_INFO_FILE" | tee -a "$LOG_FILE"
else
    log "警告: 未找到节点信息文件: $NODE_INFO_FILE"
    log "继续部署可能存在风险，请人工确认节点配置"
    log ""
    log "手动显示节点列表:"
    if [ -n "$success_hosts" ]; then
        echo "$success_hosts" | tr ',' '\n' | sed 's/^/  /' | tee -a "$LOG_FILE"
    fi
fi

log ""
log "========================================"
read -p "请仔细检查以上节点信息，确认无误后输入 Y 继续部署，输入 N 取消: " confirm_info
log "用户输入: $confirm_info"

if [ "$confirm_info" != "Y" ] && [ "$confirm_info" != "y" ]; then
    log "用户取消部署"
    rm -f "$failed_hosts"
    exit 0
fi

# ========== 磁盘格式化和挂载流程 ==========
log ""
log "========================================"
log "检测未格式化的磁盘..."
log "========================================"

UNFORMAT_DISK_FILE="${SCRIPT_DIR}/unformatted_disks_info.txt"
rm -f "$UNFORMAT_DISK_FILE"

# 检测每个节点的未格式化磁盘
declare -A node_unformatted_disks
declare -A node_mountpoints
has_unformatted=false

if [ -n "$success_hosts" ]; then
    IFS=',' read -ra HOSTS <<< "$success_hosts"
    for host in "${HOSTS[@]}"; do
        log "检测节点 $host 的未格式化磁盘..."
        
        # 创建检测脚本
        cat > /tmp/detect_unformatted_${host}.sh << 'DETECT_SCRIPT_EOF'
#!/bin/bash
# 获取所有MOUNTPOINT为空的disk类型设备
empty_disks=$(lsblk -nlo NAME,TYPE,MOUNTPOINT | grep 'disk' | awk '$3=="" {print $1}')

for disk in $empty_disks; do
    # 使用lsblk列出该磁盘及其所有子设备（正确处理RAID、LVM等）
    # -s 参数：显示依赖关系（子设备）
    all_children=$(lsblk -no NAME "/dev/${disk}" 2>/dev/null | wc -l)
    # 减1是磁盘本身
    has_children=$((all_children - 1))
    
    # 检查磁盘本身是否在df中
    in_df=$(df -h | grep "/dev/${disk}" | wc -l)
    
    # 检查该磁盘的任何子设备是否有挂载点
    children_mounted=$(lsblk -no MOUNTPOINT "/dev/${disk}" 2>/dev/null | grep -v '^$' | wc -l)
    # 如果磁盘本身有挂载点，减1
    if lsblk -no MOUNTPOINT "/dev/${disk}" 2>/dev/null | head -1 | grep -q '^$'; then
        # 磁盘本身无挂载点，不用减
        :
    else
        # 磁盘本身有挂载点，减1
        children_mounted=$((children_mounted > 0 ? children_mounted - 1 : 0))
    fi
    
    # 输出调试信息到stderr（不影响结果）
    # echo "DEBUG: $disk has_children=$has_children in_df=$in_df children_mounted=$children_mounted" >&2
    
    # 如果没有子设备、不在df中、且没有任何挂载点（包括子设备），则是未格式化的
    if [ "$has_children" -eq 0 ] && [ "$in_df" -eq 0 ]; then
        # 再确认一下该磁盘树下没有任何挂载点
        total_mounts=$(lsblk -no MOUNTPOINT "/dev/${disk}" 2>/dev/null | grep -v '^$' | wc -l)
        if [ "$total_mounts" -eq 0 ]; then
            echo "$disk"
        fi
    fi
done
DETECT_SCRIPT_EOF

        # 拷贝脚本到远程节点
        scp -q -o StrictHostKeyChecking=no -o ConnectTimeout=3 /tmp/detect_unformatted_${host}.sh root@${host}:/tmp/detect_unformatted.sh 2>/dev/null || \
        ansible "$host" -m copy -a "src=/tmp/detect_unformatted_${host}.sh dest=/tmp/detect_unformatted.sh mode=0755" > /dev/null 2>&1
        
        # 执行检测脚本
        unformatted=$(ansible "$host" -m shell -a "bash /tmp/detect_unformatted.sh" 2>/dev/null | \
                      grep -v "^$host" | grep -v "SUCCESS" | grep -v "changed" | grep -v "rc=0" | \
                      grep -E '^(sd[a-z]+|vd[a-z]+|nvme[0-9]+n[0-9]+|xvd[a-z]+)$' | xargs)
        
        # 清理临时文件
        rm -f /tmp/detect_unformatted_${host}.sh
        ansible "$host" -m file -a "path=/tmp/detect_unformatted.sh state=absent" > /dev/null 2>&1
        
        if [ -n "$unformatted" ]; then
            log "  节点 $host 发现未格式化磁盘: $unformatted"
            node_unformatted_disks["$host"]="$unformatted"
            echo "$host:$unformatted" >> "$UNFORMAT_DISK_FILE"
            has_unformatted=true
        else
            log "  节点 $host 没有未格式化磁盘"
        fi
    done
fi

log ""

# 如果有未格式化的磁盘，显示并询问是否格式化
if [ "$has_unformatted" = true ]; then
    log "========================================"
    log "发现未格式化的磁盘："
    log "========================================"
    log ""
    
    for host in "${!node_unformatted_disks[@]}"; do
        disks="${node_unformatted_disks[$host]}"
        log "节点 $host:"
        
        # 获取该节点已存在的最大datads序号
        max_num=$(ansible "$host" -m shell -a "df -h | grep '/datads' | sed 's|.*/datads||' | awk '{print \$1}' | sort -n | tail -1" 2>/dev/null | \
                  grep -v "^$host" | grep -v "SUCCESS" | grep -E '^[0-9]+$' | head -1)
        
        if [ -z "$max_num" ]; then
            disk_num=1
        else
            disk_num=$((max_num + 1))
        fi
        
        log "  (当前最大序号: ${max_num:-0}, 新磁盘从 /datads${disk_num} 开始)"
        
        for disk in $disks; do
            log "  - /dev/$disk -> 将挂载到 /datads${disk_num}"
            disk_num=$((disk_num + 1))
        done
        log ""
    done
    
    log "========================================"
    read -p "是否格式化并挂载这些磁盘？(Y/n，输入n跳过): " format_confirm
    log "用户输入: $format_confirm"
    
    if [ "$format_confirm" = "Y" ] || [ "$format_confirm" = "y" ]; then
        log ""
        log "========================================"
        log "开始格式化和挂载磁盘..."
        log "========================================"
        
        # 为每个节点格式化和挂载，并记录挂载点
        for host in "${!node_unformatted_disks[@]}"; do
            disks="${node_unformatted_disks[$host]}"
            log "处理节点 $host..."
            
            # 获取该节点已存在的最大datads序号
            max_num=$(ansible "$host" -m shell -a "df -h | grep '/datads' | sed 's|.*/datads||' | awk '{print \$1}' | sort -n | tail -1" 2>/dev/null | \
                      grep -v "^$host" | grep -v "SUCCESS" | grep -E '^[0-9]+$' | head -1)
            
            if [ -z "$max_num" ]; then
                disk_num=1
            else
                disk_num=$((max_num + 1))
            fi
            
            log "  当前最大序号: ${max_num:-0}, 新磁盘从 /datads${disk_num} 开始"
            
            host_mounts=""
            for disk in $disks; do
                mountpoint="/datads${disk_num}"
                
                log "  格式化 /dev/$disk..."
                ansible "$host" -m shell -a "mkfs.ext4 -F /dev/$disk" 2>&1 | tee -a "$LOG_FILE"
                
                log "  创建挂载点 $mountpoint..."
                ansible "$host" -m shell -a "mkdir -p $mountpoint && chmod 755 $mountpoint" 2>&1 | tee -a "$LOG_FILE"
                
                log "  挂载 /dev/$disk 到 $mountpoint..."
                ansible "$host" -m shell -a "mount /dev/$disk $mountpoint" 2>&1 | tee -a "$LOG_FILE"
                
                log "  添加到 /etc/fstab..."
                ansible "$host" -m shell -a "echo '/dev/$disk $mountpoint ext4 defaults 0 0' >> /etc/fstab" 2>&1 | tee -a "$LOG_FILE"
                
                log "  ✓ /dev/$disk 格式化并挂载到 $mountpoint 完成"
                
                # 记录挂载点
                if [ -z "$host_mounts" ]; then
                    host_mounts="$mountpoint"
                else
                    host_mounts="$host_mounts,$mountpoint"
                fi
                
                disk_num=$((disk_num + 1))
            done
            
            # 保存该节点的所有挂载点
            node_mountpoints["$host"]="$host_mounts"
            log ""
        done
        
        log "========================================"
        log "格式化和挂载完成，显示更新后的磁盘信息："
        log "========================================"
        log ""
        
        # 显示格式化后的磁盘信息
        for host in "${!node_unformatted_disks[@]}"; do
            # 判断节点类型
            node_type="Unknown"
            if ansible master --list-hosts 2>/dev/null | grep -q "$host"; then
                node_type="Master"
            elif ansible worker --list-hosts 2>/dev/null | grep -q "$host"; then
                node_type="Worker"
            fi
            
            log "================================================================================"
            log "节点: $host ($node_type) - 更新后的磁盘信息"
            log "================================================================================"
            log ""
            
            log "lsblk:"
            ansible "$host" -m shell -a "lsblk -o NAME,SIZE,TYPE,MOUNTPOINT" 2>/dev/null | \
            grep -v "^$host" | grep -v "SUCCESS" | grep -v "^$" | sed 's/^/  /' | tee -a "$LOG_FILE"
            
            log ""
            log "df -h:"
            ansible "$host" -m shell -a "df -h | head -1; df -h | grep '^/dev'" 2>/dev/null | \
            grep -v "^$host" | grep -v "SUCCESS" | grep -v "^$" | sed 's/^/  /' | tee -a "$LOG_FILE"
            
            log ""
        done
        
        log "========================================"
        read -p "请确认磁盘格式化和挂载结果，输入 Y 继续部署，输入 N 取消: " format_result_confirm
        log "用户输入: $format_result_confirm"
        
        if [ "$format_result_confirm" != "Y" ] && [ "$format_result_confirm" != "y" ]; then
            log "用户取消部署"
            rm -f "$failed_hosts"
            exit 0
        fi
    else
        log "跳过磁盘格式化"
    fi
else
    log "所有节点没有未格式化的磁盘，跳过格式化步骤"
fi

# 确认部署
log ""
if [ -n "$success_hosts" ]; then
    log "即将部署以下节点："
    echo "$success_hosts" | tr ',' '\n' | sed 's/^/  /' | tee -a "$LOG_FILE"
else
    log "没有可用节点进行部署"
    rm -f "$failed_hosts"
    exit 1
fi

log ""
log "将执行以下操作："
log "1. 配置环境变量"
log "2. 分发安装包"
log "3. 配置data_dir"
log "4. 创建systemd服务"
log ""
read -p "确认开始部署？ (y/n): " confirm
log "用户输入: $confirm"
if [ "$confirm" != "y" ]; then
    log "部署已取消"
    rm -f "$failed_hosts"
    exit 0
fi

# 执行部署（只部署连接成功的节点）
log ""
log "========================================" 
log "开始部署..."
log "========================================"
if [ -n "$success_hosts" ]; then
    log_cmd ansible-playbook deploy_curvine.yml --limit "$success_hosts" || true
    deploy_result=$?
else
    log_cmd ansible-playbook deploy_curvine.yml || true
    deploy_result=$?
fi

# 更新配置文件（如果有新格式化的磁盘）
if [ ${#node_mountpoints[@]} -gt 0 ]; then
    log ""
    log "========================================"
    log "更新配置文件以使用新挂载的磁盘..."
    log "========================================"
    log ""
    
    for host in "${!node_mountpoints[@]}"; do
        mountpoints="${node_mountpoints[$host]}"
        IFS=',' read -ra MOUNTS <<< "$mountpoints"
        first_mount="${MOUNTS[0]}"
        
        # 判断节点类型
        is_master=false
        if ansible master --list-hosts 2>/dev/null | grep -q "$host"; then
            is_master=true
        fi
        
        log "更新节点 $host 的配置..."
        
        if [ "$is_master" = true ]; then
            # Master节点：更新meta_dir、journal_dir、data_dir
            log "  节点类型: Master"
            log "  使用Python脚本更新配置（更可靠）"
            
            # 先显示当前配置文件中的data_dir实际内容
            log "  [DEBUG] 执行前，检查目标节点当前的data_dir配置..."
            current_data_dir=$(ansible "$host" -m shell -a "grep '^data_dir' /root/dist/conf/curvine-cluster.toml" 2>/dev/null | \
                grep -v "SUCCESS" | grep -v "changed" | grep -v "rc=")
            log "  [DEBUG] 当前data_dir: $current_data_dir"
            
            # 拷贝Python更新脚本到远程节点
            scp -q -o StrictHostKeyChecking=no update_toml_config.py root@${host}:/tmp/ 2>/dev/null || \
            ansible "$host" -m copy -a "src=update_toml_config.py dest=/tmp/update_toml_config.py mode=0755" > /dev/null 2>&1
            
            # 执行Python脚本更新配置
            log "  更新配置: meta_dir和journal_dir使用 ${first_mount}, data_dir追加所有挂载点"
            ansible "$host" -m shell -a "cd /root/dist/conf && python3 /tmp/update_toml_config.py master curvine-cluster.toml ${first_mount} ${mountpoints}" 2>&1 | \
                grep -v "SUCCESS" | grep -v "changed" | tee -a "$LOG_FILE"
            
            # 显示更新后的data_dir
            log "  [DEBUG] 执行后，检查目标节点更新后的data_dir配置..."
            updated_data_dir=$(ansible "$host" -m shell -a "grep '^data_dir' /root/dist/conf/curvine-cluster.toml" 2>/dev/null | \
                grep -v "SUCCESS" | grep -v "changed" | grep -v "rc=")
            log "  [DEBUG] 更新后data_dir: $updated_data_dir"
            
            # 清理
            ansible "$host" -m file -a "path=/tmp/update_toml_config.py state=absent" > /dev/null 2>&1
            
        else
            # Worker节点：只更新data_dir
            log "  节点类型: Worker"
            log "  使用Python脚本更新配置（更可靠）"
            
            # 拷贝Python更新脚本到远程节点
            scp -q -o StrictHostKeyChecking=no update_toml_config.py root@${host}:/tmp/ 2>/dev/null || \
            ansible "$host" -m copy -a "src=update_toml_config.py dest=/tmp/update_toml_config.py mode=0755" > /dev/null 2>&1
            
            # 执行Python脚本更新配置
            log "  更新配置: data_dir追加所有挂载点"
            ansible "$host" -m shell -a "python3 /tmp/update_toml_config.py worker /root/dist/conf/curvine-cluster.toml ${mountpoints}" 2>&1 | \
                grep -v "SUCCESS" | grep -v "changed" | tee -a "$LOG_FILE"
            
            # 清理
            ansible "$host" -m file -a "path=/tmp/update_toml_config.py state=absent" > /dev/null 2>&1
        fi
        
        log ""
    done
    
    # ==========以下旧代码已废弃，改用Python脚本更新配置==========
    : <<'DEPRECATED_CODE'
            # 以下代码不再使用
            cat > /tmp/update_config_${host}.sh << 'UPDATE_SCRIPT_EOF'
#!/bin/bash
cd /root/dist/conf
cp curvine-cluster.toml curvine-cluster.toml.bak

# 1. 首先处理meta_dir和journal_dir（只在是/data/*且不包含datads时更新）
first_mount="__FIRST_MOUNT__"

current_meta=$(grep '^meta_dir' curvine-cluster.toml)
if echo "$current_meta" | grep -q 'meta_dir = "/data/' && ! echo "$current_meta" | grep -q '/datads'; then
    sed -i "s|meta_dir = \"/data/|meta_dir = \"${first_mount}/|g" curvine-cluster.toml
    echo "meta_dir已更新为: ${first_mount}/meta"
else
    echo "meta_dir保持不变（已经是/datads*或非/data/*）"
fi

current_journal=$(grep '^journal_dir' curvine-cluster.toml)
if echo "$current_journal" | grep -q 'journal_dir = "/data/' && ! echo "$current_journal" | grep -q '/datads'; then
    sed -i "s|journal_dir = \"/data/|journal_dir = \"${first_mount}/|g" curvine-cluster.toml
    echo "journal_dir已更新为: ${first_mount}/journal"
else
    echo "journal_dir保持不变（已经是/datads*或非/data/*）"
fi

# 2. 读取现有的data_dir（支持单行和多行格式，支持[SSD]格式）
echo "======== DEBUG: 开始读取data_dir ========"

# 显示data_dir相关的原始行
echo "--- data_dir原始内容 ---"
grep -A 5 '^data_dir' curvine-cluster.toml || echo "未找到data_dir"

# 方法：提取data_dir行的内容
data_dir_content=$(grep '^data_dir' curvine-cluster.toml)
echo "data_dir行内容: $data_dir_content"

# 提取所有路径（改为匹配所有引号内容，不限制必须以/开头）
existing_data_dirs=$(echo "$data_dir_content" | grep -o '"[^"]*"' | tr -d '"')

echo "单行模式提取结果:"
if [ -n "$existing_data_dirs" ]; then
    echo "$existing_data_dirs"
else
    echo "(空)"
fi

# 如果单行没有提取到，尝试多行模式
if [ -z "$existing_data_dirs" ]; then
    echo "单行模式未提取到，尝试多行模式..."
    data_dir_line=$(grep -n '^data_dir' curvine-cluster.toml | head -1 | cut -d: -f1)
    if [ -n "$data_dir_line" ]; then
        existing_data_dirs=$(sed -n "${data_dir_line},/\]/p" curvine-cluster.toml | \
            grep -o '"[^"]*"' | \
            tr -d '"')
        echo "多行模式提取结果:"
        echo "$existing_data_dirs"
    fi
fi

echo "======== DEBUG: 现有data_dir路径 ========"
if [ -n "$existing_data_dirs" ]; then
    echo "$existing_data_dirs"
else
    echo "(空)"
fi
echo "========================================"

# 新挂载点列表
new_mounts="__NEW_MOUNTS__"
echo "新增挂载点: $new_mounts"

# 合并现有和新的路径
all_data_dirs=""
for dir in $existing_data_dirs; do
    if [ -z "$all_data_dirs" ]; then
        all_data_dirs="\"$dir\""
    else
        all_data_dirs="$all_data_dirs, \"$dir\""
    fi
done

# 追加新的挂载点
IFS=',' read -ra NEW_MOUNT_ARRAY <<< "$new_mounts"
for mp in "${NEW_MOUNT_ARRAY[@]}"; do
    new_path="$mp/data"
    # 检查是否已存在
    if ! echo "$existing_data_dirs" | grep -q "$new_path"; then
        if [ -z "$all_data_dirs" ]; then
            all_data_dirs="\"$new_path\""
        else
            all_data_dirs="$all_data_dirs, \"$new_path\""
        fi
        echo "追加新路径: $new_path"
    else
        echo "跳过重复路径: $new_path"
    fi
done

echo "合并后的data_dir: [$all_data_dirs]"

# 更新data_dir
sed -i "/^data_dir = \[/,/\]/c\data_dir = [$all_data_dirs]" curvine-cluster.toml

echo "data_dir更新完成"
UPDATE_SCRIPT_EOF
            
            # 替换脚本中的变量
            sed -i "s|__FIRST_MOUNT__|${first_mount}|g" /tmp/update_config_${host}.sh
            sed -i "s|__NEW_MOUNTS__|${mountpoints}|g" /tmp/update_config_${host}.sh
            
            log "  meta_dir和journal_dir: 只在当前是/data/*（非/datads*）时更新"
            log "  data_dir: 追加新挂载点 ${mountpoints}，保留现有路径"
            
        else
            # Worker节点：只更新data_dir（追加）
            log "  节点类型: Worker"
            
            # 先显示当前配置文件中的data_dir实际内容
            log "  [DEBUG] 执行前，检查目标节点当前的data_dir配置..."
            current_data_dir=$(ansible "$host" -m shell -a "grep '^data_dir' /root/dist/conf/curvine-cluster.toml" 2>/dev/null | \
                grep -v "SUCCESS" | grep -v "changed" | grep -v "rc=")
            log "  [DEBUG] 当前data_dir: $current_data_dir"
            
            # 拷贝Python更新脚本到远程节点
            scp -q -o StrictHostKeyChecking=no update_toml_config.py root@${host}:/tmp/ 2>/dev/null || \
            ansible "$host" -m copy -a "src=update_toml_config.py dest=/tmp/update_toml_config.py mode=0755" > /dev/null 2>&1
            
            # 执行Python脚本更新配置
            log "  更新配置: data_dir追加所有挂载点"
            ansible "$host" -m shell -a "cd /root/dist/conf && python3 /tmp/update_toml_config.py worker curvine-cluster.toml ${mountpoints}" 2>&1 | \
                grep -v "SUCCESS" | grep -v "changed" | tee -a "$LOG_FILE"
            
            # 显示更新后的data_dir
            log "  [DEBUG] 执行后，检查目标节点更新后的data_dir配置..."
            updated_data_dir=$(ansible "$host" -m shell -a "grep '^data_dir' /root/dist/conf/curvine-cluster.toml" 2>/dev/null | \
                grep -v "SUCCESS" | grep -v "changed" | grep -v "rc=")
            log "  [DEBUG] 更新后data_dir: $updated_data_dir"
            
            # 清理
            ansible "$host" -m file -a "path=/tmp/update_toml_config.py state=absent" > /dev/null 2>&1
        fi
        
        log ""
    done
    
    # ==========以下旧代码已废弃，改用Python脚本更新配置==========
    : <<'DEPRECATED_CODE'
            # 以下代码不再使用
            cat > /tmp/update_config_${host}.sh << 'UPDATE_SCRIPT_EOF'
#!/bin/bash
cd /root/dist/conf
cp curvine-cluster.toml curvine-cluster.toml.bak

# 读取现有的data_dir（支持单行和多行格式）
echo "======== DEBUG: 开始读取data_dir ========"

# 显示data_dir相关的原始行
echo "--- data_dir原始内容 ---"
grep -A 5 '^data_dir' curvine-cluster.toml || echo "未找到data_dir"

# 方法：提取data_dir行的内容
data_dir_content=$(grep '^data_dir' curvine-cluster.toml)
echo "data_dir行内容: $data_dir_content"

# 提取所有路径
existing_data_dirs=$(echo "$data_dir_content" | grep -o '"/[^"]*"' | tr -d '"')

# 如果单行没有提取到，尝试多行模式
if [ -z "$existing_data_dirs" ]; then
    echo "单行模式未提取到，尝试多行模式..."
    data_dir_line=$(grep -n '^data_dir' curvine-cluster.toml | head -1 | cut -d: -f1)
    if [ -n "$data_dir_line" ]; then
        existing_data_dirs=$(sed -n "${data_dir_line},/\]/p" curvine-cluster.toml | \
            grep -o '"/[^"]*"' | \
            tr -d '"')
    fi
fi

echo "======== DEBUG: 现有data_dir路径 ========"
if [ -n "$existing_data_dirs" ]; then
    echo "$existing_data_dirs"
else
    echo "(空)"
fi
echo "========================================"

# 新挂载点列表
new_mounts="__NEW_MOUNTS__"
echo "新增挂载点: $new_mounts"

# 合并现有和新的路径
all_data_dirs=""
for dir in $existing_data_dirs; do
    if [ -z "$all_data_dirs" ]; then
        all_data_dirs="\"$dir\""
    else
        all_data_dirs="$all_data_dirs, \"$dir\""
    fi
done

# 追加新的挂载点
IFS=',' read -ra NEW_MOUNT_ARRAY <<< "$new_mounts"
for mp in "${NEW_MOUNT_ARRAY[@]}"; do
    new_path="$mp/data"
    # 检查是否已存在
    if ! echo "$existing_data_dirs" | grep -q "$new_path"; then
        if [ -z "$all_data_dirs" ]; then
            all_data_dirs="\"$new_path\""
        else
            all_data_dirs="$all_data_dirs, \"$new_path\""
        fi
        echo "追加新路径: $new_path"
    else
        echo "跳过重复路径: $new_path"
    fi
done

echo "合并后的data_dir: [$all_data_dirs]"

# 更新data_dir
sed -i "/^data_dir = \[/,/\]/c\data_dir = [$all_data_dirs]" curvine-cluster.toml

echo "data_dir更新完成"
UPDATE_SCRIPT_EOF
            
            # 替换脚本中的变量
            sed -i "s|__NEW_MOUNTS__|${mountpoints}|g" /tmp/update_config_${host}.sh
            
            log "  data_dir: 追加新挂载点 ${mountpoints}，保留现有路径"
        fi
        
DEPRECATED_CODE
    # ==========旧代码结束==========
    
    log "========================================"
    log "配置文件已更新，显示修改后的配置："
    log "========================================"
    log ""
    
    # 显示每个节点更新后的配置
    for host in "${!node_mountpoints[@]}"; do
        is_master=false
        if ansible master --list-hosts 2>/dev/null | grep -q "$host"; then
            is_master=true
        fi
        
        log "================================================================================"
        log "节点: $host ($( [ "$is_master" = true ] && echo "Master" || echo "Worker" ))"
        log "================================================================================"
        
        if [ "$is_master" = true ]; then
            # Master节点：显示meta_dir、journal_dir、data_dir（包含多行）
            log "--- meta_dir ---"
            meta_result=$(ansible "$host" -m shell -a "grep '^meta_dir' /root/dist/conf/curvine-cluster.toml" 2>/dev/null | \
                grep -v "^$host" | grep -v "SUCCESS" | grep -v "changed" | grep -v "rc=")
            if [ -n "$meta_result" ]; then
                echo "$meta_result" | tee -a "$LOG_FILE"
            else
                log "无法读取meta_dir"
            fi
            
            log ""
            log "--- journal_dir ---"
            journal_result=$(ansible "$host" -m shell -a "grep '^journal_dir' /root/dist/conf/curvine-cluster.toml" 2>/dev/null | \
                grep -v "^$host" | grep -v "SUCCESS" | grep -v "changed" | grep -v "rc=")
            if [ -n "$journal_result" ]; then
                echo "$journal_result" | tee -a "$LOG_FILE"
            else
                log "无法读取journal_dir"
            fi
            
            log ""
            log "--- data_dir ---"
            log "[DEBUG] 开始读取 $host 的配置文件..."
            
            # 直接使用fetch获取配置文件到本地
            ansible "$host" -m fetch -a "src=/root/dist/conf/curvine-cluster.toml dest=/tmp/config_${host}.toml flat=yes" > /dev/null 2>&1
            
            log "[DEBUG] 检查文件是否下载成功..."
            if [ -f /tmp/config_${host}.toml ]; then
                log "[DEBUG] 文件存在，大小: $(wc -c < /tmp/config_${host}.toml) 字节"
                log "[DEBUG] 提取data_dir部分..."
                
                # 使用awk提取data_dir
                data_dir_content=$(awk '/^data_dir = \[/,/^\]/' /tmp/config_${host}.toml)
                
                if [ -n "$data_dir_content" ]; then
                    log "[DEBUG] data_dir内容提取成功"
                    echo "$data_dir_content" | tee -a "$LOG_FILE"
                else
                    log "[DEBUG] awk未匹配到data_dir，尝试grep..."
                    grep -A 10 "data_dir" /tmp/config_${host}.toml | head -15 | tee -a "$LOG_FILE"
                fi
                
                rm -f /tmp/config_${host}.toml
            else
                log "[DEBUG] 文件下载失败"
                log "[DEBUG] 尝试直接显示..."
                ansible "$host" -m shell -a "cat /root/dist/conf/curvine-cluster.toml | grep -A 10 data_dir | head -15" 2>&1 | \
                    grep -v "SUCCESS" | grep -v "changed" | tee -a "$LOG_FILE"
            fi
        else
            # Worker节点：只显示data_dir（包含多行）
            log "--- data_dir ---"
            log "[DEBUG] 开始读取 $host 的配置文件..."
            
            # 直接使用fetch获取配置文件到本地
            ansible "$host" -m fetch -a "src=/root/dist/conf/curvine-cluster.toml dest=/tmp/config_${host}.toml flat=yes" > /dev/null 2>&1
            
            log "[DEBUG] 检查文件是否下载成功..."
            if [ -f /tmp/config_${host}.toml ]; then
                log "[DEBUG] 文件存在，大小: $(wc -c < /tmp/config_${host}.toml) 字节"
                log "[DEBUG] 提取data_dir部分..."
                
                # 使用awk提取data_dir
                data_dir_content=$(awk '/^data_dir = \[/,/^\]/' /tmp/config_${host}.toml)
                
                if [ -n "$data_dir_content" ]; then
                    log "[DEBUG] data_dir内容提取成功"
                    echo "$data_dir_content" | tee -a "$LOG_FILE"
                else
                    log "[DEBUG] awk未匹配到data_dir，尝试grep..."
                    grep -A 10 "data_dir" /tmp/config_${host}.toml | head -15 | tee -a "$LOG_FILE"
                fi
                
                rm -f /tmp/config_${host}.toml
            else
                log "[DEBUG] 文件下载失败"
                log "[DEBUG] 尝试直接显示..."
                ansible "$host" -m shell -a "cat /root/dist/conf/curvine-cluster.toml | grep -A 10 data_dir | head -15" 2>&1 | \
                    grep -v "SUCCESS" | grep -v "changed" | tee -a "$LOG_FILE"
            fi
        fi
        log ""
    done
    
    log "========================================"
    read -p "请确认配置文件修改正确，输入 Y 继续启动服务，输入 N 取消: " config_confirm
    log "用户输入: $config_confirm"
    
    if [ "$config_confirm" != "Y" ] && [ "$config_confirm" != "y" ]; then
        log "用户取消启动服务"
        log "提示: 可以手动修改配置文件后，运行 ansible-playbook start_services.yml 启动服务"
        exit 0
    fi
fi

# 询问是否启动服务
if [ $deploy_result -eq 0 ]; then
    log ""
    
    # 如果没有格式化磁盘，还是要问是否启动
    if [ ${#node_mountpoints[@]} -eq 0 ]; then
        read -p "部署完成！是否立即启动服务？ (y/n): " start_services
        log "用户输入: $start_services"
    else
        # 如果已经确认过配置，直接启动
        start_services="y"
        log "配置已确认，开始启动服务..."
    fi
    
    if [ "$start_services" == "y" ]; then
        log ""
        log "========================================"
        log "检查并修复环境变量CURVINE_MASTER_HOSTNAME..."
        log "========================================"
        
        # 检查并修复每个节点的环境变量
        if [ -n "$success_hosts" ]; then
            IFS=',' read -ra ALL_HOSTS <<< "$success_hosts"
            for host in "${ALL_HOSTS[@]}"; do
                # 判断节点类型
                is_master_node=false
                if ansible master --list-hosts 2>/dev/null | grep -q "$host"; then
                    is_master_node=true
                fi
                
                # 获取当前环境变量值
                current_env=$(ansible "$host" -m shell -a "grep 'CURVINE_MASTER_HOSTNAME' /etc/profile | grep -v '^#' | tail -1" 2>/dev/null | \
                    grep -v "SUCCESS" | grep -v "changed" | grep -v "rc=" | sed 's/.*CURVINE_MASTER_HOSTNAME=//' | xargs)
                
                if [ "$is_master_node" = true ]; then
                    # Master节点：应该是自己的IP
                    # 获取该节点的eth0或bond0 IP
                    node_ip=$(ansible "$host" -m shell -a "ip addr show eth0 2>/dev/null | grep 'inet ' | awk '{print \$2}' | cut -d/ -f1 || ip addr show bond0 2>/dev/null | grep 'inet ' | awk '{print \$2}' | cut -d/ -f1" 2>/dev/null | \
                        grep -v "SUCCESS" | grep -v "changed" | grep -E '^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$' | head -1)
                    
                    if [ -z "$node_ip" ]; then
                        node_ip="$host"  # 如果获取失败，使用主机IP
                    fi
                    
                    log "节点 $host (Master):"
                    log "  当前环境变量: CURVINE_MASTER_HOSTNAME=$current_env"
                    log "  应该设置为: $node_ip"
                    
                    if [ "$current_env" != "$node_ip" ]; then
                        log "  ⚠️  环境变量不正确，正在修复..."
                        
                        # 删除旧的设置
                        ansible "$host" -m shell -a "sed -i '/CURVINE_MASTER_HOSTNAME/d' /etc/profile" 2>&1 | tee -a "$LOG_FILE"
                        
                        # 添加新的设置
                        ansible "$host" -m shell -a "echo 'export CURVINE_MASTER_HOSTNAME=$node_ip' >> /etc/profile" 2>&1 | tee -a "$LOG_FILE"
                        
                        # Source生效
                        ansible "$host" -m shell -a "source /etc/profile" 2>&1 | tee -a "$LOG_FILE"
                        
                        log "  ✓ 已修复为: $node_ip"
                    else
                        log "  ✓ 环境变量正确"
                    fi
                else
                    # Worker节点：应该是localhost
                    log "节点 $host (Worker):"
                    log "  当前环境变量: CURVINE_MASTER_HOSTNAME=$current_env"
                    log "  应该设置为: localhost"
                    
                    if [ "$current_env" != "localhost" ]; then
                        log "  ⚠️  环境变量不正确，正在修复..."
                        
                        # 删除旧的设置
                        ansible "$host" -m shell -a "sed -i '/CURVINE_MASTER_HOSTNAME/d' /etc/profile" 2>&1 | tee -a "$LOG_FILE"
                        
                        # 添加新的设置
                        ansible "$host" -m shell -a "echo 'export CURVINE_MASTER_HOSTNAME=localhost' >> /etc/profile" 2>&1 | tee -a "$LOG_FILE"
                        
                        # Source生效
                        ansible "$host" -m shell -a "source /etc/profile" 2>&1 | tee -a "$LOG_FILE"
                        
                        log "  ✓ 已修复为: localhost"
                    else
                        log "  ✓ 环境变量正确"
                    fi
                fi
                log ""
            done
        fi
        
        log ""
        log "========================================"
        log "检查并更新master_addrs和journal_addrs..."
        log "========================================"
        
        # 获取所有master节点IP
        master_ips=$(ansible master --list-hosts 2>/dev/null | grep '^ ' | sed 's/^ *//' | grep -v "hosts" | xargs)
        log "Master节点IP列表: $master_ips"
        
        # 为所有节点更新master_addrs和journal_addrs
        if [ -n "$success_hosts" ]; then
            IFS=',' read -ra ALL_HOSTS <<< "$success_hosts"
            for host in "${ALL_HOSTS[@]}"; do
                log "检查节点 $host 的master_addrs配置..."
                
                # 检查配置文件是否存在（添加错误处理）
                has_config=$(ansible "$host" -m stat -a "path=/root/dist/conf/curvine-cluster.toml" 2>/dev/null | grep -c "exists.*true" || echo "0")
                
                if [ "$has_config" -gt 0 ]; then
                    # 构建master_addrs (master_port=8995)
                    master_addrs_content=""
                    for ip in $master_ips; do
                        if [ -z "$master_addrs_content" ]; then
                            master_addrs_content="    { hostname = \"$ip\", port = 8995 }"
                        else
                            master_addrs_content="$master_addrs_content,\n    { hostname = \"$ip\", port = 8995 }"
                        fi
                    done
                    
                    # 构建journal_addrs (journal_port=8996)
                    journal_addrs_content=""
                    id=1
                    for ip in $master_ips; do
                        if [ -z "$journal_addrs_content" ]; then
                            journal_addrs_content="    {id = $id, hostname = \"$ip\", port = 8996}"
                        else
                            journal_addrs_content="$journal_addrs_content,\n    {id = $id, hostname = \"$ip\", port = 8996}"
                        fi
                        id=$((id + 1))
                    done
                    
                    # 创建更新脚本（避免sed替换问题，直接用echo写入）
                    cat > /tmp/update_addrs_${host}.py << 'PYEOF'
#!/usr/bin/env python3
import re
import sys

try:
    with open('/root/dist/conf/curvine-cluster.toml', 'r') as f:
        content = f.read()

    # 更新master_addrs
    master_addrs_new = """master_addrs = [
__MASTER_ADDRS__
]"""

    pattern = r'master_addrs\s*=\s*\[.*?\]'
    content = re.sub(pattern, master_addrs_new, content, flags=re.DOTALL)

    # 更新journal_addrs  
    journal_addrs_new = """journal_addrs = [
__JOURNAL_ADDRS__
]"""

    pattern = r'journal_addrs\s*=\s*\[.*?\]'
    content = re.sub(pattern, journal_addrs_new, content, flags=re.DOTALL)

    with open('/root/dist/conf/curvine-cluster.toml', 'w') as f:
        f.write(content)

    print("master_addrs和journal_addrs更新完成")
except Exception as e:
    print(f"更新失败: {e}")
    sys.exit(1)
PYEOF
                    
                    # 使用echo替换占位符（避免sed的特殊字符问题）
                    python3 << PYREPLACE
with open('/tmp/update_addrs_${host}.py', 'r') as f:
    content = f.read()
content = content.replace('__MASTER_ADDRS__', '''${master_addrs_content}''')
content = content.replace('__JOURNAL_ADDRS__', '''${journal_addrs_content}''')
with open('/tmp/update_addrs_${host}.py', 'w') as f:
    f.write(content)
PYREPLACE
                    
                    # 拷贝并执行（添加错误处理）
                    if scp -q -o StrictHostKeyChecking=no /tmp/update_addrs_${host}.py root@${host}:/tmp/update_addrs.py 2>/dev/null || \
                       ansible "$host" -m copy -a "src=/tmp/update_addrs_${host}.py dest=/tmp/update_addrs.py mode=0755" > /dev/null 2>&1; then
                        
                        # 执行更新脚本
                        if ansible "$host" -m shell -a "python3 /tmp/update_addrs.py" 2>&1 | grep -v "SUCCESS" | grep -v "changed" | tee -a "$LOG_FILE"; then
                            log "  ✓ 节点 $host 的master_addrs和journal_addrs已更新"
                        else
                            log "  ⚠️  节点 $host 更新失败，但继续处理其他节点"
                        fi
                    else
                        log "  ⚠️  节点 $host 脚本拷贝失败，跳过"
                    fi
                    
                    # 清理临时文件
                    rm -f /tmp/update_addrs_${host}.py 2>/dev/null || true
                    ansible "$host" -m file -a "path=/tmp/update_addrs.py state=absent" > /dev/null 2>&1 || true
                else
                    log "  ✗ 节点 $host 配置文件不存在，跳过"
                fi
            done
        fi
        
        log ""
        log "========================================"
        log "检查并修复Worker节点目录结构..."
        log "========================================"
        
        # 检查所有Worker节点的目录结构
        if ansible worker --list-hosts > /dev/null 2>&1; then
            worker_hosts=$(ansible worker --list-hosts 2>/dev/null | grep '^ ' | sed 's/^ *//' | grep -v "hosts")
            
            for whost in $worker_hosts; do
                # 检查是否有dist1目录
                has_dist1=$(ansible "$whost" -m shell -a "[ -d /root/dist/dist1 ] && echo 'yes' || echo 'no'" 2>/dev/null | \
                    grep -v "SUCCESS" | grep -v "changed" | grep -v "rc=" | xargs)
                
                log "检查Worker节点 $whost..."
                
                if [ "$has_dist1" = "yes" ]; then
                    log "  发现 /root/dist/dist1 目录，正在移动..."
                    
                    # 移动dist1下的所有内容到dist
                    ansible "$whost" -m shell -a "
cd /root/dist
if [ -d dist1 ]; then
    # 移动所有文件
    mv dist1/* . 2>/dev/null || true
    mv dist1/.* . 2>/dev/null || true
    # 删除空的dist1目录
    rmdir dist1 2>/dev/null || rm -rf dist1
    echo '目录结构已修复'
else
    echo '不需要修复'
fi
" 2>&1 | grep -v "SUCCESS" | grep -v "changed" | tee -a "$LOG_FILE"
                    
                    log "  ✓ Worker节点 $whost 目录结构已修复"
                else
                    log "  ✓ Worker节点 $whost 目录结构正确"
                fi
                
                # 验证目录结构
                log "  验证目录结构..."
                ansible "$whost" -m shell -a "ls -la /root/dist/ | head -15" 2>&1 | \
                    grep -v "SUCCESS" | grep -v "changed" | tee -a "$LOG_FILE"
            done
        fi
        
        log ""
        log "========================================"
        log "同步配置文件与实际挂载点..."
        log "========================================"
        
        # 检查所有节点的实际挂载点并同步配置
        if [ -n "$success_hosts" ]; then
            IFS=',' read -ra ALL_HOSTS <<< "$success_hosts"
            for host in "${ALL_HOSTS[@]}"; do
                # 获取该节点实际挂载的/datads*目录
                datads_mounts=$(ansible "$host" -m shell -a "df -h | grep '/datads' | awk '{print \$NF}' | sort" 2>/dev/null | \
                    grep -v "SUCCESS" | grep -v "changed" | grep -v "rc=" | grep "^/datads" | xargs | tr ' ' ',')
                
                if [ -n "$datads_mounts" ]; then
                    # 判断节点类型
                    is_master_node="worker"
                    if ansible master --list-hosts 2>/dev/null | grep -q "$host"; then
                        is_master_node="master"
                    fi
                    
                    log "节点 $host ($is_master_node):"
                    log "  实际挂载的datads: $datads_mounts"
                    
                    # 拷贝同步脚本
                    scp -q -o StrictHostKeyChecking=no sync_datads_config.py root@${host}:/tmp/ 2>/dev/null || \
                    ansible "$host" -m copy -a "src=sync_datads_config.py dest=/tmp/sync_datads_config.py mode=0755" > /dev/null 2>&1
                    
                    # 执行同步
                    ansible "$host" -m shell -a "cd /root/dist/conf && python3 /tmp/sync_datads_config.py curvine-cluster.toml $is_master_node $datads_mounts" 2>&1 | \
                        grep -v "SUCCESS" | grep -v "changed" | tee -a "$LOG_FILE" || log "  同步失败但继续"
                    
                    # 清理
                    ansible "$host" -m file -a "path=/tmp/sync_datads_config.py state=absent" > /dev/null 2>&1 || true
                    
                    log "  ✓ 配置已同步"
                else
                    log "节点 $host: 没有/datads挂载点，跳过同步"
                fi
                log ""
            done
        fi
        
        log ""
        log "启动服务..."
        if [ -n "$success_hosts" ]; then
            log_cmd ansible-playbook start_services.yml --limit "$success_hosts" || true
        else
            log_cmd ansible-playbook start_services.yml || true
        fi
        
        log ""
        log "查看服务状态..."
        sleep 3
        if [ -n "$success_hosts" ]; then
            log_cmd ansible-playbook status_services.yml --limit "$success_hosts" || true
        else
            log_cmd ansible-playbook status_services.yml || true
        fi
    fi
fi

# 最终汇总
log ""
log "========================================"
log "部署完成！"
log "========================================"
log ""

# 显示成功和失败的节点
if [ $total_fail -gt 0 ]; then
    log "⚠️  部署汇总："
    log ""
    log "成功部署的节点 ($total_ok 个):"
    echo "$success_hosts" | tr ',' '\n' | sed 's/^/  ✓ /' | tee -a "$LOG_FILE"
    log ""
    log "连接失败跳过的节点 ($total_fail 个):"
    cat "$failed_hosts" | sed 's/^/  ✗ /' | tee -a "$LOG_FILE"
    log ""
    log "提示: 请检查失败节点的网络和SSH配置"
else
    log "✓ 所有节点部署成功"
fi

log ""
log "常用命令："
log "  启动服务: ansible-playbook start_services.yml"
log "  停止服务: ansible-playbook stop_services.yml"
log "  重启服务: ansible-playbook restart_services.yml"
log "  查看状态: ansible-playbook status_services.yml"
log ""
log "Web界面访问："
log "  http://<master-ip>:9000"
log ""
log "部署日志已保存到: $LOG_FILE"
log "节点信息已保存到: node_info_summary.txt"
log ""

# 清理临时文件
rm -f "$failed_hosts"

