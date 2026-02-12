#!/bin/bash
# One-click deployment script for Curvine cluster

set -e

# Set log file
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="${SCRIPT_DIR}/deploy.log"

# Log functions
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

log_cmd() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Executing: $*" >> "$LOG_FILE"
    "$@" 2>&1 | tee -a "$LOG_FILE"
    return ${PIPESTATUS[0]}
}

log "========================================"
log "Curvine Cluster One-Click Deployment Tool"
log "Log file: $LOG_FILE"
log "========================================"
log ""

# Check necessary files
log "Checking necessary files..."
if [ ! -f "hosts.ini" ]; then
    log "Error: hosts.ini file not found"
    exit 1
fi

if [ ! -f "/root/dist.tar.gz" ]; then
    log "Warning: /root/dist.tar.gz file not found"
    read -p "Continue? (y/n): " continue
    if [ "$continue" != "y" ]; then
        exit 1
    fi
fi

if [ ! -f "/root/dist1.tar.gz" ]; then
    log "Warning: /root/dist1.tar.gz file not found"
    read -p "Continue? (y/n): " continue
    if [ "$continue" != "y" ]; then
        exit 1
    fi
fi

# Test connection
log ""
log "Quick node connection detection (3 second timeout)..."
log ""

# Create temporary file to store results
failed_hosts=$(mktemp)
success_hosts=""
master_ok_count=0
master_fail_count=0
worker_ok_count=0
worker_fail_count=0

# Check Master nodes
log "Checking Master nodes..."
if ansible master --list-hosts > /dev/null 2>&1; then
    while IFS= read -r host; do
        host=$(echo "$host" | xargs)  # Remove spaces
        # Skip "hosts (X):" lines
        if [[ "$host" =~ ^hosts ]]; then
            continue
        fi
        if ansible "$host" -m ping -o > /dev/null 2>&1; then
            master_ok_count=$((master_ok_count + 1))
            success_hosts="$success_hosts,$host"
            echo "  ✓ $host"
        else
            master_fail_count=$((master_fail_count + 1))
            echo "  ✗ $host (Connection timeout, will be skipped)"
            echo "$host" >> "$failed_hosts"
        fi
    done < <(ansible master --list-hosts 2>/dev/null | grep '^ ' | sed 's/^ *//')
fi

echo ""
echo "Checking Worker nodes..."
if ansible worker --list-hosts > /dev/null 2>&1; then
    while IFS= read -r host; do
        host=$(echo "$host" | xargs)  # Remove spaces
        # Skip "hosts (X):" lines
        if [[ "$host" =~ ^hosts ]]; then
            continue
        fi
        if ansible "$host" -m ping -o > /dev/null 2>&1; then
            worker_ok_count=$((worker_ok_count + 1))
            success_hosts="$success_hosts,$host"
            echo "  ✓ $host"
        else
            worker_fail_count=$((worker_fail_count + 1))
            echo "  ✗ $host (Connection timeout, will be skipped)"
            echo "$host" >> "$failed_hosts"
        fi
    done < <(ansible worker --list-hosts 2>/dev/null | grep '^ ' | sed 's/^ *//')
fi

echo ""
echo "Connection detection completed:"
echo "  Master nodes: $master_ok_count succeeded, $master_fail_count failed"
echo "  Worker nodes: $worker_ok_count succeeded, $worker_fail_count failed"

# Remove leading comma
success_hosts=$(echo "$success_hosts" | sed 's/^,//')

# Calculate totals
total_ok=$((master_ok_count + worker_ok_count))
total_fail=$((master_fail_count + worker_fail_count))

# If there are failed nodes
if [ $total_fail -gt 0 ]; then
    echo ""
    echo "⚠️  Following nodes failed to connect and will be skipped:"
    cat "$failed_hosts" 2>/dev/null || true
fi

echo ""

# Check if there are available nodes
if [ $total_ok -eq 0 ]; then
    echo "✗ All nodes are unreachable!"
    echo ""
    echo "Please check:"
    echo "1. Is hosts.ini file configured correctly"
    echo "2. Are nodes reachable on the network"
    echo "3. Is SSH service running"
    echo "4. Is SSH passwordless login configured"
    echo ""
    rm -f "$failed_hosts"
    exit 1
fi

master_ok=true
worker_ok=true
[ $master_ok_count -eq 0 ] && master_ok=false
[ $worker_ok_count -eq 0 ] && worker_ok=false

# Handle failed nodes
if [ $total_fail -gt 0 ]; then
    echo "⚠️  $total_fail nodes failed to connect"
    echo ""
    read -p "Configure SSH passwordless login for failed nodes? (y/n, or press s to skip failed nodes and continue): " setup_choice
    
    if [ "$setup_choice" == "y" ]; then
        echo ""
        echo "Starting SSH passwordless login configuration..."
        echo "========================================"
        
        # Execute SSH configuration, continue even if some nodes fail
        ansible-playbook setup_ssh.yml --ask-pass || true
        
        echo ""
        echo "SSH configuration completed, re-detecting all nodes..."
        
        # Re-detect - re-count all nodes
        rm -f "$failed_hosts"
        failed_hosts=$(mktemp)
        success_hosts=""
        master_ok_count=0
        master_fail_count=0
        worker_ok_count=0
        worker_fail_count=0
        
        # Re-detect Master nodes
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
        
        # Re-detect Worker nodes
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
        echo "Re-detection completed:"
        echo "  Master nodes: $master_ok_count succeeded, $master_fail_count failed"
        echo "  Worker nodes: $worker_ok_count succeeded, $worker_fail_count failed"
        
        if [ $total_ok -eq 0 ]; then
            echo ""
            echo "✗ SSH configuration failed for all nodes"
            rm -f "$failed_hosts"
            exit 1
        fi
        
        if [ $total_fail -gt 0 ]; then
            echo ""
            echo "⚠️  Still $total_fail nodes unreachable, will skip these nodes and continue deployment"
            cat "$failed_hosts"
        else
            echo ""
            echo "✓ All nodes connected successfully"
        fi
    elif [ "$setup_choice" != "s" ]; then
        echo "Deployment cancelled"
        rm -f "$failed_hosts"
        exit 0
    fi
else
    log "✓ All nodes connected successfully"
fi

# Collect node information
log ""
log "========================================"
log "Collecting node information..."
log "========================================"

NODE_INFO_FILE="${SCRIPT_DIR}/node_info_summary.txt"

# Delete old node information file
rm -f "$NODE_INFO_FILE"

# Add file header
cat > "$NODE_INFO_FILE" << EOF
================================================================================
Curvine Cluster Node Information Summary
Collection time: $(date '+%Y-%m-%d %H:%M:%S')
================================================================================

EOF

# Collect node information one by one
if [ -n "$success_hosts" ]; then
    IFS=',' read -ra HOSTS <<< "$success_hosts"
    for host in "${HOSTS[@]}"; do
        log "Collecting information for node $host..."
        
        # Determine node type
        node_type="Unknown"
        if ansible master --list-hosts 2>/dev/null | grep -q "$host"; then
            node_type="Master"
        elif ansible worker --list-hosts 2>/dev/null | grep -q "$host"; then
            node_type="Worker"
        fi
        
        # Write node information to file
        {
            echo ""
            echo "================================================================================"
            echo "Node: $host ($node_type)"
            echo "================================================================================"
            echo ""
            echo "Network Information:"
        } >> "$NODE_INFO_FILE"
        
        # Get network information
        net_info=$(ansible "$host" -m shell -a "ip addr show | grep -E '^[0-9]+: (eth|en|bond)' -A 5 | awk '/^[0-9]+:/{iface=\$2; gsub(/:/, \"\", iface)} /inet /{print iface\":\"\$2}' | sed 's|/[0-9]*||'" 2>/dev/null | grep -v "^$host" | grep -v "SUCCESS" | grep ":" | sed 's/^/  /')
        if [ -n "$net_info" ]; then
            echo "$net_info" >> "$NODE_INFO_FILE"
        else
            echo "  No network information" >> "$NODE_INFO_FILE"
        fi
        
        # Get lsblk information
        {
            echo ""
            echo "Disk Information (lsblk):"
        } >> "$NODE_INFO_FILE"
        lsblk_info=$(ansible "$host" -m shell -a "lsblk -o NAME,SIZE,TYPE,MOUNTPOINT" 2>/dev/null | grep -v "^$host" | grep -v "SUCCESS" | grep -v "^$")
        if [ -n "$lsblk_info" ]; then
            echo "$lsblk_info" | sed 's/^/  /' >> "$NODE_INFO_FILE"
        else
            echo "  Unable to get lsblk information" >> "$NODE_INFO_FILE"
        fi
        
        # Get df information (with header)
        {
            echo ""
            echo "Disk Usage (df -h):"
        } >> "$NODE_INFO_FILE"
        df_info=$(ansible "$host" -m shell -a "df -h | head -1; df -h | grep '^/dev'" 2>/dev/null | grep -v "^$host" | grep -v "SUCCESS" | grep -v "^$")
        if [ -n "$df_info" ]; then
            echo "$df_info" | sed 's/^/  /' >> "$NODE_INFO_FILE"
        else
            echo "  Unable to get disk usage information" >> "$NODE_INFO_FILE"
        fi
        
        echo "" >> "$NODE_INFO_FILE"
    done
fi

log ""
log "========================================"
log "Node information collection completed"
log "Detailed information saved at: $NODE_INFO_FILE"
log "========================================"

# Display node information summary and confirm
log ""
log "Detailed information for nodes to be deployed:"
log ""

# Display contents of node_info_summary.txt
if [ -f "$NODE_INFO_FILE" ]; then
    cat "$NODE_INFO_FILE" | tee -a "$LOG_FILE"
else
    log "Warning: Node information file not found: $NODE_INFO_FILE"
    log "Continuing deployment may be risky, please manually confirm node configuration"
    log ""
    log "Manual display of node list:"
    if [ -n "$success_hosts" ]; then
        echo "$success_hosts" | tr ',' '\n' | sed 's/^/  /' | tee -a "$LOG_FILE"
    fi
fi

log ""
log "========================================"
read -p "Please carefully check the above node information, enter Y to continue deployment, enter N to cancel: " confirm_info
log "User input: $confirm_info"

if [ "$confirm_info" != "Y" ] && [ "$confirm_info" != "y" ]; then
    log "Deployment cancelled by user"
    rm -f "$failed_hosts"
    exit 0
fi

# ========== Disk formatting and mounting process ==========
log ""
log "========================================"
log "Detecting unformatted disks..."
log "========================================"

UNFORMAT_DISK_FILE="${SCRIPT_DIR}/unformatted_disks_info.txt"
rm -f "$UNFORMAT_DISK_FILE"

# Detect unformatted disks for each node
declare -A node_unformatted_disks
declare -A node_mountpoints
has_unformatted=false

if [ -n "$success_hosts" ]; then
    IFS=',' read -ra HOSTS <<< "$success_hosts"
    for host in "${HOSTS[@]}"; do
        log "Detecting unformatted disks for node $host..."
        
        # Create detection script
        cat > /tmp/detect_unformatted_${host}.sh << 'DETECT_SCRIPT_EOF'
#!/bin/bash
# Get all disk-type devices with empty MOUNTPOINT
empty_disks=$(lsblk -nlo NAME,TYPE,MOUNTPOINT | grep 'disk' | awk '$3=="" {print $1}')

for disk in $empty_disks; do
    # Use lsblk to list the disk and all its child devices (correctly handle RAID, LVM, etc.)
    # -s parameter: show dependencies (child devices)
    all_children=$(lsblk -no NAME "/dev/${disk}" 2>/dev/null | wc -l)
    # Subtract 1 for the disk itself
    has_children=$((all_children - 1))
    
    # Check if the disk itself is in df
    in_df=$(df -h | grep "/dev/${disk}" | wc -l)
    
    # Check if any child devices of this disk have mount points
    children_mounted=$(lsblk -no MOUNTPOINT "/dev/${disk}" 2>/dev/null | grep -v '^$' | wc -l)
    # If the disk itself has a mount point, subtract 1
    if lsblk -no MOUNTPOINT "/dev/${disk}" 2>/dev/null | head -1 | grep -q '^$'; then
        # Disk itself has no mount point, no need to subtract
        :
    else
        # Disk itself has a mount point, subtract 1
        children_mounted=$((children_mounted > 0 ? children_mounted - 1 : 0))
    fi
    
    # Output debug info to stderr (does not affect results)
    # echo "DEBUG: $disk has_children=$has_children in_df=$in_df children_mounted=$children_mounted" >&2
    
    # If no child devices, not in df, and no mount points (including child devices), then it's unformatted
    if [ "$has_children" -eq 0 ] && [ "$in_df" -eq 0 ]; then
        # Double-check that there are no mount points in this disk tree
        total_mounts=$(lsblk -no MOUNTPOINT "/dev/${disk}" 2>/dev/null | grep -v '^$' | wc -l)
        if [ "$total_mounts" -eq 0 ]; then
            echo "$disk"
        fi
    fi
done
DETECT_SCRIPT_EOF

        # Copy script to remote node
        scp -q -o StrictHostKeyChecking=no -o ConnectTimeout=3 /tmp/detect_unformatted_${host}.sh root@${host}:/tmp/detect_unformatted.sh 2>/dev/null || \
        ansible "$host" -m copy -a "src=/tmp/detect_unformatted_${host}.sh dest=/tmp/detect_unformatted.sh mode=0755" > /dev/null 2>&1
        
        # Execute detection script
        unformatted=$(ansible "$host" -m shell -a "bash /tmp/detect_unformatted.sh" 2>/dev/null | \
                      grep -v "^$host" | grep -v "SUCCESS" | grep -v "changed" | grep -v "rc=0" | \
                      grep -E '^(sd[a-z]+|vd[a-z]+|nvme[0-9]+n[0-9]+|xvd[a-z]+)$' | xargs)
        
        # Clean up temporary files
        rm -f /tmp/detect_unformatted_${host}.sh
        ansible "$host" -m file -a "path=/tmp/detect_unformatted.sh state=absent" > /dev/null 2>&1
        
        if [ -n "$unformatted" ]; then
            log "  Node $host found unformatted disks: $unformatted"
            node_unformatted_disks["$host"]="$unformatted"
            echo "$host:$unformatted" >> "$UNFORMAT_DISK_FILE"
            has_unformatted=true
        else
            log "  Node $host has no unformatted disks"
        fi
    done
fi

log ""

# If there are unformatted disks, display and ask whether to format
if [ "$has_unformatted" = true ]; then
    log "========================================"
    log "Unformatted disks found:"
    log "========================================"
    log ""
    
    for host in "${!node_unformatted_disks[@]}"; do
        disks="${node_unformatted_disks[$host]}"
        log "Node $host:"
        
        # Get the maximum existing datads sequence number for this node
        max_num=$(ansible "$host" -m shell -a "df -h | grep '/datads' | sed 's|.*/datads||' | awk '{print \$1}' | sort -n | tail -1" 2>/dev/null | \
                  grep -v "^$host" | grep -v "SUCCESS" | grep -E '^[0-9]+$' | head -1)
        
        if [ -z "$max_num" ]; then
            disk_num=1
        else
            disk_num=$((max_num + 1))
        fi
        
        log "  (Current max number: ${max_num:-0}, new disks start from /datads${disk_num})"
        
        for disk in $disks; do
            log "  - /dev/$disk -> Will be mounted to /datads${disk_num}"
            disk_num=$((disk_num + 1))
        done
        log ""
    done
    
    log "========================================"
    read -p "Format and mount these disks? (Y/n, enter n to skip): " format_confirm
    log "User input: $format_confirm"
    
    if [ "$format_confirm" = "Y" ] || [ "$format_confirm" = "y" ]; then
        log ""
        log "========================================"
        log "Starting disk formatting and mounting..."
        log "========================================"
        
        # Format and mount for each node, and record mount points
        for host in "${!node_unformatted_disks[@]}"; do
            disks="${node_unformatted_disks[$host]}"
            log "Processing node $host..."
            
            # Get the maximum existing datads sequence number for this node
            max_num=$(ansible "$host" -m shell -a "df -h | grep '/datads' | sed 's|.*/datads||' | awk '{print \$1}' | sort -n | tail -1" 2>/dev/null | \
                      grep -v "^$host" | grep -v "SUCCESS" | grep -E '^[0-9]+$' | head -1)
            
            if [ -z "$max_num" ]; then
                disk_num=1
            else
                disk_num=$((max_num + 1))
            fi
            
            log "  Current max number: ${max_num:-0}, new disks start from /datads${disk_num}"
            
            host_mounts=""
            for disk in $disks; do
                mountpoint="/datads${disk_num}"
                
                log "  Formatting /dev/$disk..."
                ansible "$host" -m shell -a "mkfs.ext4 -F /dev/$disk" 2>&1 | tee -a "$LOG_FILE"
                
                log "  Creating mount point $mountpoint..."
                ansible "$host" -m shell -a "mkdir -p $mountpoint && chmod 755 $mountpoint" 2>&1 | tee -a "$LOG_FILE"
                
                log "  Mounting /dev/$disk to $mountpoint..."
                ansible "$host" -m shell -a "mount /dev/$disk $mountpoint" 2>&1 | tee -a "$LOG_FILE"
                
                log "  Adding to /etc/fstab..."
                ansible "$host" -m shell -a "echo '/dev/$disk $mountpoint ext4 defaults 0 0' >> /etc/fstab" 2>&1 | tee -a "$LOG_FILE"
                
                log "  ✓ /dev/$disk formatted and mounted to $mountpoint completed"
                
                # Record mount point
                if [ -z "$host_mounts" ]; then
                    host_mounts="$mountpoint"
                else
                    host_mounts="$host_mounts,$mountpoint"
                fi
                
                disk_num=$((disk_num + 1))
            done
            
            # Save all mount points for this node
            node_mountpoints["$host"]="$host_mounts"
            log ""
        done
        
        log "========================================"
        log "Formatting and mounting completed, showing updated disk information:"
        log "========================================"
        log ""
        
        # Display disk information after formatting
        for host in "${!node_unformatted_disks[@]}"; do
            # Determine node type
            node_type="Unknown"
            if ansible master --list-hosts 2>/dev/null | grep -q "$host"; then
                node_type="Master"
            elif ansible worker --list-hosts 2>/dev/null | grep -q "$host"; then
                node_type="Worker"
            fi
            
            log "================================================================================"
            log "Node: $host ($node_type) - Updated disk information"
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
        read -p "Please confirm disk formatting and mounting results, enter Y to continue deployment, enter N to cancel: " format_result_confirm
        log "User input: $format_result_confirm"
        
        if [ "$format_result_confirm" != "Y" ] && [ "$format_result_confirm" != "y" ]; then
            log "Deployment cancelled by user"
            rm -f "$failed_hosts"
            exit 0
        fi
    else
        log "Skipping disk formatting"
    fi
else
    log "All nodes have no unformatted disks, skipping formatting step"
fi

# Confirm deployment
log ""
if [ -n "$success_hosts" ]; then
    log "The following nodes will be deployed:"
    echo "$success_hosts" | tr ',' '\n' | sed 's/^/  /' | tee -a "$LOG_FILE"
else
    log "No available nodes for deployment"
    rm -f "$failed_hosts"
    exit 1
fi

log ""
log "The following operations will be performed:"
log "1. Configure environment variables"
log "2. Distribute installation packages"
log "3. Configure data_dir"
log "4. Create systemd services"
log ""
read -p "Confirm to start deployment? (y/n): " confirm
log "User input: $confirm"
if [ "$confirm" != "y" ]; then
    log "Deployment cancelled"
    rm -f "$failed_hosts"
    exit 0
fi

# Execute deployment (only deploy successfully connected nodes)
log ""
log "========================================" 
log "Starting deployment..."
log "========================================"
if [ -n "$success_hosts" ]; then
    log_cmd ansible-playbook deploy_curvine.yml --limit "$success_hosts" || true
    deploy_result=$?
else
    log_cmd ansible-playbook deploy_curvine.yml || true
    deploy_result=$?
fi

# Update configuration file (if there are newly formatted disks)
if [ ${#node_mountpoints[@]} -gt 0 ]; then
    log ""
    log "========================================"
    log "Updating configuration file to use newly mounted disks..."
    log "========================================"
    log ""
    
    for host in "${!node_mountpoints[@]}"; do
        mountpoints="${node_mountpoints[$host]}"
        IFS=',' read -ra MOUNTS <<< "$mountpoints"
        first_mount="${MOUNTS[0]}"
        
        # Determine node type
        is_master=false
        if ansible master --list-hosts 2>/dev/null | grep -q "$host"; then
            is_master=true
        fi
        
        log "Updating configuration for node $host..."
        
        if [ "$is_master" = true ]; then
            # Master node: update meta_dir, journal_dir, data_dir
            log "  Node type: Master"
            log "  Using Python script to update configuration (more reliable)"
            
            # Show current data_dir content in configuration file first
            log "  [DEBUG] Before execution, checking current data_dir configuration on target node..."
            current_data_dir=$(ansible "$host" -m shell -a "grep '^data_dir' /root/dist/conf/curvine-cluster.toml" 2>/dev/null | \
                grep -v "SUCCESS" | grep -v "changed" | grep -v "rc=")
            log "  [DEBUG] Current data_dir: $current_data_dir"
            
            # Copy Python update script to remote node
            scp -q -o StrictHostKeyChecking=no update_toml_config.py root@${host}:/tmp/ 2>/dev/null || \
            ansible "$host" -m copy -a "src=update_toml_config.py dest=/tmp/update_toml_config.py mode=0755" > /dev/null 2>&1
            
            # Execute Python script to update configuration
            log "  Updating configuration: meta_dir and journal_dir use ${first_mount}, data_dir appends all mount points"
            ansible "$host" -m shell -a "cd /root/dist/conf && python3 /tmp/update_toml_config.py master curvine-cluster.toml ${first_mount} ${mountpoints}" 2>&1 | \
                grep -v "SUCCESS" | grep -v "changed" | tee -a "$LOG_FILE"
            
            # Show updated data_dir
            log "  [DEBUG] After execution, checking updated data_dir configuration on target node..."
            updated_data_dir=$(ansible "$host" -m shell -a "grep '^data_dir' /root/dist/conf/curvine-cluster.toml" 2>/dev/null | \
                grep -v "SUCCESS" | grep -v "changed" | grep -v "rc=")
            log "  [DEBUG] Updated data_dir: $updated_data_dir"
            
            # Clean up
            ansible "$host" -m file -a "path=/tmp/update_toml_config.py state=absent" > /dev/null 2>&1
            
        else
            # Worker node: only update data_dir
            log "  Node type: Worker"
            log "  Using Python script to update configuration (more reliable)"
            
            # Copy Python update script to remote node
            scp -q -o StrictHostKeyChecking=no update_toml_config.py root@${host}:/tmp/ 2>/dev/null || \
            ansible "$host" -m copy -a "src=update_toml_config.py dest=/tmp/update_toml_config.py mode=0755" > /dev/null 2>&1
            
            # Execute Python script to update configuration
            log "  Updating configuration: data_dir appends all mount points"
            ansible "$host" -m shell -a "python3 /tmp/update_toml_config.py worker /root/dist/conf/curvine-cluster.toml ${mountpoints}" 2>&1 | \
                grep -v "SUCCESS" | grep -v "changed" | tee -a "$LOG_FILE"
            
            # Clean up
            ansible "$host" -m file -a "path=/tmp/update_toml_config.py state=absent" > /dev/null 2>&1
        fi
        
        log ""
    done
    
    # ==========Following old code is deprecated, now using Python script to update configuration==========
    : <<'DEPRECATED_CODE'
            # Following code is no longer used
            cat > /tmp/update_config_${host}.sh << 'UPDATE_SCRIPT_EOF'
#!/bin/bash
cd /root/dist/conf
cp curvine-cluster.toml curvine-cluster.toml.bak

# 1. First handle meta_dir and journal_dir (only update when it's /data/* and doesn't contain datads)
first_mount="__FIRST_MOUNT__"

current_meta=$(grep '^meta_dir' curvine-cluster.toml)
if echo "$current_meta" | grep -q 'meta_dir = "/data/' && ! echo "$current_meta" | grep -q '/datads'; then
    sed -i "s|meta_dir = \"/data/|meta_dir = \"${first_mount}/|g" curvine-cluster.toml
    echo "meta_dir updated to: ${first_mount}/meta"
else
    echo "meta_dir unchanged (already /datads* or not /data/*)"
fi

current_journal=$(grep '^journal_dir' curvine-cluster.toml)
if echo "$current_journal" | grep -q 'journal_dir = "/data/' && ! echo "$current_journal" | grep -q '/datads'; then
    sed -i "s|journal_dir = \"/data/|journal_dir = \"${first_mount}/|g" curvine-cluster.toml
    echo "journal_dir updated to: ${first_mount}/journal"
else
    echo "journal_dir unchanged (already /datads* or not /data/*)"
fi

# 2. Read existing data_dir (supports single-line and multi-line format, supports [SSD] format)
echo "======== DEBUG: Starting to read data_dir ========"

# Display original lines related to data_dir
echo "--- data_dir original content ---"
grep -A 5 '^data_dir' curvine-cluster.toml || echo "data_dir not found"

# Method: Extract data_dir line content
data_dir_content=$(grep '^data_dir' curvine-cluster.toml)
echo "data_dir line content: $data_dir_content"

# Extract all paths (changed to match all quoted content, not restricted to starting with /)
existing_data_dirs=$(echo "$data_dir_content" | grep -o '"[^"]*"' | tr -d '"')

echo "Single-line mode extraction result:"
if [ -n "$existing_data_dirs" ]; then
    echo "$existing_data_dirs"
else
    echo "(empty)"
fi

# If single-line didn't extract anything, try multi-line mode
if [ -z "$existing_data_dirs" ]; then
    echo "Single-line mode didn't extract anything, trying multi-line mode..."
    data_dir_line=$(grep -n '^data_dir' curvine-cluster.toml | head -1 | cut -d: -f1)
    if [ -n "$data_dir_line" ]; then
        existing_data_dirs=$(sed -n "${data_dir_line},/\]/p" curvine-cluster.toml | \
            grep -o '"[^"]*"' | \
            tr -d '"')
        echo "Multi-line mode extraction result:"
        echo "$existing_data_dirs"
    fi
fi

echo "======== DEBUG: Existing data_dir paths ========"
if [ -n "$existing_data_dirs" ]; then
    echo "$existing_data_dirs"
else
    echo "(empty)"
fi
echo "========================================"

# New mount point list
new_mounts="__NEW_MOUNTS__"
echo "New mount points: $new_mounts"

# Merge existing and new paths
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
            
            # Replace variables in script
            sed -i "s|__FIRST_MOUNT__|${first_mount}|g" /tmp/update_config_${host}.sh
            sed -i "s|__NEW_MOUNTS__|${mountpoints}|g" /tmp/update_config_${host}.sh
            
            log "  meta_dir and journal_dir: only update when current is /data/* (not /datads*)"
            log "  data_dir: append new mount points ${mountpoints}, keep existing paths"
            
        else
            # Worker node: only update data_dir (append)
            log "  Node type: Worker"
            
            # First show current data_dir content in configuration file
            log "  [DEBUG] Before execution, checking current data_dir configuration on target node..."
            current_data_dir=$(ansible "$host" -m shell -a "grep '^data_dir' /root/dist/conf/curvine-cluster.toml" 2>/dev/null | \
                grep -v "SUCCESS" | grep -v "changed" | grep -v "rc=")
            log "  [DEBUG] Current data_dir: $current_data_dir"
            
            # Copy Python update script to remote node
            scp -q -o StrictHostKeyChecking=no update_toml_config.py root@${host}:/tmp/ 2>/dev/null || \
            ansible "$host" -m copy -a "src=update_toml_config.py dest=/tmp/update_toml_config.py mode=0755" > /dev/null 2>&1
            
            # Execute Python script to update configuration
            log "  Updating configuration: data_dir appends all mount points"
            ansible "$host" -m shell -a "cd /root/dist/conf && python3 /tmp/update_toml_config.py worker curvine-cluster.toml ${mountpoints}" 2>&1 | \
                grep -v "SUCCESS" | grep -v "changed" | tee -a "$LOG_FILE"
            
            # Show updated data_dir
            log "  [DEBUG] After execution, checking updated data_dir configuration on target node..."
            updated_data_dir=$(ansible "$host" -m shell -a "grep '^data_dir' /root/dist/conf/curvine-cluster.toml" 2>/dev/null | \
                grep -v "SUCCESS" | grep -v "changed" | grep -v "rc=")
            log "  [DEBUG] Updated data_dir: $updated_data_dir"
            
            # Clean up
            ansible "$host" -m file -a "path=/tmp/update_toml_config.py state=absent" > /dev/null 2>&1
        fi
        
        log ""
    done
    
    # ==========Following old code is deprecated, now using Python script to update configuration==========
    : <<'DEPRECATED_CODE'
            # Following code is no longer used
            cat > /tmp/update_config_${host}.sh << 'UPDATE_SCRIPT_EOF'
#!/bin/bash
cd /root/dist/conf
cp curvine-cluster.toml curvine-cluster.toml.bak

# Read existing data_dir (supports single-line and multi-line format)
echo "======== DEBUG: Starting to read data_dir ========"

# Display original lines related to data_dir
echo "--- data_dir original content ---"
grep -A 5 '^data_dir' curvine-cluster.toml || echo "data_dir not found"

# Method: Extract data_dir line content
data_dir_content=$(grep '^data_dir' curvine-cluster.toml)
echo "data_dir line content: $data_dir_content"

# Extract all paths
existing_data_dirs=$(echo "$data_dir_content" | grep -o '"/[^"]*"' | tr -d '"')

# If single-line didn't extract anything, try multi-line mode
if [ -z "$existing_data_dirs" ]; then
    echo "Single-line mode didn't extract anything, trying multi-line mode..."
    data_dir_line=$(grep -n '^data_dir' curvine-cluster.toml | head -1 | cut -d: -f1)
    if [ -n "$data_dir_line" ]; then
        existing_data_dirs=$(sed -n "${data_dir_line},/\]/p" curvine-cluster.toml | \
            grep -o '"/[^"]*"' | \
            tr -d '"')
    fi
fi

echo "======== DEBUG: Existing data_dir paths ========"
if [ -n "$existing_data_dirs" ]; then
    echo "$existing_data_dirs"
else
    echo "(empty)"
fi
echo "========================================"

# New mount point list
new_mounts="__NEW_MOUNTS__"
echo "New mount points: $new_mounts"

# Merge existing and new paths
all_data_dirs=""
for dir in $existing_data_dirs; do
    if [ -z "$all_data_dirs" ]; then
        all_data_dirs="\"$dir\""
    else
        all_data_dirs="$all_data_dirs, \"$dir\""
    fi
done

# Append new mount points
IFS=',' read -ra NEW_MOUNT_ARRAY <<< "$new_mounts"
for mp in "${NEW_MOUNT_ARRAY[@]}"; do
    new_path="$mp/data"
    # Check if already exists
    if ! echo "$existing_data_dirs" | grep -q "$new_path"; then
        if [ -z "$all_data_dirs" ]; then
            all_data_dirs="\"$new_path\""
        else
            all_data_dirs="$all_data_dirs, \"$new_path\""
        fi
        echo "Appending new path: $new_path"
    else
        echo "Skipping duplicate path: $new_path"
    fi
done

echo "Merged data_dir: [$all_data_dirs]"

# Update data_dir
sed -i "/^data_dir = \[/,/\]/c\data_dir = [$all_data_dirs]" curvine-cluster.toml

echo "data_dir update completed"
UPDATE_SCRIPT_EOF
            
            # Replace variables in script
            sed -i "s|__NEW_MOUNTS__|${mountpoints}|g" /tmp/update_config_${host}.sh
            
            log "  data_dir: append new mount points ${mountpoints}, keep existing paths"
        fi
        
DEPRECATED_CODE
    # ==========End of deprecated code==========
    
    log "========================================"
    log "Configuration file updated, showing modified configuration:"
    log "========================================"
    log ""
    
    # Display updated configuration for each node
    for host in "${!node_mountpoints[@]}"; do
        is_master=false
        if ansible master --list-hosts 2>/dev/null | grep -q "$host"; then
            is_master=true
        fi
        
        log "================================================================================"
        log "Node: $host ($( [ "$is_master" = true ] && echo "Master" || echo "Worker" ))"
        log "================================================================================"
        
        if [ "$is_master" = true ]; then
            # Master node: display meta_dir, journal_dir, data_dir (including multi-line)
            log "--- meta_dir ---"
            meta_result=$(ansible "$host" -m shell -a "grep '^meta_dir' /root/dist/conf/curvine-cluster.toml" 2>/dev/null | \
                grep -v "^$host" | grep -v "SUCCESS" | grep -v "changed" | grep -v "rc=")
            if [ -n "$meta_result" ]; then
                echo "$meta_result" | tee -a "$LOG_FILE"
            else
                log "Unable to read meta_dir"
            fi
            
            log ""
            log "--- journal_dir ---"
            journal_result=$(ansible "$host" -m shell -a "grep '^journal_dir' /root/dist/conf/curvine-cluster.toml" 2>/dev/null | \
                grep -v "^$host" | grep -v "SUCCESS" | grep -v "changed" | grep -v "rc=")
            if [ -n "$journal_result" ]; then
                echo "$journal_result" | tee -a "$LOG_FILE"
            else
                log "Unable to read journal_dir"
            fi
            
            log ""
            log "--- data_dir ---"
            log "[DEBUG] Starting to read $host configuration file..."
            
            # Directly use fetch to get configuration file locally
            ansible "$host" -m fetch -a "src=/root/dist/conf/curvine-cluster.toml dest=/tmp/config_${host}.toml flat=yes" > /dev/null 2>&1
            
            log "[DEBUG] Checking if file download succeeded..."
            if [ -f /tmp/config_${host}.toml ]; then
                log "[DEBUG] File exists, size: $(wc -c < /tmp/config_${host}.toml) bytes"
                log "[DEBUG] Extracting data_dir section..."
                
                # Use awk to extract data_dir
                data_dir_content=$(awk '/^data_dir = \[/,/^\]/' /tmp/config_${host}.toml)
                
                if [ -n "$data_dir_content" ]; then
                    log "[DEBUG] data_dir content extraction successful"
                    echo "$data_dir_content" | tee -a "$LOG_FILE"
                else
                    log "[DEBUG] awk didn't match data_dir, trying grep..."
                    grep -A 10 "data_dir" /tmp/config_${host}.toml | head -15 | tee -a "$LOG_FILE"
                fi
                
                rm -f /tmp/config_${host}.toml
            else
                log "[DEBUG] File download failed"
                log "[DEBUG] Trying direct display..."
                ansible "$host" -m shell -a "cat /root/dist/conf/curvine-cluster.toml | grep -A 10 data_dir | head -15" 2>&1 | \
                    grep -v "SUCCESS" | grep -v "changed" | tee -a "$LOG_FILE"
            fi
        else
            # Worker node: only display data_dir (including multi-line)
            log "--- data_dir ---"
            log "[DEBUG] Starting to read $host configuration file..."
            
            # Directly use fetch to get configuration file locally
            ansible "$host" -m fetch -a "src=/root/dist/conf/curvine-cluster.toml dest=/tmp/config_${host}.toml flat=yes" > /dev/null 2>&1
            
            log "[DEBUG] Checking if file download succeeded..."
            if [ -f /tmp/config_${host}.toml ]; then
                log "[DEBUG] File exists, size: $(wc -c < /tmp/config_${host}.toml) bytes"
                log "[DEBUG] Extracting data_dir section..."
                
                # Use awk to extract data_dir
                data_dir_content=$(awk '/^data_dir = \[/,/^\]/' /tmp/config_${host}.toml)
                
                if [ -n "$data_dir_content" ]; then
                    log "[DEBUG] data_dir content extraction successful"
                    echo "$data_dir_content" | tee -a "$LOG_FILE"
                else
                    log "[DEBUG] awk didn't match data_dir, trying grep..."
                    grep -A 10 "data_dir" /tmp/config_${host}.toml | head -15 | tee -a "$LOG_FILE"
                fi
                
                rm -f /tmp/config_${host}.toml
            else
                log "[DEBUG] File download failed"
                log "[DEBUG] Trying direct display..."
                ansible "$host" -m shell -a "cat /root/dist/conf/curvine-cluster.toml | grep -A 10 data_dir | head -15" 2>&1 | \
                    grep -v "SUCCESS" | grep -v "changed" | tee -a "$LOG_FILE"
            fi
        fi
        log ""
    done
    
    log "========================================"
    read -p "Please confirm configuration file modification is correct, enter Y to continue starting services, enter N to cancel: " config_confirm
    log "User input: $config_confirm"
    
    if [ "$config_confirm" != "Y" ] && [ "$config_confirm" != "y" ]; then
        log "User cancelled service startup"
        log "Tip: You can manually modify the configuration file and then run ansible-playbook start_services.yml to start services"
        exit 0
    fi
fi

# Ask whether to start services
if [ $deploy_result -eq 0 ]; then
    log ""
    
    # If no disks were formatted, still need to ask whether to start
    if [ ${#node_mountpoints[@]} -eq 0 ]; then
        read -p "Deployment completed! Start services immediately? (y/n): " start_services
        log "User input: $start_services"
    else
        # If configuration has already been confirmed, start directly
        start_services="y"
        log "Configuration confirmed, starting services..."
    fi
    
    if [ "$start_services" == "y" ]; then
        log ""
        log "========================================"
        log "Checking and fixing environment variable CURVINE_MASTER_HOSTNAME..."
        log "========================================"
        
        # Check and fix environment variables for each node
        if [ -n "$success_hosts" ]; then
            IFS=',' read -ra ALL_HOSTS <<< "$success_hosts"
            for host in "${ALL_HOSTS[@]}"; do
                # Determine node type
                is_master_node=false
                if ansible master --list-hosts 2>/dev/null | grep -q "$host"; then
                    is_master_node=true
                fi
                
                # Get current environment variable value
                current_env=$(ansible "$host" -m shell -a "grep 'CURVINE_MASTER_HOSTNAME' /etc/profile | grep -v '^#' | tail -1" 2>/dev/null | \
                    grep -v "SUCCESS" | grep -v "changed" | grep -v "rc=" | sed 's/.*CURVINE_MASTER_HOSTNAME=//' | xargs)
                
                if [ "$is_master_node" = true ]; then
                    # Master node: should be its own IP
                    # Get eth0 or bond0 IP for this node
                    node_ip=$(ansible "$host" -m shell -a "ip addr show eth0 2>/dev/null | grep 'inet ' | awk '{print \$2}' | cut -d/ -f1 || ip addr show bond0 2>/dev/null | grep 'inet ' | awk '{print \$2}' | cut -d/ -f1" 2>/dev/null | \
                        grep -v "SUCCESS" | grep -v "changed" | grep -E '^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$' | head -1)
                    
                    if [ -z "$node_ip" ]; then
                        node_ip="$host"  # If getting fails, use host IP
                    fi
                    
                    log "Node $host (Master):"
                    log "  Current environment variable: CURVINE_MASTER_HOSTNAME=$current_env"
                    log "  Should be set to: $node_ip"
                    
                    if [ "$current_env" != "$node_ip" ]; then
                        log "  ⚠️  Environment variable incorrect, fixing..."
                        
                        # Remove old setting
                        ansible "$host" -m shell -a "sed -i '/CURVINE_MASTER_HOSTNAME/d' /etc/profile" 2>&1 | tee -a "$LOG_FILE"
                        
                        # Add new setting
                        ansible "$host" -m shell -a "echo 'export CURVINE_MASTER_HOSTNAME=$node_ip' >> /etc/profile" 2>&1 | tee -a "$LOG_FILE"
                        
                        # Source to take effect
                        ansible "$host" -m shell -a "source /etc/profile" 2>&1 | tee -a "$LOG_FILE"
                        
                        log "  ✓ Fixed to: $node_ip"
                    else
                        log "  ✓ Environment variable correct"
                    fi
                else
                    # Worker node: should be localhost
                    log "Node $host (Worker):"
                    log "  Current environment variable: CURVINE_MASTER_HOSTNAME=$current_env"
                    log "  Should be set to: localhost"
                    
                    if [ "$current_env" != "localhost" ]; then
                        log "  ⚠️  Environment variable incorrect, fixing..."
                        
                        # Remove old setting
                        ansible "$host" -m shell -a "sed -i '/CURVINE_MASTER_HOSTNAME/d' /etc/profile" 2>&1 | tee -a "$LOG_FILE"
                        
                        # Add new setting
                        ansible "$host" -m shell -a "echo 'export CURVINE_MASTER_HOSTNAME=localhost' >> /etc/profile" 2>&1 | tee -a "$LOG_FILE"
                        
                        # Source to take effect
                        ansible "$host" -m shell -a "source /etc/profile" 2>&1 | tee -a "$LOG_FILE"
                        
                        log "  ✓ Fixed to: localhost"
                    else
                        log "  ✓ Environment variable correct"
                    fi
                fi
                log ""
            done
        fi
        
        log ""
        log "========================================"
        log "Checking and updating master_addrs and journal_addrs..."
        log "========================================"
        
        # Get all master node IPs
        master_ips=$(ansible master --list-hosts 2>/dev/null | grep '^ ' | sed 's/^ *//' | grep -v "hosts" | xargs)
        log "Master node IP list: $master_ips"
        
        # Update master_addrs and journal_addrs for all nodes
        if [ -n "$success_hosts" ]; then
            IFS=',' read -ra ALL_HOSTS <<< "$success_hosts"
            for host in "${ALL_HOSTS[@]}"; do
                log "Checking master_addrs configuration for node $host..."
                
                # Check if configuration file exists (with error handling)
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

