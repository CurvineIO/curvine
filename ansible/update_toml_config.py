#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Update directory configuration in curvine configuration file
Supports append mode, keeps existing configuration
"""

import sys
import re
import os

def read_toml_simple(filepath):
    """Simple toml reading (does not depend on toml library)"""
    with open(filepath, 'r') as f:
        content = f.read()
    return content

def extract_data_dir(content):
    """Extract existing data_dir list"""
    # Find start position of data_dir line
    data_dir_start = content.find('data_dir')
    if data_dir_start == -1:
        print("[DEBUG] data_dir configuration not found")
        return []
    
    # From data_dir start, find [
    bracket_start = content.find('[', data_dir_start)
    if bracket_start == -1:
        print("[DEBUG] Starting bracket of data_dir not found")
        return []
    
    # Find matching ]
    bracket_count = 1
    pos = bracket_start + 1
    while pos < len(content) and bracket_count > 0:
        if content[pos] == '[':
            bracket_count += 1
        elif content[pos] == ']':
            bracket_count -= 1
        pos += 1
    
    if bracket_count == 0:
        # Extract complete array content
        array_content = content[bracket_start:pos]
        print(f"[DEBUG] Extracted array content: {array_content}")
        
        # Extract all paths within quotes
        paths = re.findall(r'"([^"]+)"', array_content)
        print(f"[DEBUG] Extracted {len(paths)} paths: {paths}")
        return paths
    
    print("[DEBUG] Ending bracket of data_dir not found")
    return []

def update_master_config(filepath, first_mount, all_mounts):
    """Update Master node configuration"""
    content = read_toml_simple(filepath)
    
    # Read existing data_dir
    existing_paths = extract_data_dir(content)
    print(f"Existing data_dir paths: {existing_paths}")
    
    # Merge paths: keep existing + append new
    all_paths = list(existing_paths)  # Copy existing paths
    for mount in all_mounts:
        new_path = f"{mount}/data"
        if new_path not in all_paths:
            all_paths.append(new_path)
            print(f"Appending new path: {new_path}")
    
    # Build new data_dir
    data_dir_str = "data_dir = [" + ", ".join([f'"{p}"' for p in all_paths]) + "]"
    print(f"New data_dir: {data_dir_str}")
    
    # Manually replace data_dir (more reliable)
    data_dir_start = content.find('data_dir')
    if data_dir_start != -1:
        bracket_start = content.find('[', data_dir_start)
        if bracket_start != -1:
            # 找到匹配的]
            bracket_count = 1
            pos = bracket_start + 1
            while pos < len(content) and bracket_count > 0:
                if content[pos] == '[':
                    bracket_count += 1
                elif content[pos] == ']':
                    bracket_count -= 1
                pos += 1
            
            if bracket_count == 0:
                # Find start of line where data_dir is located
                line_start = data_dir_start
                while line_start > 0 and content[line_start - 1] not in ['\n', '\r']:
                    line_start -= 1
                
                old_data_dir = content[line_start:pos]
                print(f"[DEBUG] Old data_dir: {old_data_dir}")
                content = content[:line_start] + data_dir_str + '\n' + content[pos:]
                print(f"[DEBUG] data_dir replacement successful")
    
    # Update meta_dir and journal_dir (only update when it's /data/* and doesn't contain datads)
    meta_match = re.search(r'meta_dir\s*=\s*"([^"]+)"', content)
    if meta_match and meta_match.group(1).startswith('/data/') and '/datads' not in meta_match.group(1):
        content = re.sub(r'(meta_dir\s*=\s*)"/data/', f'\\1"{first_mount}/', content)
        print(f"meta_dir updated to: {first_mount}/meta")
    else:
        print("meta_dir unchanged (already /datads* or not /data/*)")
    
    journal_match = re.search(r'journal_dir\s*=\s*"([^"]+)"', content)
    if journal_match and journal_match.group(1).startswith('/data/') and '/datads' not in journal_match.group(1):
        content = re.sub(r'(journal_dir\s*=\s*)"/data/', f'\\1"{first_mount}/', content)
        print(f"journal_dir updated to: {first_mount}/journal")
    else:
        print("journal_dir unchanged (already /datads* or not /data/*)")
    
    # Write back to file
    with open(filepath, 'w') as f:
        f.write(content)
    
    print("Configuration file update completed")
    return True

def update_worker_config(filepath, all_mounts):
    """Update Worker node configuration"""
    content = read_toml_simple(filepath)
    
    # Read existing data_dir
    existing_paths = extract_data_dir(content)
    print(f"Existing data_dir paths: {existing_paths}")
    
    # Merge paths: keep existing + append new
    all_paths = list(existing_paths)  # Copy existing paths
    for mount in all_mounts:
        new_path = f"{mount}/data"
        if new_path not in all_paths:
            all_paths.append(new_path)
            print(f"Appending new path: {new_path}")
    
    # Build new data_dir
    data_dir_str = "data_dir = [" + ", ".join([f'"{p}"' for p in all_paths]) + "]"
    print(f"New data_dir: {data_dir_str}")
    
    # Manually replace data_dir (more reliable)
    data_dir_start = content.find('data_dir')
    if data_dir_start != -1:
        bracket_start = content.find('[', data_dir_start)
        if bracket_start != -1:
            # 找到匹配的]
            bracket_count = 1
            pos = bracket_start + 1
            while pos < len(content) and bracket_count > 0:
                if content[pos] == '[':
                    bracket_count += 1
                elif content[pos] == ']':
                    bracket_count -= 1
                pos += 1
            
            if bracket_count == 0:
                # Find start of line where data_dir is located
                line_start = data_dir_start
                while line_start > 0 and content[line_start - 1] not in ['\n', '\r']:
                    line_start -= 1
                
                old_data_dir = content[line_start:pos]
                print(f"[DEBUG] Old data_dir: {old_data_dir}")
                content = content[:line_start] + data_dir_str + '\n' + content[pos:]
                print(f"[DEBUG] data_dir replacement successful")
    
    # Write back to file
    with open(filepath, 'w') as f:
        f.write(content)
    
    print("Configuration file update completed")
    return True

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Usage:")
        print("  Master node: update_toml_config.py master <config_file> <first_mount> <mount1,mount2,...>")
        print("  Worker node: update_toml_config.py worker <config_file> <mount1,mount2,...>")
        sys.exit(1)
    
    node_type = sys.argv[1]
    config_file = sys.argv[2]
    
    if not os.path.exists(config_file):
        print(f"Error: Configuration file does not exist: {config_file}")
        sys.exit(1)
    
    # Backup
    os.system(f"cp {config_file} {config_file}.bak")
    
    if node_type == 'master':
        if len(sys.argv) < 5:
            print("Error: Master node needs to provide first_mount and mounts parameters")
            sys.exit(1)
        first_mount = sys.argv[3]
        mounts = sys.argv[4].split(',')
        update_master_config(config_file, first_mount, mounts)
    elif node_type == 'worker':
        if len(sys.argv) < 4:
            print("Error: Worker node needs to provide mounts parameter")
            sys.exit(1)
        mounts = sys.argv[3].split(',')
        update_worker_config(config_file, mounts)
    else:
        print(f"Error: Unknown node type: {node_type}")
        sys.exit(1)
