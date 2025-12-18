#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
更新curvine配置文件中的目录配置
支持追加模式，保留现有配置
"""

import sys
import re
import os

def read_toml_simple(filepath):
    """简单的toml读取（不依赖toml库）"""
    with open(filepath, 'r') as f:
        content = f.read()
    return content

def extract_data_dir(content):
    """提取现有的data_dir列表"""
    # 查找data_dir行的开始位置
    data_dir_start = content.find('data_dir')
    if data_dir_start == -1:
        print("[DEBUG] 未找到data_dir配置")
        return []
    
    # 从data_dir开始，找到 [
    bracket_start = content.find('[', data_dir_start)
    if bracket_start == -1:
        print("[DEBUG] 未找到data_dir的开始括号")
        return []
    
    # 找到匹配的 ]
    bracket_count = 1
    pos = bracket_start + 1
    while pos < len(content) and bracket_count > 0:
        if content[pos] == '[':
            bracket_count += 1
        elif content[pos] == ']':
            bracket_count -= 1
        pos += 1
    
    if bracket_count == 0:
        # 提取完整的数组内容
        array_content = content[bracket_start:pos]
        print(f"[DEBUG] 提取的数组内容: {array_content}")
        
        # 提取所有引号内的路径
        paths = re.findall(r'"([^"]+)"', array_content)
        print(f"[DEBUG] 提取到 {len(paths)} 个路径: {paths}")
        return paths
    
    print("[DEBUG] 未找到data_dir的结束括号")
    return []

def update_master_config(filepath, first_mount, all_mounts):
    """更新Master节点配置"""
    content = read_toml_simple(filepath)
    
    # 读取现有的data_dir
    existing_paths = extract_data_dir(content)
    print(f"现有data_dir路径: {existing_paths}")
    
    # 合并路径：保留现有 + 追加新的
    all_paths = list(existing_paths)  # 复制现有路径
    for mount in all_mounts:
        new_path = f"{mount}/data"
        if new_path not in all_paths:
            all_paths.append(new_path)
            print(f"追加新路径: {new_path}")
    
    # 构建新的data_dir
    data_dir_str = "data_dir = [" + ", ".join([f'"{p}"' for p in all_paths]) + "]"
    print(f"新的data_dir: {data_dir_str}")
    
    # 手动替换data_dir（更可靠）
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
                # 找到data_dir所在行的开始
                line_start = data_dir_start
                while line_start > 0 and content[line_start - 1] not in ['\n', '\r']:
                    line_start -= 1
                
                old_data_dir = content[line_start:pos]
                print(f"[DEBUG] 旧的data_dir: {old_data_dir}")
                content = content[:line_start] + data_dir_str + '\n' + content[pos:]
                print(f"[DEBUG] data_dir替换成功")
    
    # 更新meta_dir和journal_dir（只在是/data/*且不包含datads时更新）
    meta_match = re.search(r'meta_dir\s*=\s*"([^"]+)"', content)
    if meta_match and meta_match.group(1).startswith('/data/') and '/datads' not in meta_match.group(1):
        content = re.sub(r'(meta_dir\s*=\s*)"/data/', f'\\1"{first_mount}/', content)
        print(f"meta_dir已更新为: {first_mount}/meta")
    else:
        print("meta_dir保持不变（已经是/datads*或非/data/*）")
    
    journal_match = re.search(r'journal_dir\s*=\s*"([^"]+)"', content)
    if journal_match and journal_match.group(1).startswith('/data/') and '/datads' not in journal_match.group(1):
        content = re.sub(r'(journal_dir\s*=\s*)"/data/', f'\\1"{first_mount}/', content)
        print(f"journal_dir已更新为: {first_mount}/journal")
    else:
        print("journal_dir保持不变（已经是/datads*或非/data/*）")
    
    # 写回文件
    with open(filepath, 'w') as f:
        f.write(content)
    
    print("配置文件更新完成")
    return True

def update_worker_config(filepath, all_mounts):
    """更新Worker节点配置"""
    content = read_toml_simple(filepath)
    
    # 读取现有的data_dir
    existing_paths = extract_data_dir(content)
    print(f"现有data_dir路径: {existing_paths}")
    
    # 合并路径：保留现有 + 追加新的
    all_paths = list(existing_paths)  # 复制现有路径
    for mount in all_mounts:
        new_path = f"{mount}/data"
        if new_path not in all_paths:
            all_paths.append(new_path)
            print(f"追加新路径: {new_path}")
    
    # 构建新的data_dir
    data_dir_str = "data_dir = [" + ", ".join([f'"{p}"' for p in all_paths]) + "]"
    print(f"新的data_dir: {data_dir_str}")
    
    # 手动替换data_dir（更可靠）
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
                # 找到data_dir所在行的开始
                line_start = data_dir_start
                while line_start > 0 and content[line_start - 1] not in ['\n', '\r']:
                    line_start -= 1
                
                old_data_dir = content[line_start:pos]
                print(f"[DEBUG] 旧的data_dir: {old_data_dir}")
                content = content[:line_start] + data_dir_str + '\n' + content[pos:]
                print(f"[DEBUG] data_dir替换成功")
    
    # 写回文件
    with open(filepath, 'w') as f:
        f.write(content)
    
    print("配置文件更新完成")
    return True

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("用法:")
        print("  Master节点: update_toml_config.py master <config_file> <first_mount> <mount1,mount2,...>")
        print("  Worker节点: update_toml_config.py worker <config_file> <mount1,mount2,...>")
        sys.exit(1)
    
    node_type = sys.argv[1]
    config_file = sys.argv[2]
    
    if not os.path.exists(config_file):
        print(f"错误: 配置文件不存在: {config_file}")
        sys.exit(1)
    
    # 备份
    os.system(f"cp {config_file} {config_file}.bak")
    
    if node_type == 'master':
        if len(sys.argv) < 5:
            print("错误: Master节点需要提供first_mount和mounts参数")
            sys.exit(1)
        first_mount = sys.argv[3]
        mounts = sys.argv[4].split(',')
        update_master_config(config_file, first_mount, mounts)
    elif node_type == 'worker':
        if len(sys.argv) < 4:
            print("错误: Worker节点需要提供mounts参数")
            sys.exit(1)
        mounts = sys.argv[3].split(',')
        update_worker_config(config_file, mounts)
    else:
        print(f"错误: 未知的节点类型: {node_type}")
        sys.exit(1)
