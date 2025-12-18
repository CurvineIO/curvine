#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
同步配置文件中的data_dir与实际挂载的/datads*目录
确保配置文件包含所有已挂载的数据目录
"""

import sys
import re

def sync_config(config_file, datads_list, is_master):
    """同步配置文件"""
    try:
        with open(config_file, 'r') as f:
            content = f.read()
        
        # 备份
        with open(config_file + '.before_sync', 'w') as f:
            f.write(content)
        
        print(f"实际挂载的datads目录: {datads_list}")
        
        # 1. 提取现有的data_dir
        existing_paths = []
        data_dir_match = re.search(r'data_dir\s*=\s*\[(.*?)\]', content, re.DOTALL)
        if data_dir_match:
            paths = re.findall(r'"([^"]+)"', data_dir_match.group(1))
            existing_paths = paths
            print(f"配置文件中现有的data_dir: {existing_paths}")
        
        # 2. 构建完整的data_dir列表（保留现有 + 添加缺失的datads）
        all_data_dirs = list(existing_paths)
        
        for datads in datads_list:
            data_path = f"{datads}/data"
            if data_path not in all_data_dirs:
                all_data_dirs.append(data_path)
                print(f"添加缺失的路径: {data_path}")
        
        # 3. 更新data_dir
        if len(datads_list) > 0 and len(all_data_dirs) > 0:
            data_dir_str = "data_dir = [" + ", ".join([f'"{p}"' for p in all_data_dirs]) + "]"
            print(f"新的data_dir: {data_dir_str}")
            
            # 手动替换data_dir
            data_dir_start = content.find('data_dir')
            if data_dir_start != -1:
                bracket_start = content.find('[', data_dir_start)
                bracket_count = 1
                pos = bracket_start + 1
                while pos < len(content) and bracket_count > 0:
                    if content[pos] == '[':
                        bracket_count += 1
                    elif content[pos] == ']':
                        bracket_count -= 1
                    pos += 1
                
                if bracket_count == 0:
                    line_start = data_dir_start
                    while line_start > 0 and content[line_start - 1] not in ['\n', '\r']:
                        line_start -= 1
                    
                    content = content[:line_start] + data_dir_str + '\n' + content[pos:]
                    print("data_dir已更新")
        
        # 4. 处理meta_dir和journal_dir（只针对Master节点）
        if is_master and len(datads_list) > 0:
            first_datads = datads_list[0]  # 使用第一个datads
            
            # 检查并更新meta_dir
            meta_match = re.search(r'meta_dir\s*=\s*"([^"]+)"', content)
            if meta_match:
                current_meta = meta_match.group(1)
                if '/datads' in current_meta:
                    print(f"meta_dir已经是datads格式，保持不变: {current_meta}")
                elif current_meta.startswith('/data/'):
                    new_meta = current_meta.replace('/data/', f'{first_datads}/', 1)
                    content = re.sub(r'(meta_dir\s*=\s*)"/data/', f'\\1"{first_datads}/', content)
                    print(f"meta_dir已更新: {current_meta} -> {new_meta}")
            
            # 检查并更新journal_dir
            journal_match = re.search(r'journal_dir\s*=\s*"([^"]+)"', content)
            if journal_match:
                current_journal = journal_match.group(1)
                if '/datads' in current_journal:
                    print(f"journal_dir已经是datads格式，保持不变: {current_journal}")
                elif current_journal.startswith('/data/'):
                    new_journal = current_journal.replace('/data/', f'{first_datads}/', 1)
                    content = re.sub(r'(journal_dir\s*=\s*)"/data/', f'\\1"{first_datads}/', content)
                    print(f"journal_dir已更新: {current_journal} -> {new_journal}")
        
        # 5. 写回文件
        with open(config_file, 'w') as f:
            f.write(content)
        
        print("配置文件同步完成")
        return True
        
    except Exception as e:
        print(f"错误: {e}")
        return False

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("用法: sync_datads_config.py <config_file> <is_master> <datads1,datads2,...>")
        print("  is_master: master 或 worker")
        print("  datads列表: /datads1,/datads2,/datads3")
        sys.exit(1)
    
    config_file = sys.argv[1]
    is_master = sys.argv[2].lower() == 'master'
    
    datads_list = []
    if len(sys.argv) > 3 and sys.argv[3]:
        datads_list = [d.strip() for d in sys.argv[3].split(',') if d.strip()]
    
    print(f"节点类型: {'Master' if is_master else 'Worker'}")
    print(f"配置文件: {config_file}")
    print(f"datads列表: {datads_list}")
    
    sync_config(config_file, datads_list, is_master)

