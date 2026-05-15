#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sync data_dir in configuration file with actually mounted /datads* directories
Ensure configuration file contains all mounted data directories
"""

import sys
import re

def sync_config(config_file, datads_list, is_master):
    """Sync configuration file"""
    try:
        with open(config_file, 'r') as f:
            content = f.read()
        
        # Backup
        with open(config_file + '.before_sync', 'w') as f:
            f.write(content)
        
        print(f"Actually mounted datads directories: {datads_list}")
        
        # 1. Extract existing data_dir
        existing_paths = []
        data_dir_match = re.search(r'data_dir\s*=\s*\[(.*?)\]', content, re.DOTALL)
        if data_dir_match:
            paths = re.findall(r'"([^"]+)"', data_dir_match.group(1))
            existing_paths = paths
            print(f"Existing data_dir in configuration file: {existing_paths}")
        
        # 2. Build complete data_dir list (keep existing + add missing datads)
        all_data_dirs = list(existing_paths)
        
        for datads in datads_list:
            data_path = f"{datads}/data"
            if data_path not in all_data_dirs:
                all_data_dirs.append(data_path)
                print(f"Adding missing path: {data_path}")
        
        # 3. Update data_dir
        if len(datads_list) > 0 and len(all_data_dirs) > 0:
            data_dir_str = "data_dir = [" + ", ".join([f'"{p}"' for p in all_data_dirs]) + "]"
            print(f"New data_dir: {data_dir_str}")
            
            # Manually replace data_dir
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
                    print("data_dir updated")
        
        # 4. Handle meta_dir and journal_dir (only for Master nodes)
        if is_master and len(datads_list) > 0:
            first_datads = datads_list[0]  # Use first datads
            
            # Check and update meta_dir
            meta_match = re.search(r'meta_dir\s*=\s*"([^"]+)"', content)
            if meta_match:
                current_meta = meta_match.group(1)
                if '/datads' in current_meta:
                    print(f"meta_dir already in datads format, keeping unchanged: {current_meta}")
                elif current_meta.startswith('/data/'):
                    new_meta = current_meta.replace('/data/', f'{first_datads}/', 1)
                    content = re.sub(r'(meta_dir\s*=\s*)"/data/', f'\\1"{first_datads}/', content)
                    print(f"meta_dir updated: {current_meta} -> {new_meta}")
            
            # Check and update journal_dir
            journal_match = re.search(r'journal_dir\s*=\s*"([^"]+)"', content)
            if journal_match:
                current_journal = journal_match.group(1)
                if '/datads' in current_journal:
                    print(f"journal_dir already in datads format, keeping unchanged: {current_journal}")
                elif current_journal.startswith('/data/'):
                    new_journal = current_journal.replace('/data/', f'{first_datads}/', 1)
                    content = re.sub(r'(journal_dir\s*=\s*)"/data/', f'\\1"{first_datads}/', content)
                    print(f"journal_dir updated: {current_journal} -> {new_journal}")
        
        # 5. Write back to file
        with open(config_file, 'w') as f:
            f.write(content)
        
        print("Configuration file sync completed")
        return True
        
    except Exception as e:
        print(f"Error: {e}")
        return False

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Usage: sync_datads_config.py <config_file> <is_master> <datads1,datads2,...>")
        print("  is_master: master or worker")
        print("  datads list: /datads1,/datads2,/datads3")
        sys.exit(1)
    
    config_file = sys.argv[1]
    is_master = sys.argv[2].lower() == 'master'
    
    datads_list = []
    if len(sys.argv) > 3 and sys.argv[3]:
        datads_list = [d.strip() for d in sys.argv[3].split(',') if d.strip()]
    
    print(f"Node type: {'Master' if is_master else 'Worker'}")
    print(f"Configuration file: {config_file}")
    print(f"datads list: {datads_list}")
    
    sync_config(config_file, datads_list, is_master)

