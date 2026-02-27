"""Test utilities for build-server

This module provides utility functions for test directory management and script discovery.
"""
import os
import json
import re
import shutil
from datetime import datetime


def find_latest_test_dir(test_results_dir):
    """Find the latest test directory
    
    Args:
        test_results_dir: Path to test results directory
        
    Returns:
        str: Path to latest test directory, or None if not found
    """
    if not os.path.exists(test_results_dir):
        return None
    
    test_dirs = []
    for item in os.listdir(test_results_dir):
        item_path = os.path.join(test_results_dir, item)
        if os.path.isdir(item_path):
            # Check if it's a timestamp directory (YYYYMMDD_HHMMSS)
            if re.match(r'^\d{8}_\d{6}$', item):
                test_dirs.append((item, os.path.getmtime(item_path)))
    
    if not test_dirs:
        return None
    
    # Sort by folder name (YYYYMMDD_HHMMSS timestamp string), get latest.
    # Using folder name rather than mtime avoids returning a stale directory
    # whose mtime was recently bumped by an unrelated write.
    test_dirs.sort(key=lambda x: x[0], reverse=True)
    return os.path.join(test_results_dir, test_dirs[0][0])


def ensure_test_summary(test_dir):
    """Ensure test_summary.json exists in test_dir, create if not exists
    
    Args:
        test_dir: Path to test directory
        
    Returns:
        dict: The summary dictionary (loaded or newly created)
    """
    summary_file = os.path.join(test_dir, 'test_summary.json')
    
    if os.path.exists(summary_file):
        try:
            with open(summary_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"Warning: Failed to load test_summary.json: {e}, creating new one")
    
    # Create new summary structure
    summary = {
        'timestamp': datetime.now().isoformat(),
        'total_tests': 0,
        'passed_tests': 0,
        'failed_tests': 0,
        'success_rate': 0,
        'packages': [],
        'test_cases': []
    }
    
    # Save it
    try:
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        print(f"Created test_summary.json in {test_dir}")
    except Exception as e:
        print(f"Warning: Failed to create test_summary.json: {e}")
    
    return summary


def update_test_summary(test_dir, updates):
    """Update test_summary.json with new data
    
    Args:
        test_dir: Path to test directory
        updates: Dictionary with fields to update in test_summary.json
        
    Returns:
        bool: True if update successful, False otherwise
    """
    try:
        summary = ensure_test_summary(test_dir)
        
        # Update summary with new data
        summary.update(updates)
        
        # Save updated summary
        summary_file = os.path.join(test_dir, 'test_summary.json')
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        
        return True
    except Exception as e:
        print(f"Error updating test_summary.json: {e}")
        import traceback
        traceback.print_exc()
        return False


def find_script_path(script_name="daily_regression_test.sh", project_path=None):
    """Auto-detect script path
    
    Args:
        script_name: Name of the script to find
        project_path: Optional project path to check
        
    Returns:
        str: Path to script, or None if not found
    """
    # 1. First check build/tests directory relative to project_path (if provided)
    if project_path:
        script_in_project_build_tests = os.path.join(project_path, 'build', 'tests', script_name)
        if os.path.exists(script_in_project_build_tests):
            return script_in_project_build_tests
    
    # 2. Check build/tests directory in current working directory (new location)
    current_dir = os.getcwd()
    script_in_build_tests = os.path.join(current_dir, 'build', 'tests', script_name)
    if os.path.exists(script_in_build_tests):
        return script_in_build_tests
    
    # 3. Check current directory
    script_in_current = os.path.join(current_dir, script_name)
    if os.path.exists(script_in_current):
        return script_in_current
    
    # 4. Check scripts subdirectory of current directory (legacy location)
    script_in_scripts = os.path.join(current_dir, 'scripts', script_name)
    if os.path.exists(script_in_scripts):
        return script_in_scripts
    
    # 5. Check scripts subdirectory relative to project_path (legacy location)
    if project_path:
        script_in_project_scripts = os.path.join(project_path, 'scripts', script_name)
        if os.path.exists(script_in_project_scripts):
            return script_in_project_scripts
    
    # 6. Check PATH environment variable
    script_in_path = shutil.which(script_name)
    if script_in_path:
        return script_in_path
    
    # 7. Check common locations
    common_paths = [
        '/usr/local/bin',
        '/usr/bin',
        '/opt/curvine/bin',
        '/home/curvine/bin'
    ]
    
    for path in common_paths:
        script_path = os.path.join(path, script_name)
        if os.path.exists(script_path):
            return script_path
    
    return None

