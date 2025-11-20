"""Unittest (regression test) module for build-server"""
import subprocess
import os
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
from utils import test_utils


def run_regression_test_independent(project_path, test_results_dir, regression_status, regression_lock):
    """Run regression test independently in a thread
    
    Args:
        project_path: Path to the project root
        test_results_dir: Path to test results directory
        regression_status: Status dictionary to update
        regression_lock: Lock object for thread safety
    """
    with regression_lock:
        regression_status['status'] = 'testing'
        regression_status['message'] = 'Starting regression test...'
        regression_status['test_dir'] = ''
        regression_status['report_url'] = ''
        
        try:
            # Auto-detect script path
            script_path = test_utils.find_script_path(project_path=project_path)
            if not script_path:
                regression_status['status'] = 'failed'
                regression_status['message'] = 'Cannot find daily_regression_test.sh script'
                return
            
            # Check if script exists
            if not os.path.exists(script_path):
                regression_status['status'] = 'failed'
                regression_status['message'] = f'Test script not found: {script_path}'
                return
            
            print(f"Using script path: {script_path}")

            process = subprocess.Popen(
                [script_path, project_path, test_results_dir],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True
            )

            # Stream output in real time
            for line in process.stdout:
                print(line, end='')  # Print to console

            # Wait for process to finish
            process.wait()

            # Find the latest test directory
            latest_test_dir = test_utils.find_latest_test_dir(test_results_dir)
            if latest_test_dir:
                regression_status['test_dir'] = latest_test_dir

            if process.returncode == 0:
                regression_status['status'] = 'completed'
                regression_status['message'] = 'Regression test completed successfully.'
                # Generate report URL
                if regression_status['test_dir']:
                    regression_status['report_url'] = f"http://localhost:5002/result?date={regression_status['test_dir'].split('/')[-1]}"
            else:
                regression_status['status'] = 'failed'
                # Print error output
                stderr_output = process.stderr.read()
                regression_status['message'] = f'Regression test failed. Error: {stderr_output}'
        except Exception as e:
            regression_status['status'] = 'failed'
            regression_status['message'] = f'An error occurred: {str(e)}'

