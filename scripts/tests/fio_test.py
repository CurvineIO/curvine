"""FIO test module for build-server"""
import subprocess
import os
import sys
from datetime import datetime
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
from utils import cluster_utils
from utils import test_utils


def run_fio_test_independent(project_path, test_results_dir, fio_status, fio_lock):
    """Run FIO test independently in a thread
    
    Args:
        project_path: Path to the project root
        test_results_dir: Path to test results directory
        fio_status: Status dictionary to update
        fio_lock: Lock object for thread safety
    """
    with fio_lock:
        fio_status['status'] = 'testing'
        fio_status['message'] = 'Ensuring cluster is ready for FIO test...'
        fio_status['test_dir'] = ''
        fio_status['report_url'] = ''
        
        try:
            # Ensure cluster is ready before running FIO test
            success, error_msg = cluster_utils.ensure_cluster_ready(project_path, test_path='/curvine-fuse')
            if not success:
                fio_status['status'] = 'failed'
                fio_status['message'] = f'Failed to prepare cluster for FIO test: {error_msg}'
                return
            
            fio_status['message'] = 'Starting FIO test...'
            # Change to project directory
            original_cwd = os.getcwd()
            os.chdir(project_path)
            
            # Find latest test directory or create new one
            latest_test_dir = test_utils.find_latest_test_dir(test_results_dir)
            if latest_test_dir:
                test_dir = latest_test_dir
            else:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                test_dir = os.path.join(test_results_dir, timestamp)
                os.makedirs(test_dir, exist_ok=True)
            
            # Find fio-test.sh script in build/dist/tests
            fio_script = os.path.join(project_path, 'build', 'dist', 'tests', 'fio-test.sh')
            if not os.path.exists(fio_script):
                fio_status['status'] = 'failed'
                fio_status['message'] = f'FIO test script not found: {fio_script}'
                os.chdir(original_cwd)
                return
            
            # Run FIO test with JSON output
            json_output = os.path.join(test_dir, 'fio-test-results.json')
            process = subprocess.Popen(
                ['bash', fio_script, '--json-output', json_output],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True
            )
            
            # Stream output
            for line in process.stdout:
                print(line, end='')
            
            process.wait()
            
            fio_status['test_dir'] = test_dir
            
            if process.returncode == 0:
                fio_status['status'] = 'completed'
                fio_status['message'] = 'FIO test completed successfully.'
                if fio_status['test_dir']:
                    fio_status['report_url'] = f"/result?date={os.path.basename(test_dir)}"
            else:
                fio_status['status'] = 'failed'
                stderr_output = process.stderr.read()
                fio_status['message'] = f'FIO test failed. Error: {stderr_output}'
            
            os.chdir(original_cwd)
            
        except Exception as e:
            fio_status['status'] = 'failed'
            fio_status['message'] = f'An error occurred: {str(e)}'
            import traceback
            traceback.print_exc()

