"""FUSE test module for build-server"""
import subprocess
import os
import sys
from datetime import datetime
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
from utils import cluster_utils
from utils import test_utils


def run_fuse_test_independent(project_path, test_results_dir, fuse_status, fuse_lock):
    """Run FUSE test independently in a thread
    
    Args:
        project_path: Path to the project root
        test_results_dir: Path to test results directory
        fuse_status: Status dictionary to update
        fuse_lock: Lock object for thread safety
    """
    with fuse_lock:
        fuse_status['status'] = 'testing'
        fuse_status['message'] = 'Ensuring cluster is ready for FUSE test...'
        fuse_status['test_dir'] = ''
        fuse_status['report_url'] = ''
        
        try:
            # Ensure cluster is ready before running FUSE test
            success, error_msg = cluster_utils.ensure_cluster_ready(project_path, test_path='/curvine-fuse')
            if not success:
                fuse_status['status'] = 'failed'
                fuse_status['message'] = f'Failed to prepare cluster for FUSE test: {error_msg}'
                return
            
            fuse_status['message'] = 'Starting FUSE test...'
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
            
            # Find fuse-test.sh script in build/dist/tests
            fuse_script = os.path.join(project_path, 'build', 'dist', 'tests', 'fuse-test.sh')
            if not os.path.exists(fuse_script):
                fuse_status['status'] = 'failed'
                fuse_status['message'] = f'FUSE test script not found: {fuse_script}'
                os.chdir(original_cwd)
                return
            
            # Run FUSE test with JSON output
            json_output = os.path.join(test_dir, 'fuse-test-results.json')
            process = subprocess.Popen(
                ['bash', fuse_script, '--json-output', json_output],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True
            )
            
            # Stream output
            for line in process.stdout:
                print(line, end='')
            
            process.wait()
            
            fuse_status['test_dir'] = test_dir
            
            if process.returncode == 0:
                fuse_status['status'] = 'completed'
                fuse_status['message'] = 'FUSE test completed successfully.'
                if fuse_status['test_dir']:
                    fuse_status['report_url'] = f"/result?date={os.path.basename(test_dir)}"
            else:
                fuse_status['status'] = 'failed'
                stderr_output = process.stderr.read()
                fuse_status['message'] = f'FUSE test failed. Error: {stderr_output}'
            
            os.chdir(original_cwd)
            
        except Exception as e:
            fuse_status['status'] = 'failed'
            fuse_status['message'] = f'An error occurred: {str(e)}'
            import traceback
            traceback.print_exc()

