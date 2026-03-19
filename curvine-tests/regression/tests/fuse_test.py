"""FUSE test module for build-server"""
import subprocess
import os
import sys
import json
from datetime import datetime
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
from utils import cluster_utils
from utils import test_utils


def _ensure_test_dir(test_results_dir):
    """Return latest test dir, or create a new timestamped one."""
    latest = test_utils.find_latest_test_dir(test_results_dir)
    if latest:
        return latest
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    test_dir = os.path.join(test_results_dir, timestamp)
    os.makedirs(test_dir, exist_ok=True)
    return test_dir


def _write_fuse_failure(test_dir, error_msg, cancelled=False):
    """Write a minimal fuse-test-results.json and update test_summary for failures/cancels.
    This ensures the result page always shows a FUSE section, even on failure.
    """
    if not test_dir:
        return
    status_str = 'CANCELLED' if cancelled else 'FAILED'
    result = {
        'test_suite': 'fuse-test',
        'timestamp': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
        'summary': {'total_tests': 0, 'passed': 0, 'failed': 0},
        'error': error_msg,
        'status': status_str,
        'tests': []
    }
    json_path = os.path.join(test_dir, 'fuse-test-results.json')
    if not os.path.exists(json_path):
        try:
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2)
        except Exception as e:
            print(f"Warning: could not write fuse-test-results.json: {e}")
    # Update test_summary.json so the result page summary section also reflects it
    try:
        test_utils.ensure_test_summary(test_dir)
        test_utils.update_test_summary(test_dir, {
            'fuse_test': {
                'status': status_str.lower(),
                'error': error_msg,
                'total_tests': 0,
                'passed_tests': 0,
                'failed_tests': 0,
            }
        })
    except Exception as e:
        print(f"Warning: could not update test_summary for fuse failure: {e}")


def run_fuse_test_independent(project_path, test_results_dir, fuse_status, fuse_lock,
                              test_processes=None, process_key='fuse', target_test_dir=None):
    """Run FUSE test independently in a thread."""
    with fuse_lock:
        fuse_status['status'] = 'testing'
        fuse_status['message'] = 'Ensuring cluster is ready for FUSE test...'
        fuse_status['test_dir'] = ''
        fuse_status['report_url'] = ''

        try:
            # Ensure cluster is ready before running FUSE test
            success, error_msg = cluster_utils.ensure_cluster_ready(project_path, test_path='/curvine-fuse')
            if not success:
                msg = f'Failed to prepare cluster for FUSE test: {error_msg}'
                fuse_status['status'] = 'failed'
                fuse_status['message'] = msg
                test_dir = target_test_dir or _ensure_test_dir(test_results_dir)
                fuse_status['test_dir'] = test_dir
                fuse_status['report_url'] = f"/result?date={os.path.basename(test_dir)}"
                _write_fuse_failure(test_dir, msg)
                return

            fuse_status['message'] = 'Starting FUSE test...'
            original_cwd = os.getcwd()
            os.chdir(project_path)

            test_dir = target_test_dir or _ensure_test_dir(test_results_dir)
            if target_test_dir:
                os.makedirs(test_dir, exist_ok=True)
            fuse_status['test_dir'] = test_dir

            fuse_script = os.path.join(project_path, 'build', 'dist', 'tests', 'fuse-test.sh')
            if not os.path.exists(fuse_script):
                msg = f'FUSE test script not found: {fuse_script}'
                fuse_status['status'] = 'failed'
                fuse_status['message'] = msg
                os.chdir(original_cwd)
                _write_fuse_failure(test_dir, msg)
                fuse_status['report_url'] = f"/result?date={os.path.basename(test_dir)}"
                return

            json_output = os.path.join(test_dir, 'fuse-test-results.json')
            process = subprocess.Popen(
                ['bash', fuse_script, '--json-output', json_output],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                start_new_session=True
            )
            if test_processes is not None:
                test_processes[process_key] = process
            try:
                for line in process.stdout:
                    print(line, end='')
                process.wait()
            finally:
                if test_processes is not None:
                    test_processes[process_key] = None

            os.chdir(original_cwd)
            cancelled = process.returncode < 0

            if process.returncode == 0:
                fuse_status['status'] = 'completed'
                fuse_status['message'] = 'FUSE test completed successfully.'
            else:
                fuse_status['status'] = 'cancelled' if cancelled else 'failed'
                stderr_output = process.stderr.read()
                msg = 'FUSE test was cancelled.' if cancelled else f'FUSE test failed. Error: {stderr_output}'
                fuse_status['message'] = msg
                # Write failure JSON so result page shows a FUSE section
                _write_fuse_failure(test_dir, msg, cancelled=cancelled)

            fuse_status['report_url'] = f"/result?date={os.path.basename(test_dir)}"

        except Exception as e:
            fuse_status['status'] = 'failed'
            fuse_status['message'] = f'An error occurred: {str(e)}'
            import traceback
            traceback.print_exc()
