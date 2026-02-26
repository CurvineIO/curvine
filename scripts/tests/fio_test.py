"""FIO test module for build-server"""
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


def _write_fio_failure(test_dir, error_msg, cancelled=False):
    """Write a minimal fio-test-results.json and update test_summary for failures/cancels.
    This ensures the result page always shows a FIO section, even on failure.
    """
    if not test_dir:
        return
    status_str = 'CANCELLED' if cancelled else 'FAILED'
    result = {
        'test_suite': 'fio-test',
        'timestamp': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
        'summary': {'total_tests': 0, 'passed': 0, 'failed': 0},
        'error': error_msg,
        'status': status_str,
        'tests': []
    }
    json_path = os.path.join(test_dir, 'fio-test-results.json')
    if not os.path.exists(json_path):
        try:
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2)
        except Exception as e:
            print(f"Warning: could not write fio-test-results.json: {e}")
    try:
        test_utils.ensure_test_summary(test_dir)
        test_utils.update_test_summary(test_dir, {
            'fio_test': {
                'status': status_str.lower(),
                'error': error_msg,
                'total_tests': 0,
                'passed_tests': 0,
                'failed_tests': 0,
                'success_rate': 0,
            }
        })
    except Exception as e:
        print(f"Warning: could not update test_summary for fio failure: {e}")


def run_fio_test_independent(project_path, test_results_dir, fio_status, fio_lock,
                             test_processes=None, process_key='fio', target_test_dir=None):
    """Run FIO test independently in a thread."""
    with fio_lock:
        fio_status['status'] = 'testing'
        fio_status['message'] = 'Ensuring cluster is ready for FIO test...'
        fio_status['test_dir'] = ''
        fio_status['report_url'] = ''

        try:
            success, error_msg = cluster_utils.ensure_cluster_ready(project_path, test_path='/curvine-fuse')
            if not success:
                msg = f'Failed to prepare cluster for FIO test: {error_msg}'
                fio_status['status'] = 'failed'
                fio_status['message'] = msg
                test_dir = target_test_dir or _ensure_test_dir(test_results_dir)
                fio_status['test_dir'] = test_dir
                fio_status['report_url'] = f"/result?date={os.path.basename(test_dir)}"
                _write_fio_failure(test_dir, msg)
                return

            fio_status['message'] = 'Starting FIO test...'
            original_cwd = os.getcwd()
            os.chdir(project_path)

            test_dir = target_test_dir or _ensure_test_dir(test_results_dir)
            if target_test_dir:
                os.makedirs(test_dir, exist_ok=True)
            fio_status['test_dir'] = test_dir

            fio_script = os.path.join(project_path, 'build', 'dist', 'tests', 'fio-test.sh')
            if not os.path.exists(fio_script):
                msg = f'FIO test script not found: {fio_script}'
                fio_status['status'] = 'failed'
                fio_status['message'] = msg
                os.chdir(original_cwd)
                _write_fio_failure(test_dir, msg)
                fio_status['report_url'] = f"/result?date={os.path.basename(test_dir)}"
                return

            json_output = os.path.join(test_dir, 'fio-test-results.json')
            process = subprocess.Popen(
                ['bash', fio_script, '--json-output', json_output],
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
                fio_status['status'] = 'completed'
                fio_status['message'] = 'FIO test completed successfully.'
            else:
                fio_status['status'] = 'cancelled' if cancelled else 'failed'
                stderr_output = process.stderr.read()
                msg = 'FIO test was cancelled.' if cancelled else f'FIO test failed. Error: {stderr_output}'
                fio_status['message'] = msg
                _write_fio_failure(test_dir, msg, cancelled=cancelled)

            fio_status['report_url'] = f"/result?date={os.path.basename(test_dir)}"

        except Exception as e:
            fio_status['status'] = 'failed'
            fio_status['message'] = f'An error occurred: {str(e)}'
            import traceback
            traceback.print_exc()
