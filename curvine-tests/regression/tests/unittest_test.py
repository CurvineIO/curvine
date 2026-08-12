"""Unittest (regression test) module for build-server"""
import subprocess
import os
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
from utils import test_utils


def run_regression_test_independent(project_path, test_results_dir, regression_status, regression_lock,
                                    test_processes=None, process_key='regression', target_test_dir=None):
    """Run regression test independently in a thread.

    Args:
        project_path: Path to the project root
        test_results_dir: Path to test results directory
        regression_status: Status dictionary to update
        regression_lock: Lock object for thread safety
        test_processes: Optional dict to register current process for cancel (keyed by process_key)
        process_key: Key in test_processes for this test's process
        target_test_dir: If set, write results to this specific directory instead of creating a new one
    """
    with regression_lock:
        regression_status['status'] = 'testing'
        regression_status['message'] = 'Starting regression test...'
        regression_status['test_dir'] = ''
        regression_status['report_url'] = ''

        try:
            script_path = test_utils.find_script_path(project_path=project_path)
            if not script_path:
                regression_status['status'] = 'failed'
                regression_status['message'] = 'Cannot find daily_regression_test.sh script'
                return

            if not os.path.exists(script_path):
                regression_status['status'] = 'failed'
                regression_status['message'] = f'Test script not found: {script_path}'
                return

            print(f"Using script path: {script_path}")

            # Pass target dir via env var so the shell script writes to that dir directly
            env = os.environ.copy()
            if target_test_dir:
                os.makedirs(target_test_dir, exist_ok=True)
                env['CURVINE_TEST_DIR'] = target_test_dir

            process = subprocess.Popen(
                [script_path, project_path, test_results_dir],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                start_new_session=True,
                env=env,
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

            # Determine which directory the results were written to
            if target_test_dir:
                result_dir = target_test_dir
            else:
                result_dir = test_utils.find_latest_test_dir(test_results_dir)
            if result_dir:
                regression_status['test_dir'] = result_dir

            if process.returncode == 0:
                regression_status['status'] = 'completed'
                regression_status['message'] = 'Regression test completed successfully.'
                if regression_status['test_dir']:
                    regression_status['report_url'] = f"/result?date={os.path.basename(regression_status['test_dir'])}"
            else:
                regression_status['status'] = 'cancelled' if process.returncode < 0 else 'failed'
                stderr_output = process.stderr.read()
                regression_status['message'] = ('Regression test was cancelled.'
                                                 if process.returncode < 0
                                                 else f'Regression test failed. Error: {stderr_output}')
        except Exception as e:
            regression_status['status'] = 'failed'
            regression_status['message'] = f'An error occurred: {str(e)}'

