from flask import Flask, request, jsonify, render_template, Response
from werkzeug.utils import secure_filename
import subprocess
import threading
import os
import signal
import json
import glob
import sys
import argparse
import shutil
import re
import time
from datetime import datetime

# Import test modules
from tests import unittest_test, coverage_test, fuse_test, fio_test, ltp_test
from utils import cluster_utils, test_utils

# Configure Flask app with template directory
script_dir = os.path.dirname(os.path.abspath(__file__))
template_dir = os.path.join(script_dir, 'templates')
app = Flask(__name__, template_folder=template_dir)

# Build status storage
build_status = {
    'status': 'idle',  # idle, building, completed, failed
    'message': ''
}

# Daily test status storage
dailytest_status = {
    'status': 'idle',  # idle, testing, completed, failed
    'message': '',
    'test_dir': '',
    'report_url': ''
}

# Coverage test status storage
coverage_status = {
    'status': 'idle',  # idle, testing, completed, failed
    'message': '',
    'test_dir': '',
    'report_url': ''
}

# Regression test status storage
regression_status = {
    'status': 'idle',  # idle, testing, completed, failed
    'message': '',
    'test_dir': '',
    'report_url': ''
}

# FUSE test status storage
fuse_status = {
    'status': 'idle',  # idle, testing, completed, failed
    'message': '',
    'test_dir': '',
    'report_url': ''
}

# FIO test status storage
fio_status = {
    'status': 'idle',  # idle, testing, completed, failed
    'message': '',
    'test_dir': '',
    'report_url': ''
}

# LTP test status storage
ltp_status = {
    'status': 'idle',  # idle, testing, completed, failed
    'message': '',
    'test_dir': '',
    'report_url': ''
}

# Create lock objects
build_lock = threading.Lock()
dailytest_lock = threading.Lock()
coverage_lock = threading.Lock()
regression_lock = threading.Lock()
fuse_lock = threading.Lock()
fio_lock = threading.Lock()
ltp_lock = threading.Lock()

# Project path (global)
PROJECT_PATH = None

# Test results directory (global)
TEST_RESULTS_DIR = None

# Current subprocess per test (for cancel). Keys: dailytest, regression, coverage, fuse, fio, ltp
test_processes = {
    'dailytest': None,
    'regression': None,
    'coverage': None,
    'fuse': None,
    'fio': None,
    'ltp': None
}
# Cancel requested flag for dailytest (checked between steps)
dailytest_cancel_requested = False


def _kill_process(p):
    """Kill a process and all its children by killing the process group.
    Using os.killpg instead of p.terminate() because bash scripts spawn
    child processes; p.terminate() only kills bash, children stay alive
    and keep stdout open, blocking 'for line in process.stdout' forever.
    start_new_session=True in Popen ensures the child is a process group leader.
    """
    if p is None or p.poll() is not None:
        return
    try:
        import signal as _signal
        os.killpg(os.getpgid(p.pid), _signal.SIGTERM)
    except Exception:
        try:
            p.terminate()
        except Exception:
            pass


def _handle_sigterm(signum, frame):
    """On SIGTERM (e.g. systemctl stop): kill all test subprocesses and exit immediately."""
    for key in list(test_processes):
        _kill_process(test_processes.get(key))
    os._exit(0)


signal.signal(signal.SIGTERM, _handle_sigterm)


def run_build_script(date, commit):
    global build_status
    with build_lock:  # Ensure only one build instance at a time
        build_status['status'] = 'building'
        build_status['message'] = f'Starting build for date: {date}, commit: {commit}'

        try:
            # Use Popen to run build script and stream logs
            process = subprocess.Popen(
                [os.path.join(script_dir, 'build.sh'), date, commit],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True
            )

            # Stream output in real time
            for line in process.stdout:
                print(line, end='')  # Print to console

            # Wait for process to finish
            process.wait()

            if process.returncode == 0:
                build_status['status'] = 'completed'
                build_status['message'] = 'Build completed successfully.'
            else:
                build_status['status'] = 'failed'
                # Print error output
                stderr_output = process.stderr.read()
                build_status['message'] = f'Build failed. Error: {stderr_output}'

        except Exception as e:
            build_status['status'] = 'failed'
            build_status['message'] = f'An error occurred: {str(e)}'

# Test configuration - enable/disable specific tests in dailytest
DAILYTEST_CONFIG = {
    'unittest': True,
    'coverage': True,
    'fio': True,
    'fuse': True,
    'ltp': True
}

def run_dailytest_script():
    """Run unittest, coverage, fio, fuse, and ltp tests in sequence"""
    global dailytest_status, dailytest_cancel_requested, test_processes
    with dailytest_lock:  # Ensure only one test instance at a time
        dailytest_cancel_requested = False
        dailytest_status['status'] = 'testing'
        dailytest_status['message'] = 'Starting daily test (unittest + coverage + fio + fuse + ltp)...'
        dailytest_status['test_dir'] = ''
        dailytest_status['report_url'] = ''

        try:
            project_path = PROJECT_PATH if PROJECT_PATH else os.getcwd()
            latest_test_dir = None
            
            # Track test results for final status update
            unittest_failed = False
            coverage_failed = False
            fio_test_failed = False
            fuse_test_failed = False
            ltp_test_failed = False
            
            # Count enabled tests for step numbering
            enabled_tests = [k for k, v in DAILYTEST_CONFIG.items() if v]
            total_steps = len(enabled_tests)
            current_step = 0
            
            # Step 1: Run unittest (regression test)
            if DAILYTEST_CONFIG.get('unittest', True):
                if dailytest_cancel_requested:
                    dailytest_status.update({'status': 'cancelled', 'message': 'Daily test was cancelled by user.'})
                    return
                current_step += 1
                dailytest_status['message'] = f'Step {current_step}/{total_steps}: Running unittest...'
                regression_status.update({'status': 'testing', 'message': f'Step {current_step}/{total_steps}: Running regression test...', 'test_dir': '', 'report_url': ''})
                print("="*80)
                print(f"Step {current_step}/{total_steps}: Running unittest (regression test)...")
                print("="*80)

                script_path = test_utils.find_script_path(project_path=project_path)
                if not script_path:
                    unittest_failed = True
                    regression_status.update({'status': 'failed', 'message': 'Cannot find daily_regression_test.sh script.'})
                    print(f"⚠ Step {current_step}/{total_steps}: Cannot find daily_regression_test.sh script, skipping unittest")
                elif not os.path.exists(script_path):
                    unittest_failed = True
                    regression_status.update({'status': 'failed', 'message': f'Test script not found: {script_path}.'})
                    print(f"⚠ Step {current_step}/{total_steps}: Test script not found: {script_path}, skipping unittest")
                else:
                    print(f"Using script path: {script_path}")

                    process = subprocess.Popen(
                        [script_path, project_path, TEST_RESULTS_DIR],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        universal_newlines=True,
                        start_new_session=True,
                    )
                    test_processes['dailytest'] = process
                    try:
                        for line in process.stdout:
                            print(line, end='')
                        process.wait()
                    finally:
                        test_processes['dailytest'] = None

                    if dailytest_cancel_requested:
                        regression_status.update({'status': 'cancelled', 'message': 'Cancelled (daily test was cancelled).', 'test_dir': latest_test_dir or '', 'report_url': ''})
                        dailytest_status.update({'status': 'cancelled', 'message': 'Daily test was cancelled by user.'})
                        return

                    latest_test_dir = test_utils.find_latest_test_dir(TEST_RESULTS_DIR)
                    if latest_test_dir:
                        dailytest_status['test_dir'] = latest_test_dir
                        test_utils.ensure_test_summary(latest_test_dir)

                    _r1url = f"/result?date={os.path.basename(latest_test_dir)}" if latest_test_dir else ''
                    if process.returncode != 0:
                        unittest_failed = True
                        stderr_output = process.stderr.read()
                        regression_status.update({'status': 'failed', 'message': f'Failed (return code: {process.returncode}).', 'test_dir': latest_test_dir or '', 'report_url': _r1url})
                        print(f"\n⚠ Step {current_step}/{total_steps}: Unittest completed with failures (return code: {process.returncode})")
                        print(f"Error output: {stderr_output}")
                        if latest_test_dir:
                            test_utils.update_test_summary(latest_test_dir, {
                                'unittest_status': 'failed',
                                'unittest_error': stderr_output[:500] if stderr_output else 'Unknown error'
                            })
                    else:
                        regression_status.update({'status': 'completed', 'message': 'Completed successfully (via daily test).', 'test_dir': latest_test_dir or '', 'report_url': _r1url})
                        print(f"\n✓ Step {current_step}/{total_steps}: Unittest completed successfully\n")
                        if latest_test_dir:
                            test_utils.update_test_summary(latest_test_dir, {'unittest_status': 'completed'})
            
            # Step 2: Run coverage test
            if DAILYTEST_CONFIG.get('coverage', True):
                if dailytest_cancel_requested:
                    dailytest_status.update({'status': 'cancelled', 'message': 'Daily test was cancelled by user.'})
                    return
                current_step += 1
                dailytest_status['message'] = f'Step {current_step}/{total_steps}: Running coverage test...'
                coverage_status.update({'status': 'testing', 'message': f'Step {current_step}/{total_steps}: Running coverage test...', 'test_dir': '', 'report_url': ''})
                print("="*80)
                print(f"Step {current_step}/{total_steps}: Running coverage test...")
                print("="*80)

                if latest_test_dir:
                    test_utils.ensure_test_summary(latest_test_dir)
                    # Pass coverage_status so it gets per-step updates directly
                    coverage_success = coverage_test.run_coverage_test(
                        project_path, TEST_RESULTS_DIR, latest_test_dir,
                        update_status=coverage_status,
                        process_holder=test_processes, process_key='coverage',
                    )
                    if not coverage_success:
                        coverage_failed = True
                        print(f"\n⚠ Step {current_step}/{total_steps}: Coverage test completed with failures")
                        test_utils.update_test_summary(latest_test_dir, {'coverage_status': 'failed'})
                    else:
                        print(f"\n✓ Step {current_step}/{total_steps}: Coverage test completed successfully\n")
                else:
                    coverage_failed = True
                    print(f"⚠ Step {current_step}/{total_steps}: Could not find test directory for coverage test, skipping")
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    latest_test_dir = os.path.join(TEST_RESULTS_DIR, timestamp)
                    os.makedirs(latest_test_dir, exist_ok=True)
                    dailytest_status['test_dir'] = latest_test_dir
                    test_utils.ensure_test_summary(latest_test_dir)
                    coverage_status.update({'status': 'failed', 'message': 'No test directory available, skipped.'})
            
            # Step 3: Run FIO test
            if DAILYTEST_CONFIG.get('fio', True):
                if dailytest_cancel_requested:
                    dailytest_status.update({'status': 'cancelled', 'message': 'Daily test was cancelled by user.'})
                    return
                current_step += 1
                dailytest_status['message'] = f'Step {current_step}/{total_steps}: Ensuring cluster is ready for FIO test...'
                fio_status.update({'status': 'testing', 'message': f'Step {current_step}/{total_steps}: Ensuring cluster...', 'test_dir': '', 'report_url': ''})
                print("="*80)
                print(f"Step {current_step}/{total_steps}: Ensuring cluster is ready for FIO test...")
                print("="*80)

                success, error_msg = cluster_utils.ensure_cluster_ready(project_path, test_path='/curvine-fuse')
                if not success:
                    fio_test_failed = True
                    fio_status.update({'status': 'failed', 'message': f'Cluster preparation failed: {error_msg}', 'test_dir': latest_test_dir or '', 'report_url': ''})
                    print(f"⚠ Step {current_step}/{total_steps}: Failed to prepare cluster for FIO test: {error_msg}, skipping FIO test")
                    if latest_test_dir:
                        test_utils.ensure_test_summary(latest_test_dir)
                        test_utils.update_test_summary(latest_test_dir, {'fio_test': {'status': 'skipped', 'reason': f'Cluster preparation failed: {error_msg}'}})
                else:
                    dailytest_status['message'] = f'Step {current_step}/{total_steps}: Running FIO test...'
                    fio_status['message'] = f'Step {current_step}/{total_steps}: Running FIO test...'
                    print("="*80)
                    print(f"Step {current_step}/{total_steps}: Running FIO test...")
                    print("="*80)

                    latest_test_dir = test_utils.find_latest_test_dir(TEST_RESULTS_DIR)
                    if not latest_test_dir:
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        latest_test_dir = os.path.join(TEST_RESULTS_DIR, timestamp)
                        os.makedirs(latest_test_dir, exist_ok=True)
                        dailytest_status['test_dir'] = latest_test_dir
                    fio_status['test_dir'] = latest_test_dir

                    original_cwd = os.getcwd()
                    os.chdir(project_path)

                    fio_script = os.path.join(project_path, 'build', 'dist', 'tests', 'fio-test.sh')
                    if not os.path.exists(fio_script):
                        fio_test_failed = True
                        fio_status.update({'status': 'failed', 'message': f'Script not found: {fio_script}'})
                        print(f"⚠ Step {current_step}/{total_steps}: FIO test script not found: {fio_script}, skipping FIO test")
                        os.chdir(original_cwd)
                        if latest_test_dir:
                            test_utils.ensure_test_summary(latest_test_dir)
                            test_utils.update_test_summary(latest_test_dir, {'fio_test': {'status': 'skipped', 'reason': f'Script not found: {fio_script}'}})
                    else:
                        json_output = os.path.join(latest_test_dir, 'fio-test-results.json')
                        fio_process = subprocess.Popen(
                            ['bash', fio_script, '--json-output', json_output],
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            universal_newlines=True,
                            start_new_session=True,
                        )
                        test_processes['dailytest'] = fio_process
                        try:
                            for line in fio_process.stdout:
                                print(line, end='')
                            fio_process.wait()
                        finally:
                            test_processes['dailytest'] = None
                        os.chdir(original_cwd)

                        if dailytest_cancel_requested:
                            _f3url = f"/result?date={os.path.basename(latest_test_dir)}" if latest_test_dir else ''
                            fio_status.update({'status': 'cancelled', 'message': 'Cancelled (daily test was cancelled).', 'report_url': _f3url})
                            dailytest_status.update({'status': 'cancelled', 'message': 'Daily test was cancelled by user.'})
                            return

                        if latest_test_dir:
                            test_utils.ensure_test_summary(latest_test_dir)
                            fio_json_file = os.path.join(latest_test_dir, 'fio-test-results.json')
                            fio_test_data = None
                            if os.path.exists(fio_json_file):
                                try:
                                    with open(fio_json_file, 'r', encoding='utf-8') as f:
                                        fio_test_data = json.load(f)
                                except Exception as e:
                                    print(f"Warning: Failed to read FIO test results: {e}")

                            fio_summary = {
                                'fio_test': {
                                    'status': 'completed' if fio_process.returncode == 0 else 'failed',
                                    'return_code': fio_process.returncode,
                                    'results_file': 'fio-test-results.json' if os.path.exists(fio_json_file) else None
                                }
                            }
                            if fio_test_data and 'tests' in fio_test_data:
                                _ftotal = len(fio_test_data['tests'])
                                _fpassed = sum(1 for t in fio_test_data['tests'] if t.get('status') == 'PASSED')
                                fio_summary['fio_test'].update({
                                    'total_tests': _ftotal,
                                    'passed_tests': _fpassed,
                                    'failed_tests': _ftotal - _fpassed,
                                    'success_rate': round((_fpassed / _ftotal * 100) if _ftotal > 0 else 0, 2)
                                })
                            test_utils.update_test_summary(latest_test_dir, fio_summary)

                        _f3url = f"/result?date={os.path.basename(latest_test_dir)}" if latest_test_dir else ''
                        if fio_process.returncode != 0:
                            fio_test_failed = True
                            stderr_output = fio_process.stderr.read()
                            fio_status.update({'status': 'failed', 'message': f'Failed (return code: {fio_process.returncode}).', 'report_url': _f3url})
                            print(f"\n⚠ Step {current_step}/{total_steps}: FIO test completed with failures (return code: {fio_process.returncode})")
                            print(f"Error output: {stderr_output}")
                        else:
                            fio_status.update({'status': 'completed', 'message': 'Completed successfully (via daily test).', 'report_url': _f3url})
                            print(f"\n✓ Step {current_step}/{total_steps}: FIO test completed successfully\n")
            
            # Step 4: Run FUSE test
            if DAILYTEST_CONFIG.get('fuse', True):
                if dailytest_cancel_requested:
                    dailytest_status.update({'status': 'cancelled', 'message': 'Daily test was cancelled by user.'})
                    return
                current_step += 1
                dailytest_status['message'] = f'Step {current_step}/{total_steps}: Ensuring cluster is ready for FUSE test...'
                fuse_status.update({'status': 'testing', 'message': f'Step {current_step}/{total_steps}: Ensuring cluster...', 'test_dir': '', 'report_url': ''})
                print("="*80)
                print(f"Step {current_step}/{total_steps}: Ensuring cluster is ready for FUSE test...")
                print("="*80)

                success, error_msg = cluster_utils.ensure_cluster_ready(project_path, test_path='/curvine-fuse')
                if not success:
                    fuse_test_failed = True
                    fuse_status.update({'status': 'failed', 'message': f'Cluster preparation failed: {error_msg}', 'test_dir': latest_test_dir or '', 'report_url': ''})
                    print(f"⚠ Step {current_step}/{total_steps}: Failed to prepare cluster for FUSE test: {error_msg}, skipping FUSE test")
                    if latest_test_dir:
                        test_utils.ensure_test_summary(latest_test_dir)
                        test_utils.update_test_summary(latest_test_dir, {'fuse_test': {'status': 'skipped', 'reason': f'Cluster preparation failed: {error_msg}'}})
                else:
                    dailytest_status['message'] = f'Step {current_step}/{total_steps}: Running FUSE test...'
                    fuse_status['message'] = f'Step {current_step}/{total_steps}: Running FUSE test...'
                    print("="*80)
                    print(f"Step {current_step}/{total_steps}: Running FUSE test...")
                    print("="*80)

                    latest_test_dir = test_utils.find_latest_test_dir(TEST_RESULTS_DIR)
                    if not latest_test_dir:
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        latest_test_dir = os.path.join(TEST_RESULTS_DIR, timestamp)
                        os.makedirs(latest_test_dir, exist_ok=True)
                        dailytest_status['test_dir'] = latest_test_dir
                    fuse_status['test_dir'] = latest_test_dir

                    original_cwd = os.getcwd()
                    os.chdir(project_path)

                    fuse_script = os.path.join(project_path, 'build', 'dist', 'tests', 'fuse-test.sh')
                    if not os.path.exists(fuse_script):
                        fuse_test_failed = True
                        fuse_status.update({'status': 'failed', 'message': f'Script not found: {fuse_script}'})
                        print(f"⚠ Step {current_step}/{total_steps}: FUSE test script not found: {fuse_script}, skipping FUSE test")
                        os.chdir(original_cwd)
                        if latest_test_dir:
                            test_utils.ensure_test_summary(latest_test_dir)
                            test_utils.update_test_summary(latest_test_dir, {'fuse_test': {'status': 'skipped', 'reason': f'Script not found: {fuse_script}'}})
                    else:
                        json_output = os.path.join(latest_test_dir, 'fuse-test-results.json')
                        fuse_process = subprocess.Popen(
                            ['bash', fuse_script, '--json-output', json_output],
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            universal_newlines=True,
                            start_new_session=True,
                        )
                        test_processes['dailytest'] = fuse_process
                        try:
                            for line in fuse_process.stdout:
                                print(line, end='')
                            fuse_process.wait()
                        finally:
                            test_processes['dailytest'] = None
                        os.chdir(original_cwd)

                        if dailytest_cancel_requested:
                            _f4url = f"/result?date={os.path.basename(latest_test_dir)}" if latest_test_dir else ''
                            fuse_status.update({'status': 'cancelled', 'message': 'Cancelled (daily test was cancelled).', 'report_url': _f4url})
                            dailytest_status.update({'status': 'cancelled', 'message': 'Daily test was cancelled by user.'})
                            return

                        if latest_test_dir:
                            test_utils.ensure_test_summary(latest_test_dir)
                            fuse_json_file = os.path.join(latest_test_dir, 'fuse-test-results.json')
                            fuse_test_data = None
                            if os.path.exists(fuse_json_file):
                                try:
                                    with open(fuse_json_file, 'r', encoding='utf-8') as f:
                                        fuse_test_data = json.load(f)
                                except Exception as e:
                                    print(f"Warning: Failed to read FUSE test results: {e}")

                            fuse_summary = {
                                'fuse_test': {
                                    'status': 'completed' if fuse_process.returncode == 0 else 'failed',
                                    'return_code': fuse_process.returncode,
                                    'results_file': 'fuse-test-results.json' if os.path.exists(fuse_json_file) else None
                                }
                            }
                            if fuse_test_data and 'tests' in fuse_test_data:
                                _fstotal = len(fuse_test_data['tests'])
                                _fspassed = sum(1 for t in fuse_test_data['tests'] if t.get('status') == 'PASSED')
                                fuse_summary['fuse_test'].update({
                                    'total_tests': _fstotal,
                                    'passed_tests': _fspassed,
                                    'failed_tests': _fstotal - _fspassed,
                                    'success_rate': round((_fspassed / _fstotal * 100) if _fstotal > 0 else 0, 2)
                                })
                            test_utils.update_test_summary(latest_test_dir, fuse_summary)

                        _f4url = f"/result?date={os.path.basename(latest_test_dir)}" if latest_test_dir else ''
                        if fuse_process.returncode != 0:
                            fuse_test_failed = True
                            stderr_output = fuse_process.stderr.read()
                            fuse_status.update({'status': 'failed', 'message': f'Failed (return code: {fuse_process.returncode}).', 'report_url': _f4url})
                            print(f"\n⚠ Step {current_step}/{total_steps}: FUSE test completed with failures (return code: {fuse_process.returncode})")
                            print(f"Error output: {stderr_output}")
                        else:
                            fuse_status.update({'status': 'completed', 'message': 'Completed successfully (via daily test).', 'report_url': _f4url})
                            print(f"\n✓ Step {current_step}/{total_steps}: FUSE test completed successfully\n")
                sys.stdout.flush()
            
            # Step 5: Run LTP test
            if DAILYTEST_CONFIG.get('ltp', True):
                if dailytest_cancel_requested:
                    dailytest_status.update({'status': 'cancelled', 'message': 'Daily test was cancelled by user.'})
                    return
                current_step += 1
                dailytest_status['message'] = f'Step {current_step}/{total_steps}: Ensuring cluster is ready for LTP test...'
                ltp_status.update({'status': 'testing', 'message': f'Step {current_step}/{total_steps}: Ensuring cluster...', 'test_dir': '', 'report_url': ''})
                print("="*80)
                print(f"Step {current_step}/{total_steps}: Ensuring cluster is ready for LTP test...")
                print("="*80)
                sys.stdout.flush()
                
                # Ensure cluster is ready before running LTP test
                try:
                    success, error_msg = cluster_utils.ensure_cluster_ready(project_path, test_path='/curvine-fuse')
                except Exception as e:
                    import traceback
                    traceback.print_exc()
                    success = False
                    error_msg = str(e)
                sys.stdout.flush()
                if not success:
                    ltp_test_failed = True
                    ltp_status.update({'status': 'failed', 'message': f'Cluster preparation failed: {error_msg}', 'test_dir': latest_test_dir or '', 'report_url': ''})
                    print(f"⚠ Step {current_step}/{total_steps}: Failed to prepare cluster for LTP test: {error_msg}, skipping LTP test")
                    if latest_test_dir:
                        test_utils.ensure_test_summary(latest_test_dir)
                        test_utils.update_test_summary(latest_test_dir, {'ltp_test': {'status': 'skipped', 'reason': f'Cluster preparation failed: {error_msg}'}})
                else:
                    dailytest_status['message'] = f'Step {current_step}/{total_steps}: Running LTP test...'
                    ltp_status['message'] = f'Step {current_step}/{total_steps}: Running LTP test...'
                    print("="*80)
                    print(f"Step {current_step}/{total_steps}: Running LTP test...")
                    print("="*80)

                    latest_test_dir = test_utils.find_latest_test_dir(TEST_RESULTS_DIR)
                    if not latest_test_dir:
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        latest_test_dir = os.path.join(TEST_RESULTS_DIR, timestamp)
                        os.makedirs(latest_test_dir, exist_ok=True)
                        dailytest_status['test_dir'] = latest_test_dir
                    ltp_status['test_dir'] = latest_test_dir

                    if latest_test_dir:
                        test_utils.ensure_test_summary(latest_test_dir)

                    try:
                        ltp_success = ltp_test.run_ltp_test(
                            ltp_path='/opt/ltp',
                            test_path='/curvine-fuse',
                            test_results_dir=TEST_RESULTS_DIR,
                            test_dir=latest_test_dir,
                            test_suites=None,
                            project_path=project_path,
                            ltp_status=ltp_status,       # now reflects in /ltp/status
                            process_holder=test_processes,
                            process_key='dailytest',
                        )
                    except Exception as e:
                        import traceback
                        traceback.print_exc()
                        ltp_success = False

                    if not ltp_success:
                        ltp_test_failed = True
                        print(f"\n⚠ Step {current_step}/{total_steps}: LTP test completed with failures\n")
                    else:
                        print(f"\n✓ Step {current_step}/{total_steps}: LTP test completed successfully\n")
                sys.stdout.flush()
            
            # Update final status atomically to avoid a race window between status and message
            failed_tests = []
            if unittest_failed:
                failed_tests.append('unittest')
            if coverage_failed:
                failed_tests.append('coverage')
            if fio_test_failed:
                failed_tests.append('fio')
            if fuse_test_failed:
                failed_tests.append('fuse')
            if ltp_test_failed:
                failed_tests.append('ltp')

            report_url = (f"/result?date={os.path.basename(dailytest_status['test_dir'])}"
                          if dailytest_status['test_dir'] else '')
            if failed_tests:
                dailytest_status.update({
                    'status': 'failed',
                    'message': f'Daily test completed with failures: {", ".join(failed_tests)} test(s) failed.',
                    'report_url': report_url,
                })
            else:
                dailytest_status.update({
                    'status': 'completed',
                    'message': 'Daily test (unittest + coverage + fio + fuse + ltp) completed successfully.',
                    'report_url': report_url,
                })
            
            print("="*80)
            if failed_tests:
                print(f"Daily tests completed with {len(failed_tests)} failure(s): {', '.join(failed_tests)}")
            else:
                print("All daily tests completed successfully!")
            print("="*80)

        except Exception as e:
            dailytest_status.update({'status': 'failed', 'message': f'An error occurred: {str(e)}'})
            import traceback
            traceback.print_exc()
            # Try to save partial results if we have a test directory
            if latest_test_dir:
                try:
                    test_utils.ensure_test_summary(latest_test_dir)
                    test_utils.update_test_summary(latest_test_dir, {
                        'error': str(e),
                        'error_type': type(e).__name__
                    })
                except Exception as save_error:
                    print(f"Warning: Failed to save partial results: {save_error}")

@app.route('/build', methods=['POST'])
def build():
    data = request.json
    date = data.get('date')
    commit = data.get('commit')

    if not date or not commit:
        return jsonify({'error': 'Missing date or commit parameter.'}), 400

    # Check current build status
    if build_status['status'] == 'building':
        return jsonify({
            'error': 'A build is already in progress.',
            'current_status': build_status
        }), 409

    # Start a new thread to run build script
    threading.Thread(target=run_build_script, args=(date, commit)).start()
    return jsonify({'message': 'Build started.'}), 202

@app.route('/build/status', methods=['GET'])
def status():
    return jsonify(build_status)

@app.route('/dailytest', methods=['POST'])
def dailytest():
    """Start daily test (unittest + coverage + fio + fuse + ltp in sequence)"""
    # Check current test status
    if dailytest_status['status'] == 'testing':
        return jsonify({
            'error': 'A daily test is already in progress.',
            'current_status': dailytest_status
        }), 409

    # Start a new thread to run daily test script (unittest + coverage + fio + fuse + ltp)
    threading.Thread(target=run_dailytest_script).start()
    return jsonify({'message': 'Daily test (unittest + coverage + fio + fuse + ltp) started.'}), 202

@app.route('/dailytest/status', methods=['GET'])
def get_dailytest_status():
    """Get daily test status (regression + coverage)"""
    return jsonify(dailytest_status)

# Only allow YYYYMMDD_HHMMSS to prevent path-traversal via user-supplied folder names.
_DATE_FOLDER_RE = re.compile(r'^\d{8}_\d{6}$')


def _validate_date_folder(date_folder):
    """Return True only if date_folder is a safe, expected timestamp directory name."""
    if not date_folder or not _DATE_FOLDER_RE.match(date_folder):
        return False
    # werkzeug.secure_filename is a CodeQL-recognised sanitizer: it strips path
    # separators and special characters.  A valid YYYYMMDD_HHMMSS name is
    # unchanged, so this acts as both a sanitiser and an extra safety net.
    if secure_filename(date_folder) != date_folder:
        return False
    if not TEST_RESULTS_DIR:
        return False
    # Resolve both paths to catch any remaining symlink / normalisation tricks.
    base = os.path.realpath(TEST_RESULTS_DIR)
    full = os.path.realpath(os.path.join(TEST_RESULTS_DIR, date_folder))
    return os.path.commonpath([base, full]) == base


def _resolve_target_dir(date_folder):
    """Resolve a date folder name to a full path. Returns None if not provided or invalid."""
    if not _validate_date_folder(date_folder):
        return None
    # Use the sanitised name (identical to date_folder for valid input) so
    # the path passed to os.makedirs is derived from a known-safe value.
    safe_name = secure_filename(date_folder)
    full = os.path.join(TEST_RESULTS_DIR, safe_name)
    if not os.path.isdir(full):
        os.makedirs(full, exist_ok=True)
    return full


@app.route('/regression/run', methods=['POST'])
def run_regression():
    """Run regression test independently"""
    if regression_status['status'] == 'testing':
        return jsonify({
            'error': 'A regression test is already in progress.',
            'current_status': regression_status
        }), 409
    data = request.json or {}
    target_dir = _resolve_target_dir(data.get('target_date'))
    project_path = PROJECT_PATH if PROJECT_PATH else os.getcwd()
    threading.Thread(
        target=unittest_test.run_regression_test_independent,
        args=(project_path, TEST_RESULTS_DIR, regression_status, regression_lock),
        kwargs={'test_processes': test_processes, 'process_key': 'regression', 'target_test_dir': target_dir}
    ).start()
    return jsonify({'message': 'Regression test started.'}), 202

@app.route('/regression/status', methods=['GET'])
def get_regression_status():
    """Get regression test status"""
    return jsonify(regression_status)

@app.route('/coverage/run', methods=['POST'])
def run_coverage():
    """Run coverage test independently"""
    if coverage_status['status'] == 'testing':
        return jsonify({
            'error': 'A coverage test is already in progress.',
            'current_status': coverage_status
        }), 409
    data = request.json or {}
    target_dir = _resolve_target_dir(data.get('target_date'))
    project_path = PROJECT_PATH if PROJECT_PATH else os.getcwd()
    threading.Thread(
        target=coverage_test.run_coverage_test_independent,
        args=(project_path, TEST_RESULTS_DIR, coverage_status, coverage_lock),
        kwargs={'test_processes': test_processes, 'process_key': 'coverage', 'target_test_dir': target_dir}
    ).start()
    return jsonify({'message': 'Coverage test started.'}), 202

@app.route('/coverage/status', methods=['GET'])
def get_coverage_status():
    """Get coverage test status"""
    return jsonify(coverage_status)



@app.route('/fuse/run', methods=['POST'])
def run_fuse():
    """Run FUSE test independently"""
    if fuse_status['status'] == 'testing':
        return jsonify({
            'error': 'A FUSE test is already in progress.',
            'current_status': fuse_status
        }), 409
    data = request.json or {}
    target_dir = _resolve_target_dir(data.get('target_date'))
    project_path = PROJECT_PATH if PROJECT_PATH else os.getcwd()
    threading.Thread(
        target=fuse_test.run_fuse_test_independent,
        args=(project_path, TEST_RESULTS_DIR, fuse_status, fuse_lock),
        kwargs={'test_processes': test_processes, 'process_key': 'fuse', 'target_test_dir': target_dir}
    ).start()
    return jsonify({'message': 'FUSE test started.'}), 202

@app.route('/fuse/status', methods=['GET'])
def get_fuse_status():
    """Get FUSE test status"""
    return jsonify(fuse_status)

@app.route('/fio/run', methods=['POST'])
def run_fio():
    """Run FIO test independently"""
    if fio_status['status'] == 'testing':
        return jsonify({
            'error': 'A FIO test is already in progress.',
            'current_status': fio_status
        }), 409
    data = request.json or {}
    target_dir = _resolve_target_dir(data.get('target_date'))
    project_path = PROJECT_PATH if PROJECT_PATH else os.getcwd()
    threading.Thread(
        target=fio_test.run_fio_test_independent,
        args=(project_path, TEST_RESULTS_DIR, fio_status, fio_lock),
        kwargs={'test_processes': test_processes, 'process_key': 'fio', 'target_test_dir': target_dir}
    ).start()
    return jsonify({'message': 'FIO test started.'}), 202

@app.route('/fio/status', methods=['GET'])
def get_fio_status():
    """Get FIO test status"""
    return jsonify(fio_status)

@app.route('/ltp/run', methods=['POST'])
def run_ltp():
    """Run LTP test independently"""
    if ltp_status['status'] == 'testing':
        return jsonify({
            'error': 'An LTP test is already in progress.',
            'current_status': ltp_status
        }), 409
    data = request.json or {}
    ltp_path = data.get('ltp_path', '/opt/ltp')
    test_path = data.get('test_path', '/curvine-fuse')
    test_suites = ltp_test.LTP_TEST_SUITES
    project_path = data.get('project_path', PROJECT_PATH)
    target_dir = _resolve_target_dir(data.get('target_date'))
    threading.Thread(
        target=ltp_test.run_ltp_test_independent,
        args=(ltp_path, test_path, TEST_RESULTS_DIR, test_suites, project_path, ltp_status, ltp_lock),
        kwargs={'test_processes': test_processes, 'process_key': 'ltp', 'target_test_dir': target_dir}
    ).start()
    return jsonify({
        'message': 'LTP test started.',
        'ltp_path': ltp_path,
        'test_path': test_path,
        'test_suites': ltp_test.LTP_TEST_SUITES,
        'project_path': project_path
    }), 202

@app.route('/ltp/test-suites', methods=['GET'])
def get_ltp_test_suites():
    """Get available LTP test suites"""
    return jsonify({
        'test_suites': ltp_test.LTP_TEST_SUITES,
        'default': 'fs_perms_simple'
    })

@app.route('/ltp/status', methods=['GET'])
def get_ltp_status():
    """Get LTP test status"""
    return jsonify(ltp_status)


def _status_for_api(status_dict):
    """Return a JSON-serializable copy of status (exclude any non-serializable keys)."""
    return {k: v for k, v in status_dict.items() if not k.startswith('_')}


@app.route('/api/all-test-status', methods=['GET'])
def get_all_test_status():
    """Get current date and all test module statuses for dashboard."""
    current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return jsonify({
        'current_date': current_date,
        'dailytest': _status_for_api(dailytest_status),
        'regression': _status_for_api(regression_status),
        'coverage': _status_for_api(coverage_status),
        'fuse': _status_for_api(fuse_status),
        'fio': _status_for_api(fio_status),
        'ltp': _status_for_api(ltp_status),
    })


@app.route('/dailytest/cancel', methods=['POST'])
def cancel_dailytest():
    """Request cancel of running daily test."""
    global dailytest_cancel_requested
    if dailytest_status['status'] != 'testing':
        return jsonify({'error': 'No daily test is running.', 'current_status': _status_for_api(dailytest_status)}), 400
    dailytest_cancel_requested = True
    dailytest_status['message'] = 'Cancellation requested, stopping...'
    _kill_process(test_processes.get('dailytest'))
    return jsonify({'message': 'Cancel requested for daily test.'}), 200


@app.route('/regression/cancel', methods=['POST'])
def cancel_regression():
    """Cancel running regression test."""
    if regression_status['status'] != 'testing':
        return jsonify({'error': 'No regression test is running.', 'current_status': _status_for_api(regression_status)}), 400
    regression_status['message'] = 'Cancellation requested, stopping...'
    _kill_process(test_processes.get('regression'))
    return jsonify({'message': 'Cancel requested for regression test.'}), 200


@app.route('/coverage/cancel', methods=['POST'])
def cancel_coverage():
    """Cancel running coverage test."""
    if coverage_status['status'] != 'testing':
        return jsonify({'error': 'No coverage test is running.', 'current_status': _status_for_api(coverage_status)}), 400
    coverage_status['message'] = 'Cancellation requested, stopping...'
    _kill_process(test_processes.get('coverage'))
    return jsonify({'message': 'Cancel requested for coverage test.'}), 200


@app.route('/fuse/cancel', methods=['POST'])
def cancel_fuse():
    """Cancel running FUSE test."""
    if fuse_status['status'] != 'testing':
        return jsonify({'error': 'No FUSE test is running.', 'current_status': _status_for_api(fuse_status)}), 400
    fuse_status['message'] = 'Cancellation requested, stopping...'
    _kill_process(test_processes.get('fuse'))
    return jsonify({'message': 'Cancel requested for FUSE test.'}), 200


@app.route('/fio/cancel', methods=['POST'])
def cancel_fio():
    """Cancel running FIO test."""
    if fio_status['status'] != 'testing':
        return jsonify({'error': 'No FIO test is running.', 'current_status': _status_for_api(fio_status)}), 400
    fio_status['message'] = 'Cancellation requested, stopping...'
    _kill_process(test_processes.get('fio'))
    return jsonify({'message': 'Cancel requested for FIO test.'}), 200


@app.route('/ltp/cancel', methods=['POST'])
def cancel_ltp():
    """Cancel running LTP test."""
    if ltp_status['status'] != 'testing':
        return jsonify({'error': 'No LTP test is running.', 'current_status': _status_for_api(ltp_status)}), 400
    ltp_status['message'] = 'Cancellation requested, stopping...'
    _kill_process(test_processes.get('ltp'))
    return jsonify({'message': 'Cancel requested for LTP test.'}), 200


def get_available_test_dates():
    """List available test dates"""
    if not os.path.exists(TEST_RESULTS_DIR):
        return []
    
    dates = []
    for item in os.listdir(TEST_RESULTS_DIR):
        item_path = os.path.join(TEST_RESULTS_DIR, item)
        if os.path.isdir(item_path):
            # Extract date part (format: YYYYMMDD_HHMMSS)
            try:
                date_part = item.split('_')[0]
                time_part = item.split('_')[1] if '_' in item else "000000"
                datetime_obj = datetime.strptime(f"{date_part}_{time_part}", "%Y%m%d_%H%M%S")
                dates.append({
                    'folder': item,
                    'date': date_part,
                    'time': time_part,
                    'datetime': datetime_obj.strftime("%Y-%m-%d %H:%M:%S"),
                    'sort_key': datetime_obj
                })
            except ValueError:
                continue
    
    # Sort by time descending
    dates.sort(key=lambda x: x['sort_key'], reverse=True)
    return dates

def get_fuse_test_results(date_folder):
    """Get FUSE test results from JSON file"""
    if not _validate_date_folder(date_folder):
        return None
    
    test_dir = os.path.join(TEST_RESULTS_DIR, date_folder)
    fuse_json = os.path.join(test_dir, 'fuse-test-results.json')
    
    if not os.path.exists(fuse_json):
        print(f"FUSE test results file not found: {fuse_json}")
        return None
    
    try:
        with open(fuse_json, 'r', encoding='utf-8') as f:
            data = json.load(f)
            print(f"Successfully loaded FUSE test results from {fuse_json}, tests count: {len(data.get('tests', []))}")
            return data
    except Exception as e:
        print(f"Error reading FUSE test results from {fuse_json}: {e}")
        import traceback
        traceback.print_exc()
        return None

def get_fio_test_results(date_folder):
    """Get FIO test results from JSON file"""
    if not _validate_date_folder(date_folder):
        return None
    
    test_dir = os.path.join(TEST_RESULTS_DIR, date_folder)
    fio_json = os.path.join(test_dir, 'fio-test-results.json')
    
    if not os.path.exists(fio_json):
        print(f"FIO test results file not found: {fio_json}")
        return None
    
    try:
        with open(fio_json, 'r', encoding='utf-8') as f:
            data = json.load(f)
            print(f"Successfully loaded FIO test results from {fio_json}, tests count: {len(data.get('tests', []))}")
            return data
    except Exception as e:
        print(f"Error reading FIO test results from {fio_json}: {e}")
        import traceback
        traceback.print_exc()
        return None

def get_test_result_summary(date_folder):
    """Get test result summary for a given date"""
    if not _validate_date_folder(date_folder):
        return None
    test_dir = os.path.join(TEST_RESULTS_DIR, date_folder)
    summary_file = os.path.join(test_dir, "test_summary.json")
    coverage_json_log = os.path.join(test_dir, "coverage.json.log")
    
    # Try to load existing summary file
    summary = None
    if os.path.exists(summary_file):
        try:
            with open(summary_file, 'r', encoding='utf-8') as f:
                summary = json.load(f)
        except Exception as e:
            print(f"Error reading summary file: {e}")
            summary = None
    
    # If summary doesn't exist but coverage.json.log exists, parse it
    if summary is None and os.path.exists(coverage_json_log):
        try:
            print(f"Parsing coverage data from {coverage_json_log}")
            with open(coverage_json_log, 'r', encoding='utf-8') as f:
                content = f.read()
                # Extract JSON part (before STDERR if present)
                json_content = content.split('\n\n=== STDERR ===\n')[0].strip()
                if json_content:
                    coverage_json = json.loads(json_content)
                    
                    # Support both 'llvm-cov' and 'llvm.coverage.json.export' types
                    coverage_type = coverage_json.get('type', '')
                    if coverage_type in ('llvm-cov', 'llvm.coverage.json.export'):
                        data = coverage_json.get('data', [])
                        if data:
                            totals = data[0].get('totals', {})
                            lines_info = totals.get('lines', {})
                            functions_info = totals.get('functions', {})
                            regions_info = totals.get('regions', {})
                            
                            # For llvm.coverage.json.export, lines already has 'covered' and 'count'
                            # For llvm-cov, we need to calculate from 'count' and 'uncovered_count'
                            if coverage_type == 'llvm.coverage.json.export':
                                lines_covered = lines_info.get('covered', 0)
                                lines_total = lines_info.get('count', 0)
                                functions_covered = functions_info.get('covered', 0)
                                functions_total = functions_info.get('count', 0)
                                regions_covered = regions_info.get('covered', 0)
                                regions_total = regions_info.get('count', 0)
                            else:
                                # llvm-cov format
                                lines_covered = lines_info.get('count', 0)
                                lines_total = lines_info.get('count', 0) + lines_info.get('uncovered_count', 0)
                                functions_covered = functions_info.get('count', 0)
                                functions_total = functions_info.get('count', 0) + functions_info.get('uncovered_count', 0)
                                regions_covered = regions_info.get('count', 0)
                                regions_total = regions_info.get('count', 0) + regions_info.get('uncovered_count', 0)
                            
                            coverage_data = {
                                'lines': {
                                    'covered': lines_covered,
                                    'total': lines_total,
                                    'percent': lines_info.get('percent', 0.0)
                                },
                                'functions': {
                                    'covered': functions_covered,
                                    'total': functions_total,
                                    'percent': functions_info.get('percent', 0.0)
                                },
                                'regions': {
                                    'covered': regions_covered,
                                    'total': regions_total,
                                    'percent': regions_info.get('percent', 0.0)
                                }
                            }
                            
                            # Create summary with coverage data
                            summary = {
                                'timestamp': datetime.now().isoformat(),
                                'total_tests': 0,
                                'passed_tests': 0,
                                'failed_tests': 0,
                                'success_rate': 0,
                                'packages': [],
                                'test_cases': [],
                                'coverage': coverage_data,
                                'coverage_report_url': f"/coverage/{date_folder}/index.html"
                            }
                            
                            # Check if coverage HTML report exists
                            coverage_html_dir = os.path.join(test_dir, 'coverage')
                            if not os.path.exists(coverage_html_dir):
                                summary['coverage_report_url'] = None
                            
                            print(f"Successfully parsed coverage data from coverage.json.log")
        except Exception as e:
            print(f"Error parsing coverage.json.log: {e}")
            import traceback
            traceback.print_exc()
    
    # If summary is still None, check if test directory exists and has any test result files
    # Return a basic summary structure so the page can still display other test results (FUSE, FIO, LTP, etc.)
    if summary is None:
        # Check if test directory exists
        if os.path.exists(test_dir) and os.path.isdir(test_dir):
            # Check for any test result files
            has_fuse_results = os.path.exists(os.path.join(test_dir, 'fuse-test-results.json'))
            has_fio_results = os.path.exists(os.path.join(test_dir, 'fio-test-results.json'))
            has_ltp_results = os.path.exists(os.path.join(test_dir, 'ltp_results'))
            has_coverage = os.path.exists(os.path.join(test_dir, 'coverage'))
            
            # If any test result files exist, create a basic summary structure
            if has_fuse_results or has_fio_results or has_ltp_results or has_coverage:
                summary = {
                    'timestamp': datetime.now().isoformat(),
                    'total_tests': 0,
                    'passed_tests': 0,
                    'failed_tests': 0,
                    'success_rate': 0,
                    'packages': [],
                    'test_cases': []
                }
                print(f"Created basic summary structure for {date_folder} (no test_summary.json found, but other test results exist)")
    
    return summary

@app.route('/result', methods=['GET'])
def result():
    """Test results page (supports new JSON structure, date selection and tables)"""
    date = request.args.get('date')

    available_dates = get_available_test_dates()

    if not available_dates:
        return render_template('no_results.html')

    if not date:
        date = available_dates[0]['folder']

    test_summary = get_test_result_summary(date)

    # Compatibility with old JSON structure (modules/results) and new JSON structure (packages/test_cases)
    packages = []
    test_cases = []
    total_tests = 0
    passed_tests = 0
    failed_tests = 0
    success_rate = 0

    if test_summary:
        if 'packages' in test_summary and 'test_cases' in test_summary:
            packages = test_summary.get('packages', [])
            test_cases = test_summary.get('test_cases', [])
            total_tests = test_summary.get('total_tests', 0)
            passed_tests = test_summary.get('passed_tests', 0)
            failed_tests = test_summary.get('failed_tests', 0)
            success_rate = test_summary.get('success_rate', 0)
        else:
            # Old structure conversion
            modules = test_summary.get('modules', [])
            results = test_summary.get('results', [])
            for m in modules:
                packages.append({
                    'name': m.get('name', 'unknown'),
                    'total': m.get('total', 0),
                    'passed': m.get('passed', 0),
                    'failed': m.get('failed', 0),
                    'success_rate': m.get('success_rate', 0)
                })
            for r in results:
                test_expr = r.get('test', '')
                status = r.get('status', 'UNKNOWN')
                log = r.get('log', '')
                # Try to split: package::test_file::test_case or package::test_case
                package = 'unknown'
                test_file = 'lib'
                test_case = test_expr
                parts = test_expr.split('::')
                if len(parts) >= 1:
                    package = parts[0]
                if len(parts) == 2:
                    test_case = parts[1]
                elif len(parts) >= 3:
                    test_file = '::'.join(parts[1:-1])
                    test_case = parts[-1]
                test_cases.append({
                    'package': package,
                    'test_file': test_file,
                    'test_case': test_case,
                    'status': status,
                    'log': log
                })
            total_tests = test_summary.get('total_tests', 0)
            passed_tests = test_summary.get('passed_tests', 0)
            failed_tests = test_summary.get('failed_tests', 0)
            success_rate = test_summary.get('success_rate', 0)

    # Group test cases by package
    cases_by_package = {}
    for c in test_cases:
        pkg = c.get('package', 'unknown')
        cases_by_package.setdefault(pkg, []).append(c)

    # Further group TestFile and sort
    cases_by_package_file = {}
    for pkg_name, cases in cases_by_package.items():
        file_map = {}
        for c in cases:
            tf = c.get('test_file', 'lib')
            file_map.setdefault(tf, []).append(c)
        # Sort each file's test cases by test_case
        for tf in file_map:
            file_map[tf] = sorted(file_map[tf], key=lambda x: x.get('test_case', ''))
        # Sort files by name
        cases_by_package_file[pkg_name] = dict(sorted(file_map.items(), key=lambda item: item[0]))

    # Get FUSE and FIO test results
    fuse_results = get_fuse_test_results(date)
    fio_results = get_fio_test_results(date)
    
    # Get LTP test results from test_summary
    ltp_test = None
    if test_summary and 'ltp_test' in test_summary:
        ltp_test = test_summary['ltp_test']

    # Group FUSE and FIO tests by test_group
    fuse_tests_by_group = {}
    if fuse_results and fuse_results.get('tests'):
        for test in fuse_results['tests']:
            group = test.get('test_group', 'Unknown')
            fuse_tests_by_group.setdefault(group, []).append(test)
    
    fio_tests_by_group = {}
    if fio_results and fio_results.get('tests'):
        for test in fio_results['tests']:
            group = test.get('test_group', 'Unknown')
            fio_tests_by_group.setdefault(group, []).append(test)
    
    return render_template(
        'result.html',
        available_dates=available_dates,
        selected_date=date,
        current_date=next((d['datetime'] for d in available_dates if d['folder'] == date), 'Unknown'),
        test_summary=test_summary,
        packages=packages,
        cases_by_package=cases_by_package,
        cases_by_package_file=cases_by_package_file,
        total_tests=total_tests,
        passed_tests=passed_tests,
        failed_tests=failed_tests,
        success_rate=success_rate,
        fuse_results=fuse_results,
        fio_results=fio_results,
        fuse_tests_by_group=fuse_tests_by_group,
        fio_tests_by_group=fio_tests_by_group,
        ltp_test=ltp_test,
        current_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )

@app.route('/api/test-dates', methods=['GET'])
def api_test_dates():
    """API: List all available test dates"""
    dates = get_available_test_dates()
    return jsonify(dates)

@app.route('/api/test-result/<date>', methods=['GET'])
def api_test_result(date):
    """API: Get test result for a specific date"""
    test_summary = get_test_result_summary(date)
    if test_summary is None:
        return jsonify({'error': 'Test result not found'}), 404
    return jsonify(test_summary)

def get_available_logs(date_folder):
    """Get available log files for a given date (recursive scan)"""
    base_dir = os.path.join(TEST_RESULTS_DIR, date_folder)
    if not os.path.exists(base_dir):
        return []

    log_files = []
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            if file.endswith('.log') and file != 'daily_test.log':
                abs_path = os.path.join(root, file)
                rel_path = os.path.relpath(abs_path, base_dir)
                test_name = rel_path.replace('.log', '')
                log_files.append({
                    'filename': rel_path.replace('\\', '/'),
                    'test_name': test_name.replace('\\', '/'),
                    'display_name': test_name.replace('\\', '/')
                })

    return sorted(log_files, key=lambda x: x['test_name'])

@app.route('/logs/<date>/', defaults={'log_file': ''}, methods=['GET'])
@app.route('/logs/<date>/<path:log_file>', methods=['GET'])
def view_log(date, log_file):
    """View test logs (nested paths supported)"""
    # Validate date directory
    base_dir = os.path.join(TEST_RESULTS_DIR, date)
    if not os.path.exists(base_dir):
        return jsonify({'error': 'Date not found'}), 404

    # If not specified a specific file, redirect to result page
    if not log_file:
        return Response('<html><body><script>window.location.href="/result?date=' + date + '"</script></body></html>', mimetype='text/html')

    # Join log file path
    log_path = os.path.join(base_dir, log_file)
    if not os.path.exists(log_path):
        return jsonify({'error': 'Log file not found'}), 404

    # Read log content
    try:
        with open(log_path, 'r', encoding='utf-8') as f:
            log_content = f.read()
    except Exception as e:
        return jsonify({'error': f'Failed to read log file: {str(e)}'}), 500

    # Get available log files list
    available_logs = get_available_logs(date)
    current_log = next((log for log in available_logs if log['filename'] == log_file), None)

    # Get referrer URL from query parameter, default to main result page
    referrer_url = request.args.get('referrer', f'/result?date={date}')

    # Generate log viewer page
    return render_template(
        'log_viewer.html',
        date=date,
        log_file=log_file,
        log_content=log_content,
        available_logs=available_logs,
        current_log=current_log,
        referrer_url=referrer_url
    )

@app.route('/unit-tests/<date>', methods=['GET'])
def view_unit_tests(date):
    """View detailed unit test results"""
    # Validate date directory
    test_dir = os.path.join(TEST_RESULTS_DIR, date)
    if not os.path.exists(test_dir):
        return jsonify({'error': 'Test results not found for this date'}), 404
    
    # Get test summary
    test_summary = get_test_result_summary(date)
    if not test_summary:
        return jsonify({'error': 'Test summary not found'}), 404
    
    # Parse test data (same logic as result page)
    packages = []
    test_cases = []
    total_tests = 0
    passed_tests = 0
    failed_tests = 0
    success_rate = 0
    
    if 'packages' in test_summary and 'test_cases' in test_summary:
        packages = test_summary.get('packages', [])
        test_cases = test_summary.get('test_cases', [])
        total_tests = test_summary.get('total_tests', 0)
        passed_tests = test_summary.get('passed_tests', 0)
        failed_tests = test_summary.get('failed_tests', 0)
        success_rate = test_summary.get('success_rate', 0)
    else:
        # Old structure conversion
        modules = test_summary.get('modules', [])
        results = test_summary.get('results', [])
        for m in modules:
            packages.append({
                'name': m.get('name', 'unknown'),
                'total': m.get('total', 0),
                'passed': m.get('passed', 0),
                'failed': m.get('failed', 0),
                'success_rate': m.get('success_rate', 0)
            })
        for r in results:
            test_expr = r.get('test', '')
            status = r.get('status', 'UNKNOWN')
            log = r.get('log', '')
            parts = test_expr.split('::')
            package = 'unknown'
            test_file = 'lib'
            test_case = test_expr
            if len(parts) >= 1:
                package = parts[0]
            if len(parts) == 2:
                test_case = parts[1]
            elif len(parts) >= 3:
                test_file = '::'.join(parts[1:-1])
                test_case = parts[-1]
            test_cases.append({
                'package': package,
                'test_file': test_file,
                'test_case': test_case,
                'status': status,
                'log': log
            })
        total_tests = test_summary.get('total_tests', 0)
        passed_tests = test_summary.get('passed_tests', 0)
        failed_tests = test_summary.get('failed_tests', 0)
        success_rate = test_summary.get('success_rate', 0)
    
    # Group test cases by package
    cases_by_package = {}
    for c in test_cases:
        pkg = c.get('package', 'unknown')
        cases_by_package.setdefault(pkg, []).append(c)
    
    # Further group TestFile and sort
    cases_by_package_file = {}
    for pkg_name, cases in cases_by_package.items():
        file_map = {}
        for c in cases:
            tf = c.get('test_file', 'lib')
            file_map.setdefault(tf, []).append(c)
        for tf in file_map:
            file_map[tf] = sorted(file_map[tf], key=lambda x: x.get('test_case', ''))
        cases_by_package_file[pkg_name] = dict(sorted(file_map.items(), key=lambda item: item[0]))
    
    # Get available dates for navigation
    available_dates = get_available_test_dates()
    
    return render_template(
        'unit_test_details.html',
        date=date,
        packages=packages,
        cases_by_package_file=cases_by_package_file,
        total_tests=total_tests,
        passed_tests=passed_tests,
        failed_tests=failed_tests,
        success_rate=success_rate,
        current_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )

@app.route('/fuse-test-details/<date>', methods=['GET'])
def view_fuse_test_details(date):
    """View detailed FUSE test results"""
    # Validate date directory
    test_dir = os.path.join(TEST_RESULTS_DIR, date)
    if not os.path.exists(test_dir):
        return jsonify({'error': 'Test results not found for this date'}), 404
    
    # Get FUSE test results
    fuse_results = get_fuse_test_results(date)
    if not fuse_results:
        return jsonify({'error': 'FUSE test results not found'}), 404
    
    # Group tests by test_group
    fuse_tests_by_group = {}
    if fuse_results.get('tests'):
        for test in fuse_results['tests']:
            group = test.get('test_group', 'Unknown')
            fuse_tests_by_group.setdefault(group, []).append(test)
    
    return render_template(
        'fuse_test_details.html',
                                date=date,
        fuse_results=fuse_results,
        fuse_tests_by_group=fuse_tests_by_group,
        current_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )

@app.route('/fio-test-details/<date>', methods=['GET'])
def view_fio_test_details(date):
    """View detailed FIO test results"""
    # Validate date directory
    test_dir = os.path.join(TEST_RESULTS_DIR, date)
    if not os.path.exists(test_dir):
        return jsonify({'error': 'Test results not found for this date'}), 404
    
    # Get FIO test results
    fio_results = get_fio_test_results(date)
    if not fio_results:
        return jsonify({'error': 'FIO test results not found'}), 404
    
    # Group tests by test_group
    fio_tests_by_group = {}
    if fio_results.get('tests'):
        for test in fio_results['tests']:
            group = test.get('test_group', 'Unknown')
            fio_tests_by_group.setdefault(group, []).append(test)
    
    return render_template(
        'fio_test_details.html',
        date=date,
        fio_results=fio_results,
        fio_tests_by_group=fio_tests_by_group,
        current_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )

@app.route('/ltp-test-details/<date>', methods=['GET'])
def view_ltp_test_details(date):
    """View detailed LTP test results"""
    # Validate date directory
    test_dir = os.path.join(TEST_RESULTS_DIR, date)
    if not os.path.exists(test_dir):
        return jsonify({'error': 'Test results not found for this date'}), 404
    
    # Get test summary
    test_summary = get_test_result_summary(date)
    if not test_summary:
        return jsonify({'error': 'Test summary not found'}), 404
    
    # Get LTP test data
    ltp_test = None
    if 'ltp_test' in test_summary:
        ltp_test = test_summary['ltp_test']
    
    if not ltp_test:
        return jsonify({'error': 'LTP test results not found'}), 404
    
    return render_template(
        'ltp_test_details.html',
        date=date,
        ltp_test=ltp_test,
        current_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )

@app.route('/coverage/<date>/', defaults={'file_path': ''}, methods=['GET'])
@app.route('/coverage/<date>/<path:file_path>', methods=['GET'])
def view_coverage(date, file_path):
    """Serve coverage HTML report files"""
    # Validate date directory
    base_dir = os.path.join(TEST_RESULTS_DIR, date, 'coverage')
    if not os.path.exists(base_dir):
        return jsonify({'error': 'Coverage report not found for this date'}), 404
    
    # If no file path specified, redirect to index.html
    if not file_path:
        file_path = 'index.html'
    
    # Join file path
    file_full_path = os.path.join(base_dir, file_path)
    
    # Security check: ensure the file is within the coverage directory
    if not os.path.abspath(file_full_path).startswith(os.path.abspath(base_dir)):
        return jsonify({'error': 'Invalid file path'}), 403
    
    if not os.path.exists(file_full_path):
        return jsonify({'error': 'File not found'}), 404
    
    # Determine content type
    content_type = 'text/html'
    if file_path.endswith('.css'):
        content_type = 'text/css'
    elif file_path.endswith('.js'):
        content_type = 'application/javascript'
    elif file_path.endswith('.png'):
        content_type = 'image/png'
    elif file_path.endswith('.svg'):
        content_type = 'image/svg+xml'
    elif file_path.endswith('.json'):
        content_type = 'application/json'
    
    # Read and return file
    try:
        if file_path.endswith(('.png', '.svg', '.ico')):
            # Binary files
            with open(file_full_path, 'rb') as f:
                return Response(f.read(), mimetype=content_type)
        else:
            # Text files
            with open(file_full_path, 'r', encoding='utf-8') as f:
                content = f.read()
                # Fix relative paths in HTML to work with our routing
                if file_path.endswith('.html'):
                    # Replace relative paths with absolute paths (only if they don't start with / or http)
                    # Fix href attributes (support both single and double quotes)
                    # Match href='...' where content doesn't start with / or http
                    content = re.sub(r"href='(?![/h])([^']*)'", rf"href='/coverage/{date}/\1'", content)
                    content = re.sub(r'href="(?![/h])([^"]*)"', rf'href="/coverage/{date}/\1"', content)
                    # Fix src attributes (support both single and double quotes)
                    content = re.sub(r"src='(?![/h])([^']*)'", rf"src='/coverage/{date}/\1'", content)
                    content = re.sub(r'src="(?![/h])([^"]*)"', rf'src="/coverage/{date}/\1"', content)
                return Response(content, mimetype=content_type)
    except Exception as e:
        return jsonify({'error': f'Failed to read file: {str(e)}'}), 500

def parse_arguments():
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser(
        description='Curvine Build Server - Build & Test Server',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 build-server.py                           # Use default paths
  python3 build-server.py --project-path /path/to/curvine
  python3 build-server.py -p /home/user/curvine-project
        """
    )
    
    parser.add_argument(
        '--project-path', '-p',
        type=str,
        default=None,
        help='Path to the curvine project (defaults to current working directory)'
    )
    
    parser.add_argument(
        '--port',
        type=int,
        default=5002,
        help='Server port (default 5002)'
    )
    
    parser.add_argument(
        '--host',
        type=str,
        default='0.0.0.0',
        help='Server host address (default 0.0.0.0)'
    )
    
    parser.add_argument(
        '--results-dir', '-r',
        type=str,
        default=None,
        help='Test results directory (defaults to <project_path>/result)'
    )
    
    return parser.parse_args()


def validate_project_path(project_path):
    """Validate project path"""
    if not os.path.exists(project_path):
        print(f"Error: Specified project path does not exist: {project_path}")
        sys.exit(1)
    
    if not os.path.isdir(project_path):
        print(f"Error: Specified path is not a directory: {project_path}")
        sys.exit(1)
    
    # Check if it's a curvine project
    cargo_toml = os.path.join(project_path, 'Cargo.toml')
    if not os.path.exists(cargo_toml):
        print(f"Warning: Specified path may not be a curvine project (Cargo.toml not found): {project_path}")
    
    # Check if build/tests directory exists (preferred location)
    build_tests_dir = os.path.join(project_path, 'build', 'tests')
    if not os.path.exists(build_tests_dir):
        print(f"Warning: build/tests directory not found in project path: {build_tests_dir}")
    
    # Check if scripts directory exists (legacy location)
    scripts_dir = os.path.join(project_path, 'scripts')
    if not os.path.exists(scripts_dir):
        print(f"Info: scripts directory not found in project path (legacy location): {scripts_dir}")
    
    return project_path

if __name__ == '__main__':
    # Parse command-line arguments
    args = parse_arguments()
    
    # Set project path
    if args.project_path:
        PROJECT_PATH = validate_project_path(args.project_path)
        print(f"Using specified project path: {PROJECT_PATH}")
    else:
        PROJECT_PATH = os.getcwd()
        print(f"Using current working directory as project path: {PROJECT_PATH}")
    
    # Set test results directory: prefer argument, otherwise default <project_path>/result
    TEST_RESULTS_DIR = args.results_dir if args.results_dir else os.path.join(PROJECT_PATH, 'result')
    print(f"Using test results directory: {TEST_RESULTS_DIR}")
    
    # Auto-detect script path
    script_path = test_utils.find_script_path(project_path=PROJECT_PATH)
    if script_path:
        print(f"Script path auto-detected: {script_path}")
    else:
        print("Warning: Could not auto-detect daily_regression_test.sh")
        print("Please ensure the script is in one of the following locations:")
        print("  - build/tests/ subdirectory (preferred)")
        print("  - Current directory")
        print("  - scripts subdirectory (legacy)")
        print("  - In system PATH")
        print("  - /usr/local/bin, /usr/bin, /opt/curvine/bin, /home/curvine/bin")
    
    # Start server
    print(f"Starting server: http://{args.host}:{args.port}")
    print(f"Project path: {PROJECT_PATH}")
    print(f"Results directory: {TEST_RESULTS_DIR}")
    if script_path:
        print(f"Script path: {script_path}")
    app.run(host=args.host, port=args.port)
