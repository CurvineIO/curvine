"""LTP test module for build-server"""
import subprocess
import os
import json
import re
import shutil
import time
import sys
from datetime import datetime
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
from utils import cluster_utils
from utils import test_utils

# LTP test suites list (can be extended)
LTP_TEST_SUITES = [
    'fs_perms_simple',
    'cv-fs',
]


def run_ltp_test(ltp_path='/opt/ltp', test_path='/curvine-fuse', test_results_dir=None, test_dir=None, test_suites=None, project_path=None, ltp_status=None):
    """Run LTP test
    
    Args:
        ltp_path: Path to LTP installation (default: /opt/ltp)
        test_path: Target test path (default: /curvine-fuse)
        test_results_dir: Path to test results directory
        test_dir: Test directory to store results (if None, uses latest or creates new)
        test_suites: List of test suites to run (default: LTP_TEST_SUITES)
        project_path: Path to the project root (required for cluster preparation)
        ltp_status: Status dictionary to update (optional)
        
    Returns:
        bool: True if test completed successfully, False otherwise
    """
    try:
        print("Starting LTP test...")
        if ltp_status:
            ltp_status['message'] = 'Starting LTP test...'
        
        # Default test suites - use LTP_TEST_SUITES if not specified
        if test_suites is None:
            test_suites = LTP_TEST_SUITES
        
        # Validate test suites
        for suite in test_suites:
            if suite not in LTP_TEST_SUITES:
                print(f"Warning: Test suite '{suite}' is not in the known list: {LTP_TEST_SUITES}")
        
        # Check if LTP path exists
        if not os.path.exists(ltp_path):
            error_msg = f"LTP path does not exist: {ltp_path}"
            print(f"Error: {error_msg}")
            if ltp_status:
                ltp_status['status'] = 'failed'
                ltp_status['message'] = error_msg
            return False
        
        # Check if runltp script exists
        runltp_script = os.path.join(ltp_path, 'runltp')
        if not os.path.exists(runltp_script):
            error_msg = f"runltp script not found at: {runltp_script}"
            print(f"Error: {error_msg}")
            if ltp_status:
                ltp_status['status'] = 'failed'
                ltp_status['message'] = error_msg
            return False
        
        # Ensure cluster is ready before testing
        if project_path:
            if ltp_status:
                ltp_status['message'] = 'Ensuring cluster is ready...'
            print("Ensuring cluster is ready...")
            success, error_msg = cluster_utils.ensure_cluster_ready(project_path, test_path)
            if not success:
                if ltp_status:
                    ltp_status['status'] = 'failed'
                    ltp_status['message'] = f"Failed to ensure cluster is ready: {error_msg}"
                return False
        else:
            print("Warning: project_path not provided, checking mount point only...")
            # If project_path not provided, at least check mount point
            is_mounted, mount_error = cluster_utils.check_mount_point(test_path)
            if not is_mounted:
                error_msg = f"Mount point check failed: {mount_error}"
                print(f"Error: {error_msg}")
                if ltp_status:
                    ltp_status['status'] = 'failed'
                    ltp_status['message'] = error_msg
                return False
        
        print(f"Mount point {test_path} is properly mounted")
        
        # Wait 10 seconds to ensure mount point is fully ready
        if ltp_status:
            ltp_status['message'] = 'Mount point verified. Waiting 10 seconds for mount to stabilize...'
        print("Waiting 10 seconds for mount point to stabilize...")
        time.sleep(10)
        print("Mount point stabilization wait completed. Starting LTP tests...")
        
        # Determine test directory
        if test_dir is None:
            latest_dir = test_utils.find_latest_test_dir(test_results_dir) if test_results_dir else None
            if latest_dir:
                test_dir = latest_dir
                print(f"Using latest test directory: {test_dir}")
            else:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                test_dir = os.path.join(test_results_dir, timestamp)
                os.makedirs(test_dir, exist_ok=True)
                print(f"Created new test directory: {test_dir}")
        
        # Create LTP results directory in test_dir
        ltp_results_dir = os.path.join(test_dir, 'ltp_results')
        os.makedirs(ltp_results_dir, exist_ok=True)
        
        # Change to LTP directory
        original_cwd = os.getcwd()
        os.chdir(ltp_path)
        
        try:
            # Normalize test suites: remove ./ prefix if present
            normalized_suites = []
            if test_suites:
                for suite in test_suites:
                    # Remove ./ prefix if already present
                    clean_suite = suite[2:] if suite.startswith('./') else suite
                    normalized_suites.append(clean_suite)
            else:
                normalized_suites = ['fs_perms_simple']
            
            # Run each test suite sequentially
            test_results = []
            all_passed = True
            
            for idx, suite in enumerate(normalized_suites):
                suite_num = idx + 1
                total_suites = len(normalized_suites)
                
                # Generate log file name for this test suite: ltp_${test_suite}.log
                log_file = os.path.join(test_dir, f'ltp_{suite}.log')
                
                # Format test suite for runltp command: with ./ prefix
                test_suite_str = f'./{suite}'
                
                # Update status message
                if ltp_status:
                    ltp_status['message'] = f'Running LTP test suite {suite_num}/{total_suites}: {suite}...'
                print(f"\n{'='*80}")
                print(f"Running LTP test suite {suite_num}/{total_suites}: {suite}")
                print(f"Command: ./runltp -d {test_path} -f {test_suite_str} -l {log_file}")
                print(f"{'='*80}\n")
                
                process = subprocess.Popen(
                    ['./runltp', '-d', test_path, '-f', test_suite_str, '-l', log_file],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    universal_newlines=True
                )
                
                # Stream output in real time
                for line in process.stdout:
                    print(line, end='')
                
                # Wait for process to finish
                process.wait()
                
                # Read stderr
                stderr_output = process.stderr.read()
                if stderr_output:
                    print(f"LTP test stderr for {suite}: {stderr_output}")
                
                # Parse log file to count PASS and FAILED tests based on LTP log format
                # Format: tag=test_name stime=... exit=exited stat=0/1 ...
                # stat=0 means PASS, stat!=0 means FAIL
                passed_count = 0
                failed_count = 0
                test_cases = []
                
                if os.path.exists(log_file):
                    try:
                        with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
                            log_content = f.read()
                            
                        # Parse LTP log file format
                        # Format example:
                        # startup='Wed Nov 19 18:03:38 2025'
                        # tag=fs_perms01 stime=1763546618 dur=0 exit=exited stat=0 core=no cu=0 cs=0
                        # tag=fs_perms10 stime=1763546619 dur=0 exit=exited stat=1 core=no cu=0 cs=0
                        # If log contains multiple runs, only parse the last run
                        
                        lines = log_content.split('\n')
                        test_results_map = {}  # Map test_name -> status (to handle multiple runs, keep last)
                        
                        # Find the last startup line to identify the last run
                        last_startup_idx = -1
                        for i in range(len(lines) - 1, -1, -1):
                            if lines[i].strip().startswith("startup="):
                                last_startup_idx = i
                                break
                        
                        # Parse lines after the last startup (or all lines if no startup found)
                        parse_start_idx = last_startup_idx + 1 if last_startup_idx >= 0 else 0
                        
                        for line in lines[parse_start_idx:]:
                            line = line.strip()
                            if not line:
                                continue
                            
                            # Skip startup lines (shouldn't appear after last_startup_idx, but just in case)
                            if line.startswith("startup="):
                                continue
                            
                            # Parse tag=test_name ... stat=0/1 format
                            # Extract tag and stat values
                            tag_match = re.search(r'tag=([^\s]+)', line)
                            stat_match = re.search(r'stat=(\d+)', line)
                            
                            if tag_match and stat_match:
                                test_name = tag_match.group(1)
                                stat_value = int(stat_match.group(1))
                                
                                # Store result (will overwrite if same test appears multiple times)
                                test_results_map[test_name] = 'PASSED' if stat_value == 0 else 'FAILED'
                            else:
                                # Fallback: try to find PASS/FAIL patterns
                                pass_match = re.search(r'\bPASS\b', line, re.IGNORECASE)
                                fail_match = re.search(r'\bFAIL\b', line, re.IGNORECASE)
                                
                                if pass_match or fail_match:
                                    # Try to extract test name
                                    test_name_match = re.search(r'^([^\s:]+(?::[^\s:]+)*)', line)
                                    if test_name_match:
                                        test_name = test_name_match.group(1).strip()
                                        if test_name not in test_results_map and test_name not in ['startup']:
                                            if pass_match:
                                                test_results_map[test_name] = 'PASSED'
                                            elif fail_match:
                                                test_results_map[test_name] = 'FAILED'
                        
                        # Convert map to list and count
                        for test_name, status in test_results_map.items():
                            test_cases.append({
                                'name': test_name,
                                'status': status
                            })
                            if status == 'PASSED':
                                passed_count += 1
                            else:
                                failed_count += 1
                        
                        # Sort test cases by name for consistent display
                        test_cases.sort(key=lambda x: x['name'])
                        
                        # If no tests found by parsing, use return code as indicator
                        if passed_count == 0 and failed_count == 0:
                            if process.returncode == 0:
                                passed_count = 1
                                test_cases.append({
                                    'name': suite,
                                    'status': 'PASSED'
                                })
                            else:
                                failed_count = 1
                                test_cases.append({
                                    'name': suite,
                                    'status': 'FAILED'
                                })
                        
                        print(f"Parsed log file: {passed_count} PASS, {failed_count} FAIL (total: {passed_count + failed_count} test cases)")
                    except Exception as e:
                        print(f"Warning: Failed to parse log file {log_file}: {e}")
                        import traceback
                        traceback.print_exc()
                        # Fallback: use return code
                        if process.returncode == 0:
                            passed_count = 1
                        else:
                            failed_count = 1
                
                # Record result
                suite_passed = process.returncode == 0
                test_results.append({
                    'suite': suite,
                    'status': 'passed' if suite_passed else 'failed',
                    'return_code': process.returncode,
                    'log_file': os.path.relpath(log_file, test_dir),
                    'passed_count': passed_count,
                    'failed_count': failed_count,
                    'total_count': passed_count + failed_count,
                    'test_cases': test_cases
                })
                
                if suite_passed:
                    print(f"\n✓ Test suite '{suite}' completed successfully")
                else:
                    print(f"\n✗ Test suite '{suite}' failed with return code {process.returncode}")
                    all_passed = False
                    # Continue with next test suite instead of stopping
                
                # Copy LTP results from /opt/ltp/results to test_dir/ltp_results after each test
                ltp_results_source = os.path.join(ltp_path, 'results')
                if os.path.exists(ltp_results_source):
                    # Copy all files from results directory
                    for item in os.listdir(ltp_results_source):
                        source_item = os.path.join(ltp_results_source, item)
                        dest_item = os.path.join(ltp_results_dir, item)
                        if os.path.isfile(source_item):
                            shutil.copy2(source_item, dest_item)
                        elif os.path.isdir(source_item):
                            if os.path.exists(dest_item):
                                shutil.rmtree(dest_item)
                            shutil.copytree(source_item, dest_item)
                
                # Wait a bit between test suites
                if idx < len(normalized_suites) - 1:
                    print(f"\nWaiting 2 seconds before next test suite...")
                    time.sleep(2)
            
            # Calculate LTP test statistics
            ltp_total_passed = sum(r.get('passed_count', 0) for r in test_results)
            ltp_total_failed = sum(r.get('failed_count', 0) for r in test_results)
            ltp_total_tests = ltp_total_passed + ltp_total_failed
            ltp_success_rate = (ltp_total_passed / ltp_total_tests * 100) if ltp_total_tests > 0 else 0
            
            # Update test_summary.json with LTP test info
            summary = test_utils.ensure_test_summary(test_dir)
            
            summary['ltp_test'] = {
                'status': 'completed' if all_passed else 'completed_with_failures',
                'test_suites': normalized_suites,
                'test_results': test_results,
                'results_dir': 'ltp_results',
                'total_tests': ltp_total_tests,
                'passed_tests': ltp_total_passed,
                'failed_tests': ltp_total_failed,
                'success_rate': round(ltp_success_rate, 2)
            }
            
            test_utils.update_test_summary(test_dir, summary)
            
            if all_passed:
                if ltp_status:
                    ltp_status['status'] = 'completed'
                    ltp_status['message'] = f'All LTP test suites completed successfully ({len(normalized_suites)} suites).'
                print(f"\n✓ All {len(normalized_suites)} test suite(s) completed successfully")
            else:
                if ltp_status:
                    ltp_status['status'] = 'completed'
                    ltp_status['message'] = f'LTP test completed with some failures ({len(normalized_suites)} suites).'
                print(f"\n⚠ LTP test completed with some failures")
                # Print summary
                for result in test_results:
                    status_icon = '✓' if result['status'] == 'passed' else '✗'
                    print(f"  {status_icon} {result['suite']}: {result['status']}")
            
            if ltp_status:
                ltp_status['test_dir'] = test_dir
                ltp_status['report_url'] = f"/result?date={os.path.basename(test_dir)}"
            print(f"LTP test results saved to: {ltp_results_dir}")
            return True
                
        finally:
            os.chdir(original_cwd)
            
    except Exception as e:
        error_msg = f"Error running LTP test: {e}"
        print(error_msg)
        import traceback
        traceback.print_exc()
        if ltp_status:
            ltp_status['status'] = 'failed'
            ltp_status['message'] = error_msg
        return False


def run_ltp_test_independent(ltp_path, test_path, test_results_dir, test_suites, project_path, ltp_status, ltp_lock):
    """Run LTP test independently in a thread
    
    Args:
        ltp_path: Path to LTP installation
        test_path: Target test path
        test_results_dir: Path to test results directory
        test_suites: List of test suites to run
        project_path: Path to the project root
        ltp_status: Status dictionary to update
        ltp_lock: Lock object for thread safety
    """
    with ltp_lock:
        ltp_status['status'] = 'testing'
        ltp_status['message'] = 'Starting LTP test...'
        ltp_status['test_dir'] = ''
        ltp_status['report_url'] = ''
        
        try:
            success = run_ltp_test(
                ltp_path=ltp_path,
                test_path=test_path,
                test_results_dir=test_results_dir,
                test_dir=None,
                test_suites=test_suites,
                project_path=project_path,
                ltp_status=ltp_status
            )
            if not success:
                ltp_status['status'] = 'failed'
        except Exception as e:
            ltp_status['status'] = 'failed'
            ltp_status['message'] = f'An error occurred: {str(e)}'

