"""Coverage test module for build-server"""
import subprocess
import os
import json
import shutil
import sys
from datetime import datetime
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
from utils import test_utils


def run_coverage_test(project_path, test_results_dir, test_dir=None, update_status=None, process_holder=None, process_key='coverage'):
    """Run coverage test using cargo llvm-cov and integrate results
    
    Args:
        project_path: Path to the project root
        test_results_dir: Path to test results directory
        test_dir: Test directory to store results (if None, creates new timestamped dir)
        update_status: Status dict to update (if None, uses coverage_status)
        process_holder: Optional dict to register current process for cancel (keyed by process_key)
        process_key: Key in process_holder for this test's process
        
    Returns:
        bool: True if successful, False otherwise
    """
    status = update_status
    
    try:
        print("Starting coverage test...")
        if status:
            status['message'] = 'Running coverage test...'
        
        # Change to project directory
        original_cwd = os.getcwd()
        os.chdir(project_path)
        
        # If test_dir is not provided, try to use latest test directory or create a new timestamped directory
        if test_dir is None:
            # Try to find the latest test directory first
            latest_dir = test_utils.find_latest_test_dir(test_results_dir)
            if latest_dir:
                test_dir = latest_dir
                print(f"Using latest test directory: {test_dir}")
            else:
                # Create a new timestamped directory (same format as unit tests)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                test_dir = os.path.join(test_results_dir, timestamp)
                os.makedirs(test_dir, exist_ok=True)
                print(f"Created new test directory: {test_dir}")
        
        try:
            # Check if cargo llvm-cov is available
            check_process = subprocess.Popen(
                ['cargo', 'llvm-cov', '--version'],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True
            )
            check_process.wait()
            if check_process.returncode != 0:
                error_msg = "cargo llvm-cov is not installed. Please install it with: cargo install cargo-llvm-cov"
                print(f"Error: {error_msg}")
                if status:
                    status['status'] = 'failed'
                    status['message'] = error_msg
                return False
            
            # Run coverage test with JSON output for parsing.
            # --ignore-run-fail: emit coverage JSON even when some unit tests fail,
            # so we can always display coverage metrics alongside test failures.
            print("Running coverage test with JSON output...")
            coverage_json_log = os.path.join(test_dir, 'coverage.json.log')
            coverage_process = subprocess.Popen(
                ['cargo', 'llvm-cov', '--ignore-run-fail', 'test', '--workspace', '--json'],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                start_new_session=True
            )
            if process_holder is not None:
                process_holder[process_key] = coverage_process
            try:
                stdout, stderr = coverage_process.communicate()
            finally:
                if process_holder is not None:
                    process_holder[process_key] = None
            
            # Print stderr for debugging
            if stderr:
                print(f"Coverage test stderr: {stderr[:2000]}")

            # Save raw output for diagnostics regardless of exit code
            with open(coverage_json_log, 'w', encoding='utf-8') as f:
                f.write(stdout)
                if stderr:
                    f.write("\n\n=== STDERR ===\n")
                    f.write(stderr)

            # Handle cancellation immediately — no coverage data to salvage
            if coverage_process.returncode < 0:
                if status:
                    status['status'] = 'cancelled'
                    status['message'] = 'Coverage test was cancelled.'
                return False

            # cargo llvm-cov outputs valid JSON to stdout even when unit tests fail.
            # Try to parse coverage data regardless of exit code so we can show
            # coverage metrics even in runs where some tests fail.
            coverage_data = None
            unit_tests_failed = coverage_process.returncode != 0

            if stdout.strip():
                try:
                    coverage_json = json.loads(stdout)
                    coverage_type = coverage_json.get('type', 'unknown')
                    print(f"Parsed coverage JSON, type: {coverage_type}")

                    if coverage_type in ('llvm-cov', 'llvm.coverage.json.export'):
                        data = coverage_json.get('data', [])
                        if data:
                            totals = data[0].get('totals', {})
                            lines_info = totals.get('lines', {})
                            functions_info = totals.get('functions', {})
                            regions_info = totals.get('regions', {})

                            if coverage_type == 'llvm.coverage.json.export':
                                lines_covered = lines_info.get('covered', 0)
                                lines_total = lines_info.get('count', 0)
                                functions_covered = functions_info.get('covered', 0)
                                functions_total = functions_info.get('count', 0)
                                regions_covered = regions_info.get('covered', 0)
                                regions_total = regions_info.get('count', 0)
                            else:
                                lines_covered = lines_info.get('count', 0)
                                lines_total = lines_info.get('count', 0) + lines_info.get('uncovered_count', 0)
                                functions_covered = functions_info.get('count', 0)
                                functions_total = functions_info.get('count', 0) + functions_info.get('uncovered_count', 0)
                                regions_covered = regions_info.get('count', 0)
                                regions_total = regions_info.get('count', 0) + regions_info.get('uncovered_count', 0)

                            coverage_data = {
                                'lines': {'covered': lines_covered, 'total': lines_total, 'percent': lines_info.get('percent', 0.0)},
                                'functions': {'covered': functions_covered, 'total': functions_total, 'percent': functions_info.get('percent', 0.0)},
                                'regions': {'covered': regions_covered, 'total': regions_total, 'percent': regions_info.get('percent', 0.0)},
                            }
                            print("Parsed coverage data successfully")
                        else:
                            print("Warning: Coverage JSON has no data section")
                except json.JSONDecodeError as e:
                    print(f"Warning: Failed to parse coverage JSON: {e}. Output head: {stdout[:300]}")
                except (KeyError, IndexError) as e:
                    print(f"Warning: Failed to extract coverage data: {e}")
            else:
                print("Warning: Coverage test produced no stdout output")

            # If we couldn't get any coverage data at all, it's a real failure
            # (build error, llvm-cov not installed correctly, etc.)
            if coverage_data is None:
                error_msg = (
                    f"Coverage data could not be collected (exit code {coverage_process.returncode}). "
                    f"Check {coverage_json_log} for details."
                )
                print(f"Error: {error_msg}")
                if status:
                    status['status'] = 'failed'
                    status['message'] = error_msg
                return False
            
            # Generate HTML coverage report (also with --ignore-run-fail so the
            # HTML output is always produced even when some unit tests fail).
            print("Generating HTML coverage report...")
            html_process = subprocess.Popen(
                ['cargo', 'llvm-cov', '--ignore-run-fail', 'test', '--workspace', '--html'],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True
            )
            html_stdout, html_stderr = html_process.communicate()
            
            if html_process.returncode != 0:
                print(f"Warning: HTML report generation failed: {html_stderr}")
            
            # Copy HTML coverage report to test directory
            coverage_html_source = os.path.join(project_path, 'target', 'llvm-cov', 'html')
            coverage_html_dest = os.path.join(test_dir, 'coverage')
            
            if os.path.exists(coverage_html_source):
                if os.path.exists(coverage_html_dest):
                    shutil.rmtree(coverage_html_dest)
                shutil.copytree(coverage_html_source, coverage_html_dest)
                print(f"Coverage HTML report copied to: {coverage_html_dest}")
            else:
                print(f"Warning: Coverage HTML report not found at: {coverage_html_source}")
            
            # Update or create test_summary.json with coverage data
            if coverage_data:
                # Load existing summary if it exists, otherwise create new
                summary = test_utils.ensure_test_summary(test_dir)
                
                summary['coverage'] = coverage_data
                summary['coverage_report_url'] = f"/coverage/{os.path.basename(test_dir)}/index.html"
                
                test_utils.update_test_summary(test_dir, summary)
                
                print(f"Coverage data added to test_summary.json")
                print(f"  Lines: {coverage_data['lines']['covered']}/{coverage_data['lines']['total']} ({coverage_data['lines']['percent']:.2f}%)")
                print(f"  Functions: {coverage_data['functions']['covered']}/{coverage_data['functions']['total']} ({coverage_data['functions']['percent']:.2f}%)")
                print(f"  Regions: {coverage_data['regions']['covered']}/{coverage_data['regions']['total']} ({coverage_data['regions']['percent']:.2f}%)")
            
            if status:
                if unit_tests_failed:
                    status['status'] = 'completed'
                    status['message'] = 'Coverage collected (some unit tests failed — see regression report for details).'
                else:
                    status['status'] = 'completed'
                    status['message'] = 'Coverage test completed successfully.'
                status['test_dir'] = test_dir
                status['report_url'] = f"/result?date={os.path.basename(test_dir)}"

            return True
            
        finally:
            os.chdir(original_cwd)
            
    except Exception as e:
        error_msg = f"Error running coverage test: {e}"
        print(error_msg)
        import traceback
        traceback.print_exc()
        if status:
            status['status'] = 'failed'
            status['message'] = error_msg
        return False


def run_coverage_test_independent(project_path, test_results_dir, coverage_status, coverage_lock,
                                   test_processes=None, process_key='coverage', target_test_dir=None):
    """Run coverage test independently in a thread.

    Args:
        project_path: Path to the project root
        test_results_dir: Path to test results directory
        coverage_status: Status dictionary to update
        coverage_lock: Lock object for thread safety
        test_processes: Optional dict to register current process for cancel (keyed by process_key)
        process_key: Key in test_processes for this test's process
        target_test_dir: If set, write results to this specific directory
    """
    with coverage_lock:
        coverage_status['status'] = 'testing'
        coverage_status['message'] = 'Starting coverage test...'
        coverage_status['test_dir'] = ''
        coverage_status['report_url'] = ''

        try:
            if target_test_dir:
                os.makedirs(target_test_dir, exist_ok=True)
            success = run_coverage_test(
                project_path, test_results_dir, test_dir=target_test_dir, update_status=coverage_status,
                process_holder=test_processes, process_key=process_key
            )
            if not success:
                coverage_status['status'] = 'failed'
        except Exception as e:
            coverage_status['status'] = 'failed'
            coverage_status['message'] = f'An error occurred: {str(e)}'

