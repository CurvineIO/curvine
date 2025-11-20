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


def run_coverage_test(project_path, test_results_dir, test_dir=None, update_status=None):
    """Run coverage test using cargo llvm-cov and integrate results
    
    Args:
        project_path: Path to the project root
        test_results_dir: Path to test results directory
        test_dir: Test directory to store results (if None, creates new timestamped dir)
        update_status: Status dict to update (if None, uses coverage_status)
        
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
            
            # Run coverage test with JSON output for parsing
            print("Running coverage test with JSON output...")
            coverage_json_log = os.path.join(test_dir, 'coverage.json.log')
            coverage_process = subprocess.Popen(
                ['cargo', 'llvm-cov', 'test', '--workspace', '--json'],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True
            )
            
            stdout, stderr = coverage_process.communicate()
            
            # Print stderr for debugging
            if stderr:
                print(f"Coverage test stderr: {stderr}")
            
            # Save coverage JSON output
            with open(coverage_json_log, 'w', encoding='utf-8') as f:
                f.write(stdout)
                if stderr:
                    f.write("\n\n=== STDERR ===\n")
                    f.write(stderr)
            
            if coverage_process.returncode != 0:
                error_msg = f"Coverage test failed with return code {coverage_process.returncode}. Check {coverage_json_log} for details."
                print(f"Error: {error_msg}")
                if status:
                    status['status'] = 'failed'
                    status['message'] = error_msg
                return False
            
            # Parse coverage data from JSON
            coverage_data = None
            try:
                if not stdout.strip():
                    print("Warning: Coverage test produced no output")
                    if status:
                        status['status'] = 'failed'
                        status['message'] = 'Coverage test produced no output'
                    return False
                
                coverage_json = json.loads(stdout)
                coverage_type = coverage_json.get('type', 'unknown')
                print(f"Parsed coverage JSON, type: {coverage_type}")
                
                # Support both 'llvm-cov' and 'llvm.coverage.json.export' types
                if coverage_type in ('llvm-cov', 'llvm.coverage.json.export'):
                    data = coverage_json.get('data', [])
                    if not data:
                        print("Warning: Coverage JSON has no data")
                        if status:
                            status['status'] = 'failed'
                            status['message'] = 'Coverage JSON has no data'
                        return False
                    
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
                    print(f"Parsed coverage data successfully")
            except json.JSONDecodeError as e:
                error_msg = f"Failed to parse coverage JSON: {e}. Output: {stdout[:500]}"
                print(f"Error: {error_msg}")
                if status:
                    status['status'] = 'failed'
                    status['message'] = error_msg
                return False
            except (KeyError, IndexError) as e:
                error_msg = f"Failed to extract coverage data from JSON: {e}"
                print(f"Error: {error_msg}")
                if status:
                    status['status'] = 'failed'
                    status['message'] = error_msg
                return False
            
            # Generate HTML coverage report
            print("Generating HTML coverage report...")
            html_process = subprocess.Popen(
                ['cargo', 'llvm-cov', 'test', '--workspace', '--html'],
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


def run_coverage_test_independent(project_path, test_results_dir, coverage_status, coverage_lock):
    """Run coverage test independently in a thread
    
    Args:
        project_path: Path to the project root
        test_results_dir: Path to test results directory
        coverage_status: Status dictionary to update
        coverage_lock: Lock object for thread safety
    """
    with coverage_lock:
        coverage_status['status'] = 'testing'
        coverage_status['message'] = 'Starting coverage test...'
        coverage_status['test_dir'] = ''
        coverage_status['report_url'] = ''
        
        try:
            success = run_coverage_test(project_path, test_results_dir, test_dir=None, update_status=coverage_status)
            if not success:
                coverage_status['status'] = 'failed'
        except Exception as e:
            coverage_status['status'] = 'failed'
            coverage_status['message'] = f'An error occurred: {str(e)}'

