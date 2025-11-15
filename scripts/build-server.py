from flask import Flask, request, jsonify, render_template_string, Response
import subprocess
import threading
import os
import json
import glob
import sys
import argparse
import shutil
import re
from datetime import datetime

app = Flask(__name__)

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

# Create lock objects
build_lock = threading.Lock()
dailytest_lock = threading.Lock()
coverage_lock = threading.Lock()
regression_lock = threading.Lock()

# Project path (global)
PROJECT_PATH = None

# Test results directory (global)
TEST_RESULTS_DIR = None

def run_build_script(date, commit):
    global build_status
    with build_lock:  # Ensure only one build instance at a time
        build_status['status'] = 'building'
        build_status['message'] = f'Starting build for date: {date}, commit: {commit}'

        try:
            # Use Popen to run build script and stream logs
            process = subprocess.Popen(
                ['./build.sh', date, commit],
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

def run_coverage_test(project_path, test_dir=None, update_status=None):
    """Run coverage test using cargo llvm-cov and integrate results
    
    Args:
        project_path: Path to the project root
        test_dir: Test directory to store results (if None, creates new timestamped dir)
        update_status: Status dict to update (if None, uses coverage_status)
    """
    status = update_status if update_status else coverage_status
    
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
            latest_dir = find_latest_test_dir()
            if latest_dir:
                test_dir = latest_dir
                print(f"Using latest test directory: {test_dir}")
            else:
                # Create a new timestamped directory (same format as unit tests)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                test_dir = os.path.join(TEST_RESULTS_DIR, timestamp)
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
            summary_file = os.path.join(test_dir, 'test_summary.json')
            if coverage_data:
                # Load existing summary if it exists, otherwise create new
                if os.path.exists(summary_file):
                    with open(summary_file, 'r', encoding='utf-8') as f:
                        summary = json.load(f)
                else:
                    summary = {
                        'timestamp': datetime.now().isoformat(),
                        'total_tests': 0,
                        'passed_tests': 0,
                        'failed_tests': 0,
                        'success_rate': 0,
                        'packages': [],
                        'test_cases': []
                    }
                
                summary['coverage'] = coverage_data
                summary['coverage_report_url'] = f"/coverage/{os.path.basename(test_dir)}/index.html"
                
                with open(summary_file, 'w', encoding='utf-8') as f:
                    json.dump(summary, f, indent=2, ensure_ascii=False)
                
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

def find_latest_test_dir():
    """Find the latest test directory"""
    if not os.path.exists(TEST_RESULTS_DIR):
        return None
    
    test_dirs = []
    for item in os.listdir(TEST_RESULTS_DIR):
        item_path = os.path.join(TEST_RESULTS_DIR, item)
        if os.path.isdir(item_path):
            # Check if it's a timestamp directory (YYYYMMDD_HHMMSS)
            if re.match(r'^\d{8}_\d{6}$', item):
                test_dirs.append((item, os.path.getmtime(item_path)))
    
    if not test_dirs:
        return None
    
    # Sort by modification time, get latest
    test_dirs.sort(key=lambda x: x[1], reverse=True)
    return os.path.join(TEST_RESULTS_DIR, test_dirs[0][0])

def run_regression_test_independent(project_path):
    """Run regression test independently in a thread"""
    global regression_status
    with regression_lock:
        regression_status['status'] = 'testing'
        regression_status['message'] = 'Starting regression test...'
        regression_status['test_dir'] = ''
        regression_status['report_url'] = ''
        
        try:
            # Auto-detect script path
            script_path = find_script_path()
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
                [script_path, project_path, TEST_RESULTS_DIR],
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
            latest_test_dir = find_latest_test_dir()
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

def run_dailytest_script():
    """Run both regression test and coverage test in sequence"""
    global dailytest_status
    with dailytest_lock:  # Ensure only one test instance at a time
        dailytest_status['status'] = 'testing'
        dailytest_status['message'] = 'Starting daily test (regression + coverage)...'
        dailytest_status['test_dir'] = ''
        dailytest_status['report_url'] = ''

        try:
            project_path = PROJECT_PATH if PROJECT_PATH else os.getcwd()
            
            # Step 1: Run regression test
            dailytest_status['message'] = 'Running regression test...'
            print("Step 1: Running regression test...")
            
            # Auto-detect script path
            script_path = find_script_path()
            if not script_path:
                dailytest_status['status'] = 'failed'
                dailytest_status['message'] = 'Cannot find daily_regression_test.sh script'
                return
            
            # Check if script exists
            if not os.path.exists(script_path):
                dailytest_status['status'] = 'failed'
                dailytest_status['message'] = f'Test script not found: {script_path}'
                return
            
            print(f"Using script path: {script_path}")

            process = subprocess.Popen(
                [script_path, project_path, TEST_RESULTS_DIR],
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
            latest_test_dir = find_latest_test_dir()
            if latest_test_dir:
                dailytest_status['test_dir'] = latest_test_dir

            if process.returncode != 0:
                dailytest_status['status'] = 'failed'
                stderr_output = process.stderr.read()
                dailytest_status['message'] = f'Regression test failed. Error: {stderr_output}'
                return
            
            # Step 2: Run coverage test after regression test completes successfully
            dailytest_status['message'] = 'Regression test completed successfully. Running coverage test...'
            print("Step 2: Running coverage test...")
            
            if latest_test_dir:
                coverage_success = run_coverage_test(project_path, latest_test_dir, update_status=dailytest_status)
                if coverage_success:
                    dailytest_status['status'] = 'completed'
                    dailytest_status['message'] = 'Daily test (regression + coverage) completed successfully.'
                else:
                    dailytest_status['status'] = 'completed'
                    dailytest_status['message'] = 'Regression test completed, but coverage test had issues.'
            else:
                dailytest_status['status'] = 'completed'
                dailytest_status['message'] = 'Regression test completed, but could not find test directory for coverage test.'
            
            # Generate report URL
            if dailytest_status['test_dir']:
                dailytest_status['report_url'] = f"http://localhost:5002/result?date={dailytest_status['test_dir'].split('/')[-1]}"

        except Exception as e:
            dailytest_status['status'] = 'failed'
            dailytest_status['message'] = f'An error occurred: {str(e)}'

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
    """Start daily test (regression + coverage in sequence)"""
    # Check current test status
    if dailytest_status['status'] == 'testing':
        return jsonify({
            'error': 'A daily test is already in progress.',
            'current_status': dailytest_status
        }), 409

    # Start a new thread to run daily test script (regression + coverage)
    threading.Thread(target=run_dailytest_script).start()
    return jsonify({'message': 'Daily test (regression + coverage) started.'}), 202

@app.route('/dailytest/status', methods=['GET'])
def get_dailytest_status():
    """Get daily test status (regression + coverage)"""
    return jsonify(dailytest_status)

@app.route('/regression/run', methods=['POST'])
def run_regression():
    """Run regression test independently"""
    # Check current regression test status
    if regression_status['status'] == 'testing':
        return jsonify({
            'error': 'A regression test is already in progress.',
            'current_status': regression_status
        }), 409
    
    # Start a new thread to run regression test
    project_path = PROJECT_PATH if PROJECT_PATH else os.getcwd()
    threading.Thread(target=run_regression_test_independent, args=(project_path,)).start()
    return jsonify({'message': 'Regression test started.'}), 202

@app.route('/regression/status', methods=['GET'])
def get_regression_status():
    """Get regression test status"""
    return jsonify(regression_status)

@app.route('/coverage/run', methods=['POST'])
def run_coverage():
    """Run coverage test independently"""
    # Check current coverage test status
    if coverage_status['status'] == 'testing':
        return jsonify({
            'error': 'A coverage test is already in progress.',
            'current_status': coverage_status
        }), 409
    
    # Start a new thread to run coverage test
    project_path = PROJECT_PATH if PROJECT_PATH else os.getcwd()
    threading.Thread(target=run_coverage_test_independent, args=(project_path,)).start()
    return jsonify({'message': 'Coverage test started.'}), 202

def run_coverage_test_independent(project_path):
    """Run coverage test independently in a thread"""
    global coverage_status
    with coverage_lock:
        coverage_status['status'] = 'testing'
        coverage_status['message'] = 'Starting coverage test...'
        coverage_status['test_dir'] = ''
        coverage_status['report_url'] = ''
        
        try:
            success = run_coverage_test(project_path, test_dir=None, update_status=coverage_status)
            if not success:
                coverage_status['status'] = 'failed'
        except Exception as e:
            coverage_status['status'] = 'failed'
            coverage_status['message'] = f'An error occurred: {str(e)}'

@app.route('/coverage/status', methods=['GET'])
def get_coverage_status():
    """Get coverage test status"""
    return jsonify(coverage_status)

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

def get_test_result_summary(date_folder):
    """Get test result summary for a given date"""
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
    
    return summary

@app.route('/result', methods=['GET'])
def result():
    """Test results page (supports new JSON structure, date selection and tables)"""
    date = request.args.get('date')

    available_dates = get_available_test_dates()

    if not available_dates:
        return render_template_string("""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Curvine Test Results</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 40px; background: #f5f5f5; }
                .container { max-width: 800px; margin: 0 auto; background: white; padding: 30px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
                .header { text-align: center; margin-bottom: 30px; }
                .no-data { text-align: center; color: #666; font-size: 18px; }
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>🧪 Curvine Test Results</h1>
                </div>
                <div class="no-data">
                    <p>No test results available</p>
                    <p>Please run the daily regression test first</p>
                </div>
            </div>
        </body>
        </html>
        """)

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

    html_template = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Curvine Test Results</title>
        <style>
            * { margin: 0; padding: 0; box-sizing: border-box; }
            body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; line-height: 1.6; color: #333; background: #f5f5f5; }
            .container { max-width: 1200px; margin: 0 auto; padding: 20px; }
            .header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; border-radius: 10px; margin-bottom: 30px; text-align: center; }
            .header h1 { font-size: 2.2em; margin-bottom: 10px; }
            .date-selector { background: white; padding: 20px; border-radius: 10px; margin-bottom: 20px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
            .date-selector h3 { margin-bottom: 15px; color: #2c3e50; }
            .date-dropdown { display: flex; align-items: center; gap: 15px; }
            .date-dropdown label { color: #2c3e50; font-weight: bold; font-size: 16px; }
            .date-dropdown select { flex: 1; padding: 10px 15px; border: 2px solid #ddd; border-radius: 8px; font-size: 16px; background: white; color: #2c3e50; cursor: pointer; transition: border-color 0.3s; }
            .date-dropdown select:hover { border-color: #667eea; }
            .date-dropdown select:focus { outline: none; border-color: #667eea; box-shadow: 0 0 0 3px rgba(102, 126, 234, 0.1); }
            .summary { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin-bottom: 30px; }
            .summary-card { background: white; padding: 25px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); text-align: center; }
            .summary-card h3 { color: #666; margin-bottom: 10px; }
            .summary-card .number { font-size: 1.8em; font-weight: bold; }
            .total { color: #3498db; }
            .passed { color: #27ae60; }
            .failed { color: #e74c3c; }
            .success-rate { color: #f39c12; }
            .coverage { color: #9b59b6; }
            .coverage-section { background: white; padding: 25px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); margin-bottom: 30px; }
            .coverage-title { font-size: 1.3em; font-weight: bold; color: #2c3e50; margin-bottom: 15px; padding-bottom: 10px; border-bottom: 2px solid #9b59b6; }
            .coverage-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin-bottom: 20px; }
            .coverage-card { background: #f8f9fa; padding: 20px; border-radius: 8px; text-align: center; }
            .coverage-card h4 { color: #666; margin-bottom: 10px; font-size: 0.9em; }
            .coverage-card .number { font-size: 1.5em; font-weight: bold; color: #9b59b6; }
            .coverage-link { display: inline-block; margin-top: 15px; padding: 10px 20px; background: #9b59b6; color: white; text-decoration: none; border-radius: 5px; font-weight: bold; }
            .coverage-link:hover { background: #7d3c98; }
            .package-section { background: white; border-radius: 10px; padding: 20px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); margin-bottom: 30px; }
            .package-title { font-size: 1.3em; font-weight: bold; color: #2c3e50; margin-bottom: 15px; padding-bottom: 10px; border-bottom: 2px solid #3498db; }
            .test-table { width: 100%; border-collapse: collapse; margin-bottom: 20px; }
            .test-table th, .test-table td { padding: 10px; text-align: left; border-bottom: 1px solid #ddd; }
            .test-table th { background-color: #f8f9fa; font-weight: bold; color: #495057; }
            .file-group { background: #eef; font-weight: bold; color: #2c3e50; }
            .status-passed { background: #d4edda; color: #155724; padding: 3px 6px; border-radius: 4px; font-weight: bold; }
            .status-failed { background: #f8d7da; color: #721c24; padding: 3px 6px; border-radius: 4px; font-weight: bold; }
            .log-link { color: #007bff; text-decoration: none; font-size: 0.9em; }
            .log-link:hover { text-decoration: underline; }
            .footer { text-align: center; margin-top: 40px; padding: 20px; color: #666; border-top: 1px solid #e0e0e0; }
            .file-block { margin: 18px 0; padding: 14px; border: 1px solid #e0e0e0; border-radius: 8px; background: #fafafa; }
            .file-block-title { font-weight: 600; color: #2c3e50; margin-bottom: 8px; }
            /* Fixed column widths for consistent cross-block alignment */
            .test-table { table-layout: fixed; }
            .test-table th, .test-table td { white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🧪 Curvine Test Results</h1>
                <div id="current-date">Selected: {{ current_date }}</div>
            </div>

            <div class="date-selector">
                <h3>📅 Select Test Date</h3>
                <div class="date-dropdown">
                    <label for="date-select">Test Date:</label>
                    <select id="date-select" onchange="loadTestResult(this.value)">
                        {% for date_info in available_dates %}
                        <option value="{{ date_info.folder }}" {% if date_info.folder == selected_date %}selected{% endif %}>
                            {{ date_info.datetime }} ({{ date_info.folder }})
                        </option>
                        {% endfor %}
                    </select>
                </div>
            </div>

            {% if test_summary %}
            {% if total_tests > 0 or packages|length > 0 or test_cases|length > 0 %}
            <div class="summary">
                <div class="summary-card"><h3>Total Tests</h3><div class="number total">{{ total_tests }}</div></div>
                <div class="summary-card"><h3>Passed</h3><div class="number passed">{{ passed_tests }}</div></div>
                <div class="summary-card"><h3>Failed</h3><div class="number failed">{{ failed_tests }}</div></div>
                <div class="summary-card"><h3>Success Rate</h3><div class="number success-rate">{{ success_rate }}%</div></div>
            </div>
            {% endif %}

            {% if test_summary.coverage %}
            <div class="coverage-section">
                <div class="coverage-title">📊 Code Coverage Report</div>
                <div class="coverage-grid">
                    <div class="coverage-card">
                        <h4>Lines Coverage</h4>
                        <div class="number">{{ "%.2f"|format(test_summary.coverage.lines.percent) }}%</div>
                        <div style="font-size: 0.9em; color: #666; margin-top: 5px;">
                            {{ test_summary.coverage.lines.covered }}/{{ test_summary.coverage.lines.total }}
                        </div>
                    </div>
                    <div class="coverage-card">
                        <h4>Functions Coverage</h4>
                        <div class="number">{{ "%.2f"|format(test_summary.coverage.functions.percent) }}%</div>
                        <div style="font-size: 0.9em; color: #666; margin-top: 5px;">
                            {{ test_summary.coverage.functions.covered }}/{{ test_summary.coverage.functions.total }}
                        </div>
                    </div>
                    <div class="coverage-card">
                        <h4>Regions Coverage</h4>
                        <div class="number">{{ "%.2f"|format(test_summary.coverage.regions.percent) }}%</div>
                        <div style="font-size: 0.9em; color: #666; margin-top: 5px;">
                            {{ test_summary.coverage.regions.covered }}/{{ test_summary.coverage.regions.total }}
                        </div>
                    </div>
                </div>
                {% if test_summary.coverage_report_url %}
                <div style="text-align: center;">
                    <a href="{{ test_summary.coverage_report_url }}" class="coverage-link" target="_blank">View Detailed Coverage Report →</a>
                </div>
                {% endif %}
            </div>
            {% endif %}

            {% if packages|length > 0 %}
            {% for pkg in packages %}
            <div class="package-section">
                <div class="package-title">📦 Package: {{ pkg.name }} (Total: {{ pkg.total }}, Passed: {{ pkg.passed }}, Failed: {{ pkg.failed }}, Success Rate: {{ pkg.success_rate }}%)</div>
                {% for test_file, file_cases in cases_by_package_file.get(pkg.name, {}).items() %}
                <div class="file-block">
                    <div class="file-block-title">{{ test_file }}</div>
                    <table class="test-table">
                        <colgroup>
                            <col style="width: 20%">
                            <col style="width: 50%">
                            <col style="width: 15%">
                            <col style="width: 15%">
                        </colgroup>
                        <thead>
                            <tr>
                                <th>TestFile</th>
                                <th>TestCase</th>
                                <th>Result</th>
                                <th>Log</th>
                            </tr>
                        </thead>
                        <tbody>
                            {% for c in file_cases %}
                            <tr>
                                <td>{{ test_file }}</td>
                                <td>{{ c.test_case }}</td>
                                <td>
                                    <span class="{% if c.status == 'PASSED' %}status-passed{% else %}status-failed{% endif %}">{{ 'PASS' if c.status == 'PASSED' else 'FAIL' }}</span>
                                </td>
                                <td>
                                    <a class="log-link" href="/logs/{{ selected_date }}/{{ c.log }}" target="_blank">View Log</a>
                                </td>
                            </tr>
                            {% endfor %}
                        </tbody>
                    </table>
                </div>
                {% endfor %}
            </div>
            {% endfor %}
            {% endif %}

            {% else %}
            <div class="package-section">No test results for the selected date</div>
            {% endif %}

            <div class="footer">
                <p>Report generated at: {{ current_time }}</p>
                <p>Curvine Test Results Viewer</p>
            </div>
        </div>

        <script>
            function loadTestResult(date) { window.location.href = '/result?date=' + date; }
            setTimeout(function() { location.reload(); }, 30000);
        </script>
    </body>
    </html>
    """

    return render_template_string(
        html_template,
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
        return render_template_string('<html><body><script>window.location.href="/result?date={{date}}"</script></body></html>', date=date)

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

    # Generate log viewer page
    html_template = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Test Log - {{ current_log.display_name if current_log else log_file }}</title>
        <style>
            * { margin: 0; padding: 0; box-sizing: border-box; }
            body { font-family: 'Consolas', 'Monaco', 'Courier New', monospace; background: #1e1e1e; color: #d4d4d4; }
            .header { background: #2d2d30; padding: 15px 20px; border-bottom: 1px solid #3e3e42; }
            .header h1 { color: #ffffff; font-size: 1.5em; margin-bottom: 10px; }
            .log-selector { display: flex; align-items: center; gap: 15px; }
            .log-selector label { color: #cccccc; font-weight: bold; }
            .log-selector select { 
                background: #3c3c3c; 
                color: #ffffff; 
                border: 1px solid #555; 
                padding: 8px 12px; 
                border-radius: 4px;
                font-size: 14px;
            }
            .log-selector select:focus { outline: none; border-color: #007acc; }
            .back-btn { 
                background: #007acc; 
                color: white; 
                border: none; 
                padding: 8px 16px; 
                border-radius: 4px; 
                cursor: pointer;
                text-decoration: none;
                display: inline-block;
            }
            .back-btn:hover { background: #005a9e; }
            .log-content { 
                padding: 20px; 
                white-space: pre-wrap; 
                word-wrap: break-word; 
                line-height: 1.4;
                font-size: 13px;
                max-height: calc(100vh - 120px);
                overflow-y: auto;
            }
            .log-line { margin-bottom: 2px; }
            .log-info { color: #569cd6; }
            .log-error { color: #f44747; }
            .log-warning { color: #ffcc02; }
            .log-success { color: #4ec9b0; }
            .timestamp { color: #808080; }
        </style>
    </head>
    <body>
        <div class="header">
            <h1>📋 Test Log Viewer</h1>
            <div class="log-selector">
                <label for="log-select">Select Log File:</label>
                <select id="log-select" onchange="switchLog()">
                    {% for log in available_logs %}
                    <option value="{{ log.filename }}" {% if log.filename == log_file %}selected{% endif %}>
                        {{ log.display_name }}
                    </option>
                    {% endfor %}
                </select>
                <a href="/result?date={{ date }}" class="back-btn">← Back to Test Results</a>
            </div>
        </div>
        <div class="log-content" id="log-content">{{ log_content }}</div>
        
        <script>
            function switchLog() {
                const selectedLog = document.getElementById('log-select').value;
                window.location.href = '/logs/{{ date }}/' + selectedLog;
            }
            
            // Highlight log lines
            function highlightLogLines() {
                const content = document.getElementById('log-content');
                const lines = content.innerHTML.split('\\n');
                let highlightedLines = [];
                
                for (let line of lines) {
                    let highlightedLine = line;
                    
                    // Highlight different log levels
                    if (line.includes('ERROR')) {
                        highlightedLine = '<span class="log-error">' + line + '</span>';
                    } else if (line.includes('WARN')) {
                        highlightedLine = '<span class="log-warning">' + line + '</span>';
                    } else if (line.includes('INFO')) {
                        highlightedLine = '<span class="log-info">' + line + '</span>';
                    } else if (line.includes('SUCCESS')) {
                        highlightedLine = '<span class="log-success">' + line + '</span>';
                    }
                    
                    // Highlight timestamps
                    highlightedLine = highlightedLine.replace(
                        /(\\d{2}\\/\\d{2}\\/\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3})/g,
                        '<span class="timestamp">$1</span>'
                    );
                    
                    highlightedLines.push(highlightedLine);
                }
                
                content.innerHTML = highlightedLines.join('\\n');
            }
            
            // Page load after highlighting logs
            document.addEventListener('DOMContentLoaded', highlightLogLines);
        </script>
    </body>
    </html>
    """
    
    return render_template_string(html_template,
                                date=date,
                                log_file=log_file,
                                log_content=log_content,
                                available_logs=available_logs,
                                current_log=current_log)

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

def find_script_path(script_name="daily_regression_test.sh"):
    """Auto-detect script path"""
    # 1. First check current directory
    current_dir = os.getcwd()
    script_in_current = os.path.join(current_dir, script_name)
    if os.path.exists(script_in_current):
        return script_in_current
    
    # 2. Check scripts subdirectory of current directory
    script_in_scripts = os.path.join(current_dir, 'scripts', script_name)
    if os.path.exists(script_in_scripts):
        return script_in_scripts
    
    # 3. Check PATH environment variable
    import shutil
    script_in_path = shutil.which(script_name)
    if script_in_path:
        return script_in_path
    
    # 4. Check common locations
    common_paths = [
        '/usr/local/bin',
        '/usr/bin',
        '/opt/curvine/bin',
        '/home/curvine/bin'
    ]
    
    for path in common_paths:
        script_path = os.path.join(path, script_name)
        if os.path.exists(script_path):
            return script_path
    
    return None

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
    
    # Check if scripts directory exists
    scripts_dir = os.path.join(project_path, 'scripts')
    if not os.path.exists(scripts_dir):
        print(f"Warning: scripts directory not found in project path: {scripts_dir}")
    
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
    script_path = find_script_path()
    if script_path:
        print(f"Script path auto-detected: {script_path}")
    else:
        print("Warning: Could not auto-detect daily_regression_test.sh")
        print("Please ensure the script is in one of the following locations:")
        print("  - Current directory")
        print("  - scripts subdirectory of current directory")
        print("  - In system PATH")
        print("  - /usr/local/bin, /usr/bin, /opt/curvine/bin, /home/curvine/bin")
    
    # Start server
    print(f"Starting server: http://{args.host}:{args.port}")
    print(f"Project path: {PROJECT_PATH}")
    print(f"Results directory: {TEST_RESULTS_DIR}")
    if script_path:
        print(f"Script path: {script_path}")
    app.run(host=args.host, port=args.port)