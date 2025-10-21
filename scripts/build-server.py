from flask import Flask, request, jsonify, render_template_string
import subprocess
import threading
import os
import json
import glob
import sys
import argparse
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

# Create lock objects
build_lock = threading.Lock()
dailytest_lock = threading.Lock()

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

def run_dailytest_script():
    global dailytest_status
    with dailytest_lock:  # Ensure only one test instance at a time
        dailytest_status['status'] = 'testing'
        dailytest_status['message'] = 'Starting daily regression test...'
        dailytest_status['test_dir'] = ''
        dailytest_status['report_url'] = ''

        try:
            # Use Popen to run daily test script and stream logs
            # Pass project path and results directory as arguments
            project_path = PROJECT_PATH if PROJECT_PATH else os.getcwd()
            
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

            if process.returncode == 0:
                dailytest_status['status'] = 'completed'
                dailytest_status['message'] = 'Daily regression test completed successfully.'
                # Generate report URL
                if dailytest_status['test_dir']:
                    dailytest_status['report_url'] = f"http://localhost:5002/result?date={dailytest_status['test_dir'].split('/')[-1]}"
            else:
                dailytest_status['status'] = 'failed'
                # Print error output
                stderr_output = process.stderr.read()
                dailytest_status['message'] = f'Daily regression test failed. Error: {stderr_output}'

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
    """Start daily regression test"""
    # Check current test status
    if dailytest_status['status'] == 'testing':
        return jsonify({
            'error': 'A daily test is already in progress.',
            'current_status': dailytest_status
        }), 409

    # Start a new thread to run daily test script
    threading.Thread(target=run_dailytest_script).start()
    return jsonify({'message': 'Daily regression test started.'}), 202

@app.route('/dailytest/status', methods=['GET'])
def get_dailytest_status():
    """Get daily test status"""
    return jsonify(dailytest_status)

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
    summary_file = os.path.join(TEST_RESULTS_DIR, date_folder, "test_summary.json")
    if not os.path.exists(summary_file):
        return None
    
    try:
        with open(summary_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error reading summary file: {e}")
        return None

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
            <div class="summary">
                <div class="summary-card"><h3>Total Tests</h3><div class="number total">{{ total_tests }}</div></div>
                <div class="summary-card"><h3>Passed</h3><div class="number passed">{{ passed_tests }}</div></div>
                <div class="summary-card"><h3>Failed</h3><div class="number failed">{{ failed_tests }}</div></div>
                <div class="summary-card"><h3>Success Rate</h3><div class="number success-rate">{{ success_rate }}%</div></div>
            </div>

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