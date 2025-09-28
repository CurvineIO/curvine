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

# 用于存储构建状态
build_status = {
    'status': 'idle',  # idle, building, completed, failed
    'message': ''
}

# 用于存储每日测试状态
dailytest_status = {
    'status': 'idle',  # idle, testing, completed, failed
    'message': '',
    'test_dir': '',
    'report_url': ''
}

# 创建锁对象
build_lock = threading.Lock()
dailytest_lock = threading.Lock()

# 项目路径（全局变量）
PROJECT_PATH = None

# 测试结果目录（全局变量）
TEST_RESULTS_DIR = "daily_test_results"

def run_build_script(date, commit):
    global build_status
    with build_lock:  # 确保同一时间只能有一个构建实例
        build_status['status'] = 'building'
        build_status['message'] = f'Starting build for date: {date}, commit: {commit}'

        try:
            # 使用 Popen 执行构建脚本，并实时输出日志
            process = subprocess.Popen(
                ['./build.sh', date, commit],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True
            )

            # 实时读取输出
            for line in process.stdout:
                print(line, end='')  # 输出到控制台

            # 等待进程结束
            process.wait()

            if process.returncode == 0:
                build_status['status'] = 'completed'
                build_status['message'] = 'Build completed successfully.'
            else:
                build_status['status'] = 'failed'
                # 输出错误信息
                stderr_output = process.stderr.read()
                build_status['message'] = f'Build failed. Error: {stderr_output}'

        except Exception as e:
            build_status['status'] = 'failed'
            build_status['message'] = f'An error occurred: {str(e)}'

def run_dailytest_script():
    global dailytest_status
    with dailytest_lock:  # 确保同一时间只能有一个测试实例
        dailytest_status['status'] = 'testing'
        dailytest_status['message'] = 'Starting daily regression test...'
        dailytest_status['test_dir'] = ''
        dailytest_status['report_url'] = ''

        try:
            # 使用 Popen 执行每日测试脚本，并实时输出日志
            # 传递项目路径和结果目录作为参数
            project_path = PROJECT_PATH if PROJECT_PATH else os.getcwd()
            
            # 自动查找脚本路径
            script_path = find_script_path()
            if not script_path:
                dailytest_status['status'] = 'failed'
                dailytest_status['message'] = 'Cannot find daily_regression_test.sh script'
                return
            
            # 检查脚本是否存在
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

            # 实时读取输出
            for line in process.stdout:
                print(line, end='')  # 输出到控制台
                # Try to extract test directory from output
                if 'Test directory:' in line:
                    import re
                    match = re.search(r'Test directory: (.+)', line)
                    if match:
                        dailytest_status['test_dir'] = match.group(1).strip()

            # 等待进程结束
            process.wait()

            if process.returncode == 0:
                dailytest_status['status'] = 'completed'
                dailytest_status['message'] = 'Daily regression test completed successfully.'
                # 生成报告URL
                if dailytest_status['test_dir']:
                    dailytest_status['report_url'] = f"http://localhost:5002/result?date={dailytest_status['test_dir'].split('/')[-1]}"
            else:
                dailytest_status['status'] = 'failed'
                # 输出错误信息
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

    # 检查当前构建状态
    if build_status['status'] == 'building':
        return jsonify({
            'error': 'A build is already in progress.',
            'current_status': build_status
        }), 409

    # 启动一个新线程来运行构建脚本
    threading.Thread(target=run_build_script, args=(date, commit)).start()
    return jsonify({'message': 'Build started.'}), 202

@app.route('/build/status', methods=['GET'])
def status():
    return jsonify(build_status)

@app.route('/dailytest', methods=['POST'])
def dailytest():
    """启动每日回归测试"""
    # 检查当前测试状态
    if dailytest_status['status'] == 'testing':
        return jsonify({
            'error': 'A daily test is already in progress.',
            'current_status': dailytest_status
        }), 409

    # 启动一个新线程来运行每日测试脚本
    threading.Thread(target=run_dailytest_script).start()
    return jsonify({'message': 'Daily regression test started.'}), 202

@app.route('/dailytest/status', methods=['GET'])
def get_dailytest_status():
    """获取每日测试状态"""
    return jsonify(dailytest_status)

def get_available_test_dates():
    """获取可用的测试日期列表"""
    if not os.path.exists(TEST_RESULTS_DIR):
        return []
    
    dates = []
    for item in os.listdir(TEST_RESULTS_DIR):
        item_path = os.path.join(TEST_RESULTS_DIR, item)
        if os.path.isdir(item_path):
            # 提取日期部分 (格式: YYYYMMDD_HHMMSS)
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
    
    # 按时间倒序排列
    dates.sort(key=lambda x: x['sort_key'], reverse=True)
    return dates

def get_test_result_summary(date_folder):
    """获取指定日期的测试结果摘要"""
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
    """测试结果页面"""
    date = request.args.get('date')
    
    # 获取所有可用的测试日期
    available_dates = get_available_test_dates()
    
    if not available_dates:
        return render_template_string("""
        <!DOCTYPE html>
        <html lang="zh-CN">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Curvine 测试结果</title>
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
                    <h1>🧪 Curvine 测试结果</h1>
                </div>
                <div class="no-data">
                    <p>暂无测试结果数据</p>
                    <p>请先运行每日回归测试</p>
                </div>
            </div>
        </body>
        </html>
        """)
    
    # 如果没有指定日期，使用最新的日期
    if not date:
        date = available_dates[0]['folder']
    
    # 获取指定日期的测试结果
    test_summary = get_test_result_summary(date)
    
    # 生成HTML页面
    html_template = """
    <!DOCTYPE html>
    <html lang="zh-CN">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Curvine 测试结果</title>
        <style>
            * { margin: 0; padding: 0; box-sizing: border-box; }
            body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; line-height: 1.6; color: #333; background: #f5f5f5; }
            .container { max-width: 1200px; margin: 0 auto; padding: 20px; }
            .header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; border-radius: 10px; margin-bottom: 30px; text-align: center; }
            .header h1 { font-size: 2.5em; margin-bottom: 10px; }
            .date-selector { background: white; padding: 20px; border-radius: 10px; margin-bottom: 20px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
            .date-selector h3 { margin-bottom: 15px; color: #2c3e50; }
            .date-dropdown { display: flex; align-items: center; gap: 15px; }
            .date-dropdown label { color: #2c3e50; font-weight: bold; font-size: 16px; }
            .date-dropdown select { 
                flex: 1;
                padding: 10px 15px; 
                border: 2px solid #ddd; 
                border-radius: 8px; 
                font-size: 16px;
                background: white;
                color: #2c3e50;
                cursor: pointer;
                transition: border-color 0.3s;
            }
            .date-dropdown select:hover { border-color: #667eea; }
            .date-dropdown select:focus { 
                outline: none; 
                border-color: #667eea; 
                box-shadow: 0 0 0 3px rgba(102, 126, 234, 0.1);
            }
            .summary { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin-bottom: 30px; }
            .summary-card { background: white; padding: 25px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); text-align: center; }
            .summary-card h3 { color: #666; margin-bottom: 10px; }
            .summary-card .number { font-size: 2.5em; font-weight: bold; }
            .total { color: #3498db; }
            .passed { color: #27ae60; }
            .failed { color: #e74c3c; }
            .success-rate { color: #f39c12; }
            .results { background: white; border-radius: 10px; padding: 30px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
            .results h2 { color: #2c3e50; margin-bottom: 20px; }
            .result-item { 
                display: flex; 
                justify-content: space-between; 
                align-items: center; 
                padding: 15px; 
                border-bottom: 1px solid #eee; 
                cursor: pointer;
                transition: background-color 0.3s;
            }
            .result-item:hover { background-color: #f8f9fa; }
            .result-item:last-child { border-bottom: none; }
            .result-item > div { display: flex; align-items: center; gap: 10px; }
            .status-badge { padding: 5px 15px; border-radius: 20px; font-weight: bold; }
            .status-passed { background: #d4edda; color: #155724; }
            .status-failed { background: #f8d7da; color: #721c24; }
            .view-log-btn { 
                color: #007acc; 
                font-size: 0.9em; 
                cursor: pointer;
                padding: 5px 10px;
                border-radius: 4px;
                transition: background-color 0.3s;
            }
            .view-log-btn:hover { background-color: #e3f2fd; }
            .footer { text-align: center; margin-top: 40px; padding: 20px; color: #666; border-top: 1px solid #e0e0e0; }
            .no-data { text-align: center; color: #666; font-size: 18px; padding: 40px; }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🧪 Curvine 测试结果</h1>
                <div id="current-date">当前选择: {{ current_date }}</div>
            </div>
            
            <div class="date-selector">
                <h3>📅 选择测试日期</h3>
                <div class="date-dropdown">
                    <label for="date-select">测试日期:</label>
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
                <div class="summary-card">
                    <h3>总测试数</h3>
                    <div class="number total">{{ test_summary.total_tests }}</div>
                </div>
                <div class="summary-card">
                    <h3>通过</h3>
                    <div class="number passed">{{ test_summary.passed_tests }}</div>
                </div>
                <div class="summary-card">
                    <h3>失败</h3>
                    <div class="number failed">{{ test_summary.failed_tests }}</div>
                </div>
                <div class="summary-card">
                    <h3>成功率</h3>
                    <div class="number success-rate">{{ test_summary.success_rate }}%</div>
                </div>
            </div>
            
            <div class="results">
                <h2>📊 测试结果详情</h2>
                {% for result in test_summary.results %}
                <div class="result-item" onclick="viewTestLog('{{ result.category }}')">
                    <span><strong>{{ result.category }}</strong></span>
                    <div>
                        <span class="status-badge {% if result.status == 'PASSED' %}status-passed{% else %}status-failed{% endif %}">
                            {% if result.status == 'PASSED' %}通过{% else %}失败{% endif %}
                        </span>
                        <span class="view-log-btn">📋 查看日志</span>
                    </div>
                </div>
                {% endfor %}
            </div>
            {% else %}
            <div class="no-data">
                <p>该日期没有测试结果数据</p>
            </div>
            {% endif %}
            
            <div class="footer">
                <p>报告生成时间: {{ current_time }}</p>
                <p>Curvine 测试结果查看系统</p>
            </div>
        </div>
        
        <script>
            function loadTestResult(date) {
                window.location.href = '/result?date=' + date;
            }
            
            function viewTestLog(testCategory) {
                // 将测试分类名称转换为日志文件名
                const logFileName = testCategory.toLowerCase().replace(/\s+/g, '_') + '.log';
                window.location.href = '/logs/{{ selected_date }}/' + logFileName;
            }
            
            // 自动刷新页面（每30秒）
            setTimeout(function() {
                location.reload();
            }, 30000);
        </script>
    </body>
    </html>
    """
    
    return render_template_string(html_template, 
                                available_dates=available_dates,
                                selected_date=date,
                                current_date=next((d['datetime'] for d in available_dates if d['folder'] == date), '未知'),
                                test_summary=test_summary,
                                current_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

@app.route('/api/test-dates', methods=['GET'])
def api_test_dates():
    """API: 获取所有可用的测试日期"""
    dates = get_available_test_dates()
    return jsonify(dates)

@app.route('/api/test-result/<date>', methods=['GET'])
def api_test_result(date):
    """API: 获取指定日期的测试结果"""
    test_summary = get_test_result_summary(date)
    if test_summary is None:
        return jsonify({'error': 'Test result not found'}), 404
    return jsonify(test_summary)

def get_available_logs(date_folder):
    """获取指定日期的可用日志文件"""
    logs_dir = os.path.join(TEST_RESULTS_DIR, date_folder)
    if not os.path.exists(logs_dir):
        return []
    
    log_files = []
    for file in os.listdir(logs_dir):
        if file.endswith('.log') and file != 'daily_test.log':
            # 提取测试名称（去掉.log后缀）
            test_name = file.replace('.log', '')
            log_files.append({
                'filename': file,
                'test_name': test_name,
                'display_name': test_name.replace('_', ' ').title()
            })
    
    return sorted(log_files, key=lambda x: x['test_name'])

@app.route('/logs/<date>/<log_file>', methods=['GET'])
def view_log(date, log_file):
    """查看测试日志"""
    # 验证日期格式
    if not os.path.exists(os.path.join(TEST_RESULTS_DIR, date)):
        return jsonify({'error': 'Date not found'}), 404
    
    log_path = os.path.join(TEST_RESULTS_DIR, date, log_file)
    if not os.path.exists(log_path):
        return jsonify({'error': 'Log file not found'}), 404
    
    # 读取日志内容
    try:
        with open(log_path, 'r', encoding='utf-8') as f:
            log_content = f.read()
    except Exception as e:
        return jsonify({'error': f'Failed to read log file: {str(e)}'}), 500
    
    # 获取可用的日志文件列表
    available_logs = get_available_logs(date)
    current_log = next((log for log in available_logs if log['filename'] == log_file), None)
    
    # 生成日志查看页面
    html_template = """
    <!DOCTYPE html>
    <html lang="zh-CN">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>测试日志 - {{ current_log.display_name if current_log else log_file }}</title>
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
            <h1>📋 测试日志查看器</h1>
            <div class="log-selector">
                <label for="log-select">选择日志文件:</label>
                <select id="log-select" onchange="switchLog()">
                    {% for log in available_logs %}
                    <option value="{{ log.filename }}" {% if log.filename == log_file %}selected{% endif %}>
                        {{ log.display_name }}
                    </option>
                    {% endfor %}
                </select>
                <a href="/result?date={{ date }}" class="back-btn">← 返回测试结果</a>
            </div>
        </div>
        <div class="log-content" id="log-content">{{ log_content }}</div>
        
        <script>
            function switchLog() {
                const selectedLog = document.getElementById('log-select').value;
                window.location.href = '/logs/{{ date }}/' + selectedLog;
            }
            
            // 高亮日志行
            function highlightLogLines() {
                const content = document.getElementById('log-content');
                const lines = content.innerHTML.split('\\n');
                let highlightedLines = [];
                
                for (let line of lines) {
                    let highlightedLine = line;
                    
                    // 高亮不同级别的日志
                    if (line.includes('ERROR')) {
                        highlightedLine = '<span class="log-error">' + line + '</span>';
                    } else if (line.includes('WARN')) {
                        highlightedLine = '<span class="log-warning">' + line + '</span>';
                    } else if (line.includes('INFO')) {
                        highlightedLine = '<span class="log-info">' + line + '</span>';
                    } else if (line.includes('SUCCESS')) {
                        highlightedLine = '<span class="log-success">' + line + '</span>';
                    }
                    
                    // 高亮时间戳
                    highlightedLine = highlightedLine.replace(
                        /(\\d{2}\\/\\d{2}\\/\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3})/g,
                        '<span class="timestamp">$1</span>'
                    );
                    
                    highlightedLines.push(highlightedLine);
                }
                
                content.innerHTML = highlightedLines.join('\\n');
            }
            
            // 页面加载完成后高亮日志
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
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description='Curvine Build Server - 构建和测试服务器',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python3 build-server.py                           # 使用默认路径
  python3 build-server.py --project-path /path/to/curvine
  python3 build-server.py -p /home/user/curvine-project
        """
    )
    
    parser.add_argument(
        '--project-path', '-p',
        type=str,
        default=None,
        help='指定curvine项目的路径（默认使用当前工作目录）'
    )
    
    parser.add_argument(
        '--port',
        type=int,
        default=5002,
        help='服务器端口（默认5002）'
    )
    
    parser.add_argument(
        '--host',
        type=str,
        default='0.0.0.0',
        help='服务器主机地址（默认0.0.0.0）'
    )
    
    parser.add_argument(
        '--results-dir', '-r',
        type=str,
        default='daily_test_results',
        help='测试结果目录（默认daily_test_results）'
    )
    
    return parser.parse_args()

def find_script_path(script_name="daily_regression_test.sh"):
    """自动查找脚本路径"""
    # 1. 首先检查当前目录
    current_dir = os.getcwd()
    script_in_current = os.path.join(current_dir, script_name)
    if os.path.exists(script_in_current):
        return script_in_current
    
    # 2. 检查当前目录的scripts子目录
    script_in_scripts = os.path.join(current_dir, 'scripts', script_name)
    if os.path.exists(script_in_scripts):
        return script_in_scripts
    
    # 3. 检查PATH环境变量
    import shutil
    script_in_path = shutil.which(script_name)
    if script_in_path:
        return script_in_path
    
    # 4. 在常见位置查找
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
    """验证项目路径"""
    if not os.path.exists(project_path):
        print(f"错误: 指定的项目路径不存在: {project_path}")
        sys.exit(1)
    
    if not os.path.isdir(project_path):
        print(f"错误: 指定的路径不是目录: {project_path}")
        sys.exit(1)
    
    # 检查是否为curvine项目
    cargo_toml = os.path.join(project_path, 'Cargo.toml')
    if not os.path.exists(cargo_toml):
        print(f"警告: 指定路径可能不是curvine项目（未找到Cargo.toml）: {project_path}")
    
    # 检查是否存在scripts目录
    scripts_dir = os.path.join(project_path, 'scripts')
    if not os.path.exists(scripts_dir):
        print(f"警告: 项目路径中未找到scripts目录: {scripts_dir}")
    
    return project_path

if __name__ == '__main__':
    # 解析命令行参数
    args = parse_arguments()
    
    # 设置项目路径
    if args.project_path:
        PROJECT_PATH = validate_project_path(args.project_path)
        print(f"使用指定的项目路径: {PROJECT_PATH}")
    else:
        PROJECT_PATH = os.getcwd()
        print(f"使用当前工作目录作为项目路径: {PROJECT_PATH}")
    
    # 设置测试结果目录
    TEST_RESULTS_DIR = args.results_dir
    print(f"使用测试结果目录: {TEST_RESULTS_DIR}")
    
    # 自动查找脚本路径
    script_path = find_script_path()
    if script_path:
        print(f"自动找到脚本路径: {script_path}")
    else:
        print("警告: 无法自动找到daily_regression_test.sh脚本")
        print("请确保脚本在以下位置之一:")
        print("  - 当前目录")
        print("  - 当前目录的scripts子目录")
        print("  - 系统PATH中")
        print("  - /usr/local/bin, /usr/bin, /opt/curvine/bin, /home/curvine/bin")
    
    # 启动服务器
    print(f"启动服务器: http://{args.host}:{args.port}")
    print(f"项目路径: {PROJECT_PATH}")
    print(f"结果目录: {TEST_RESULTS_DIR}")
    if script_path:
        print(f"脚本路径: {script_path}")
    app.run(host=args.host, port=args.port)