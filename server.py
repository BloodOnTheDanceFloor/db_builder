import os
import sys
import time
from datetime import datetime
from flask import Flask, render_template, jsonify, request
import threading
import logging

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

# 导入所需模块
from StockDownloader.src.utils.correlation_calculator import update_stock_index_correlation
from StockDownloader.src.database.session import SessionLocal, engine
from StockDownloader.src.database.base import Base
from StockDownloader.src.tasks.update_data_task import update_stock_data, update_index_data

# 设置日志
log_dir = os.path.join(os.path.dirname(__file__), 'logs')
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"server_{datetime.now().strftime('%Y%m%d')}.log")

# 配置日志
logger = logging.getLogger('db_builder_server')
logger.setLevel(logging.INFO)
# 清除已有的处理器，避免重复
if logger.handlers:
    for handler in logger.handlers:
        logger.removeHandler(handler)
# 添加处理器
file_handler = logging.FileHandler(log_file, encoding='utf-8')
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(console_handler)

# 创建Flask应用
app = Flask(__name__)

# 存储任务状态
task_status = {
    'update_data': {'status': 'idle', 'message': ''},
    'rebuild_database': {'status': 'idle', 'message': ''},
    'update_correlation': {'status': 'idle', 'message': ''}
}

# 后台任务锁，防止同时运行多个任务
task_lock = threading.Lock()

# HTML模板
@app.route('/')
def index():
    html = '''
    <!DOCTYPE html>
    <html>
    <head>
        <title>数据库管理工具</title>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <style>
            body {
                font-family: Arial, sans-serif;
                max-width: 800px;
                margin: 0 auto;
                padding: 20px;
                line-height: 1.6;
            }
            h1 {
                color: #333;
                text-align: center;
            }
            .button-container {
                display: flex;
                flex-direction: column;
                gap: 15px;
                margin-top: 30px;
            }
            button {
                padding: 12px 20px;
                font-size: 16px;
                cursor: pointer;
                background-color: #4CAF50;
                color: white;
                border: none;
                border-radius: 4px;
                transition: background-color 0.3s;
            }
            button:hover {
                background-color: #45a049;
            }
            button:disabled {
                background-color: #cccccc;
                cursor: not-allowed;
            }
            #status {
                margin-top: 30px;
                padding: 15px;
                border: 1px solid #ddd;
                border-radius: 4px;
                background-color: #f9f9f9;
                min-height: 100px;
            }
            .task-status {
                margin-bottom: 10px;
                padding: 10px;
                border-radius: 4px;
            }
            .idle {
                background-color: #f0f0f0;
            }
            .running {
                background-color: #fff3cd;
            }
            .success {
                background-color: #d4edda;
            }
            .error {
                background-color: #f8d7da;
            }
        </style>
    </head>
    <body>
        <h1>数据库管理工具</h1>
        
        <div class="button-container">
            <button id="updateDataBtn" onclick="runTask('update_data')">更新数据</button>
            <button id="rebuildDatabaseBtn" onclick="runTask('rebuild_database')">重建数据库</button>
            <button id="updateCorrelationBtn" onclick="runTask('update_correlation')">更新指数相关度</button>
        </div>
        
        <div id="status">
            <div id="update_data_status" class="task-status idle">更新数据: 空闲</div>
            <div id="rebuild_database_status" class="task-status idle">重建数据库: 空闲</div>
            <div id="update_correlation_status" class="task-status idle">更新指数相关度: 空闲</div>
        </div>
        
        <script>
            // 定期检查任务状态
            function checkStatus() {
                fetch('/status')
                    .then(response => response.json())
                    .then(data => {
                        updateStatusUI(data);
                        setTimeout(checkStatus, 2000); // 每2秒检查一次
                    })
                    .catch(error => {
                        console.error('获取状态失败:', error);
                        setTimeout(checkStatus, 5000); // 出错后5秒再试
                    });
            }
            
            // 更新UI显示
            function updateStatusUI(data) {
                const tasks = ['update_data', 'rebuild_database', 'update_correlation'];
                const buttons = {
                    'update_data': document.getElementById('updateDataBtn'),
                    'rebuild_database': document.getElementById('rebuildDatabaseBtn'),
                    'update_correlation': document.getElementById('updateCorrelationBtn')
                };
                
                // 如果有任何任务正在运行，禁用所有按钮
                const anyRunning = tasks.some(task => data[task].status === 'running');
                
                tasks.forEach(task => {
                    const statusElement = document.getElementById(`${task}_status`);
                    const status = data[task].status;
                    const message = data[task].message || '';
                    
                    // 更新状态显示
                    statusElement.className = `task-status ${status}`;
                    statusElement.textContent = `${getTaskName(task)}: ${getStatusText(status)} ${message}`;
                    
                    // 更新按钮状态
                    buttons[task].disabled = anyRunning;
                });
            }
            
            // 获取任务名称
            function getTaskName(task) {
                const names = {
                    'update_data': '更新数据',
                    'rebuild_database': '重建数据库',
                    'update_correlation': '更新指数相关度'
                };
                return names[task] || task;
            }
            
            // 获取状态文本
            function getStatusText(status) {
                const texts = {
                    'idle': '空闲',
                    'running': '运行中...',
                    'success': '成功',
                    'error': '错误'
                };
                return texts[status] || status;
            }
            
            // 运行任务
            function runTask(task) {
                fetch(`/run/${task}`, {
                    method: 'POST'
                })
                .then(response => response.json())
                .then(data => {
                    console.log(`任务 ${task} 启动:`, data);
                })
                .catch(error => {
                    console.error(`启动任务 ${task} 失败:`, error);
                });
            }
            
            // 页面加载完成后开始检查状态
            window.onload = checkStatus;
        </script>
    </body>
    </html>
    '''
    return html

# 获取任务状态
@app.route('/status')
def get_status():
    return jsonify(task_status)

# 运行更新数据任务
@app.route('/run/update_data', methods=['POST'])
def run_update_data():
    if task_lock.acquire(blocking=False):
        try:
            # 检查是否有任务正在运行
            if any(task['status'] == 'running' for task in task_status.values()):
                task_lock.release()
                return jsonify({'success': False, 'message': '有其他任务正在运行'})
            
            # 更新状态
            task_status['update_data']['status'] = 'running'
            task_status['update_data']['message'] = '正在更新数据...'
            
            # 启动后台线程执行任务
            thread = threading.Thread(target=run_update_data_task)
            thread.daemon = True
            thread.start()
            
            return jsonify({'success': True, 'message': '更新数据任务已启动'})
        except Exception as e:
            task_status['update_data']['status'] = 'error'
            task_status['update_data']['message'] = f'启动任务失败: {str(e)}'
            task_lock.release()
            return jsonify({'success': False, 'message': str(e)})
    else:
        return jsonify({'success': False, 'message': '有其他任务正在运行'})

# 运行重建数据库任务
@app.route('/run/rebuild_database', methods=['POST'])
def run_rebuild_database():
    if task_lock.acquire(blocking=False):
        try:
            # 检查是否有任务正在运行
            if any(task['status'] == 'running' for task in task_status.values()):
                task_lock.release()
                return jsonify({'success': False, 'message': '有其他任务正在运行'})
            
            # 更新状态
            task_status['rebuild_database']['status'] = 'running'
            task_status['rebuild_database']['message'] = '正在重建数据库...'
            
            # 启动后台线程执行任务
            thread = threading.Thread(target=run_rebuild_database_task)
            thread.daemon = True
            thread.start()
            
            return jsonify({'success': True, 'message': '重建数据库任务已启动'})
        except Exception as e:
            task_status['rebuild_database']['status'] = 'error'
            task_status['rebuild_database']['message'] = f'启动任务失败: {str(e)}'
            task_lock.release()
            return jsonify({'success': False, 'message': str(e)})
    else:
        return jsonify({'success': False, 'message': '有其他任务正在运行'})

# 运行更新指数相关度任务
@app.route('/run/update_correlation', methods=['POST'])
def run_update_correlation():
    if task_lock.acquire(blocking=False):
        try:
            # 检查是否有任务正在运行
            if any(task['status'] == 'running' for task in task_status.values()):
                task_lock.release()
                return jsonify({'success': False, 'message': '有其他任务正在运行'})
            
            # 更新状态
            task_status['update_correlation']['status'] = 'running'
            task_status['update_correlation']['message'] = '正在更新指数相关度...'
            
            # 启动后台线程执行任务
            thread = threading.Thread(target=run_update_correlation_task)
            thread.daemon = True
            thread.start()
            
            return jsonify({'success': True, 'message': '更新指数相关度任务已启动'})
        except Exception as e:
            task_status['update_correlation']['status'] = 'error'
            task_status['update_correlation']['message'] = f'启动任务失败: {str(e)}'
            task_lock.release()
            return jsonify({'success': False, 'message': str(e)})
    else:
        return jsonify({'success': False, 'message': '有其他任务正在运行'})

# 更新数据任务实现
def run_update_data_task():
    try:
        logger.info("开始执行更新数据任务")
        task_status['update_data']['message'] = '正在更新股票数据...'
        
        # 更新股票数据
        update_stock_data()
        
        task_status['update_data']['message'] = '正在更新指数数据...'
        # 更新指数数据
        update_index_data()
        
        task_status['update_data']['status'] = 'success'
        task_status['update_data']['message'] = f'更新完成 ({datetime.now().strftime("%H:%M:%S")})'  
        logger.info("更新数据任务完成")
    except Exception as e:
        logger.error(f"更新数据任务失败: {str(e)}")
        task_status['update_data']['status'] = 'error'
        task_status['update_data']['message'] = f'错误: {str(e)}'
    finally:
        task_lock.release()

# 重建数据库任务实现
def run_rebuild_database_task():
    try:
        logger.info("开始执行重建数据库任务")
        task_status['rebuild_database']['message'] = '正在删除现有数据库表...'
        
        # 删除所有表并重新创建
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)
        
        task_status['rebuild_database']['message'] = '正在下载股票数据...'
        # 导入下载任务模块
        from StockDownloader.src.tasks.download_stock_task import download_all_stock_data
        download_all_stock_data(update_only=False)
        
        task_status['rebuild_database']['message'] = '正在下载指数数据...'
        # 导入下载任务模块
        from StockDownloader.src.tasks.download_index_task import download_all_index_data
        download_all_index_data(update_only=False)
        
        task_status['rebuild_database']['status'] = 'success'
        task_status['rebuild_database']['message'] = f'重建完成 ({datetime.now().strftime("%H:%M:%S")})'  
        logger.info("重建数据库任务完成")
    except Exception as e:
        logger.error(f"重建数据库任务失败: {str(e)}")
        task_status['rebuild_database']['status'] = 'error'
        task_status['rebuild_database']['message'] = f'错误: {str(e)}'
    finally:
        task_lock.release()

# 更新指数相关度任务实现
def run_update_correlation_task():
    try:
        logger.info("开始执行更新指数相关度任务")
        task_status['update_correlation']['message'] = '正在计算股票与指数相关度...'
        
        # 获取CPU逻辑处理器数量
        import os
        cpu_count = os.cpu_count()
        # 保留一些核心给系统使用，至少保留4个核心
        max_workers = max(1, cpu_count - 4) if cpu_count else 1
        
        logger.info(f"使用 {max_workers} 个进程进行并行处理")
        task_status['update_correlation']['message'] = f'正在计算股票与指数相关度... (使用 {max_workers} 个进程)'
        
        # 调用相关度计算函数，传入进程数
        update_stock_index_correlation(max_workers=max_workers)
        
        task_status['update_correlation']['status'] = 'success'
        task_status['update_correlation']['message'] = f'更新完成 ({datetime.now().strftime("%H:%M:%S")})'
        logger.info("更新指数相关度任务完成")
    except Exception as e:
        logger.error(f"更新指数相关度任务失败: {str(e)}")
        task_status['update_correlation']['status'] = 'error'
        task_status['update_correlation']['message'] = f'错误: {str(e)}'
    finally:
        task_lock.release()

if __name__ == "__main__":
    # 启动Flask应用
    app.run(host='0.0.0.0', port=5000, debug=True)