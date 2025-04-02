import sys
import os
import time
from datetime import datetime, timedelta
import akshare as ak
from functools import wraps
import random
import logging
from dotenv import load_dotenv

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

# 设置日志
log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../logs'))
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"daily_update_{datetime.now().strftime('%Y%m%d')}.log")

# 配置日志 - 只配置当前模块的logger，避免与StockDownloader的logger冲突
logger = logging.getLogger('daily_update')
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

# 加载环境变量
env_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../StockDownloader/.env'))
if os.path.exists(env_path):
    load_dotenv(env_path)
    logger.info(f"已加载环境变量文件: {env_path}")
    logger.info(f"数据库用户: {os.getenv('DB_USER')}")
else:
    logger.error(f"环境变量文件不存在: {env_path}")

# 导入StockDownloader模块
from StockDownloader.src.tasks.download_stock_task import download_all_stock_data
from StockDownloader.src.tasks.download_index_task import download_all_index_data

def retry_with_delay(max_retries=3, initial_delay=60):
    """
    带重试和延迟的装饰器
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            delay = initial_delay
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    retries += 1
                    if retries == max_retries:
                        raise e
                    logger.warning(f"操作失败，{delay}秒后重试: {str(e)}")
                    # 添加随机延迟，避免固定间隔
                    actual_delay = delay + random.randint(1, 30)
                    time.sleep(actual_delay)
                    # 增加下次重试的延迟时间
                    delay *= 2
            return None
        return wrapper
    return decorator

@retry_with_delay(max_retries=3, initial_delay=60)
def is_trading_day():
    """
    判断今天是否为交易日
    通过获取交易日历来判断
    """
    try:
        today = datetime.now().strftime('%Y%m%d')
        # 获取交易日历
        df = ak.tool_trade_date_hist_sina()
        # 将交易日历中的日期转换为字符串格式
        trade_dates = [d.strftime('%Y%m%d') for d in df['trade_date'].values]
        return today in trade_dates
    except Exception as e:
        logger.error(f"检查交易日时发生错误: {str(e)}")
        raise

@retry_with_delay(max_retries=3, initial_delay=300)  # 5分钟初始延迟
def update_stock_data():
    """
    更新股票数据，带重试机制
    """
    download_all_stock_data(update_only=True)

@retry_with_delay(max_retries=3, initial_delay=300)  # 5分钟初始延迟
def update_index_data():
    """
    更新指数数据，带重试机制
    """
    download_all_index_data(update_only=True)

def is_update_time_window():
    """
    检查是否在更新时间窗口内（23:00-03:00）
    """
    current_hour = datetime.now().hour
    return current_hour >= 23 or current_hour <= 3

def wait_until_next_check():
    """
    等待到第二天23:00
    """
    now = datetime.now()
    # 计算到第二天23:00的时间
    next_check = now.replace(hour=23, minute=0, second=0, microsecond=0)
    if now >= next_check:
        next_check += timedelta(days=1)
    
    wait_seconds = (next_check - now).total_seconds()
    logger.info(f"等待 {wait_seconds/3600:.2f} 小时到下次检查时间")
    time.sleep(wait_seconds)

def update_data():
    """
    执行数据更新任务
    """
    try:
        # 更新股票数据
        logger.info("开始更新股票日线数据...")
        update_stock_data()
        
        # 随机等待5-10分钟
        wait_time = random.randint(300, 600)
        logger.info(f"等待{wait_time}秒后开始更新指数日线数据...")
        time.sleep(wait_time)
        
        # 更新指数数据
        logger.info("开始更新指数日线数据...")
        update_index_data()
        
        logger.info("数据更新任务执行完成")
        return True
    except Exception as e:
        logger.error(f"数据更新过程中发生错误: {str(e)}")
        return False

def main():
    """
    主函数，执行每日数据更新任务
    """
    # 执行一次更新任务
    logger.info("服务启动，执行数据更新...")
    
    # 检查是否为交易日
    if not is_trading_day():
        logger.info("今天不是交易日，无需更新数据")
        return
    
    # 执行更新任务
    update_data()
    logger.info("数据更新任务执行完成，服务退出")

if __name__ == "__main__":
    main()