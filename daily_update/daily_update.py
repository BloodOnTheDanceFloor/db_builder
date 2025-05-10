import sys
import os
import time
from datetime import datetime, timedelta
import akshare as ak
from functools import wraps
import random
import logging
from dotenv import load_dotenv
import pandas as pd

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
from StockDownloader.src.tasks.download_etf_task import download_all_etf_data

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

@retry_with_delay(max_retries=3, initial_delay=166)  # 1分钟初始延迟
def update_stock_data():
    """
    更新股票数据，带重试机制
    """
    download_all_stock_data(update_only=True)

@retry_with_delay(max_retries=3, initial_delay=166)  # 1分钟初始延迟
def update_index_data():
    """
    更新指数数据，带重试机制
    """
    download_all_index_data(update_only=True)

@retry_with_delay(max_retries=3, initial_delay=166)  # 1分钟初始延迟
def update_etf_data():
    """
    更新ETF数据，带重试机制
    """
    download_all_etf_data(update_only=True)

def run_daily_update():
    """
    运行每日更新任务
    """
    logger.info("开始执行每日更新任务...")
        
    # 更新股票数据
    try:
        logger.info("开始更新股票数据...")
        update_stock_data()
        logger.info("股票数据更新完成")
    except Exception as e:
        logger.error(f"更新股票数据失败: {str(e)}")
    
    # 更新ETF数据
    try:
        logger.info("开始更新ETF数据...")
        update_etf_data()
        logger.info("ETF数据更新完成")
    except Exception as e:
        logger.error(f"更新ETF数据失败: {str(e)}")
    
    # 更新指数数据
    try:
        logger.info("开始更新指数数据...")
        update_index_data()
        logger.info("指数数据更新完成")
    except Exception as e:
        logger.error(f"更新指数数据失败: {str(e)}")
    
    logger.info("每日更新任务执行完成")

def get_latest_data_date():
    """
    获取数据库中最新的数据日期
    """
    from StockDownloader.src.database.session import engine
    from StockDownloader.src.database.models.stock import StockDailyData
    from StockDownloader.src.database.models.index import IndexDailyData
    from StockDownloader.src.database.models.etf import ETFDailyData
    from StockDownloader.src.tasks.update_data_task import get_latest_date_from_db
    
    # 获取各表最新日期
    stock_latest = get_latest_date_from_db(engine, StockDailyData)
    index_latest = get_latest_date_from_db(engine, IndexDailyData)
    etf_latest = get_latest_date_from_db(engine, ETFDailyData)
    
    # 返回最早的日期，确保所有数据都更新到位
    dates = [d for d in [stock_latest, index_latest, etf_latest] if d is not None]
    return min(dates) if dates else None

def get_next_trading_day(from_date):
    """
    获取指定日期之后的第一个交易日
    """
    try:
        # 获取交易日历
        df = ak.tool_trade_date_hist_sina()
        # 将日期列转换为日期对象
        df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
        # 获取大于指定日期的第一个交易日
        next_trading_days = df[df['trade_date'] > from_date]['trade_date']
        return next_trading_days.iloc[0] if not next_trading_days.empty else None
    except Exception as e:
        logger.error(f"获取下一个交易日时发生错误: {str(e)}")
        return None

def calculate_next_update_time(is_trading=False):
    """
    计算下一次更新时间
    """
    now = datetime.now()
    today = now.date()
    
    # 如果当前时间在17点之前
    if now.hour < 17:
        return datetime(today.year, today.month, today.day, 17, 0)
    # 如果当前时间在22点之后，获取下一个交易日的17点
    elif now.hour >= 22:
        next_trading = get_next_trading_day(today)
        if next_trading:
            return datetime(next_trading.year, next_trading.month, next_trading.day, 17, 0)
    # 如果是交易日且在17-22点之间，可以立即更新
    elif is_trading and 17 <= now.hour < 22:
        return now
    
    # 默认等到下一个交易日的17点
    next_trading = get_next_trading_day(today)
    if next_trading:
        return datetime(next_trading.year, next_trading.month, next_trading.day, 17, 0)
    return now + timedelta(days=1)  # 如果无法获取下一个交易日，默认等待24小时

def need_update():
    """
    判断是否需要更新数据
    """
    latest_data_date = get_latest_data_date()
    if not latest_data_date:
        return True
    
    # 获取最近的交易日
    from StockDownloader.src.utils.trading_calendar import get_latest_trading_day
    latest_trading_day = get_latest_trading_day()
    
    # 如果数据日期落后于最近的交易日，需要更新
    return latest_data_date < latest_trading_day

def main():
    """
    主函数
    """
    try:
        while True:
            now = datetime.now()
            is_trade_day = is_trading_day()
            should_update = need_update()
            
            if not should_update:
                logger.info("数据已是最新，无需更新")
                # 计算下一次更新时间
                next_update = calculate_next_update_time(is_trade_day)
            else:
                # 判断当前是否在合适的更新时间
                if is_trade_day:
                    # 交易日：如果在17-22点之间可以更新，否则等到17点
                    if 17 <= now.hour < 22:
                        logger.info("开始更新数据...")
                        run_daily_update()
                        next_update = calculate_next_update_time(is_trade_day)
                    else:
                        next_update = calculate_next_update_time(is_trade_day)
                        logger.info(f"当前时间不在更新时间范围内，等待到下一个更新时间")
                else:
                    # 非交易日：可以立即更新
                    logger.info("非交易日，开始更新数据...")
                    run_daily_update()
                    next_update = calculate_next_update_time(is_trade_day)
            
            # 计算等待时间
            wait_seconds = (next_update - now).total_seconds()
            logger.info(f"下次更新时间: {next_update.strftime('%Y-%m-%d %H:%M:%S')}, ")
            logger.info(f"等待时间: {wait_seconds/3600:.2f}小时")
            
            # 如果不是容器环境，更新一次后退出
            if os.environ.get('CONTAINER_ENV') != 'true':
                logger.info("非容器环境，更新完成后退出")
                break
                
            # 休眠到下一次更新时间
            time.sleep(wait_seconds)
            
    except Exception as e:
        logger.error(f"程序执行出错: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()