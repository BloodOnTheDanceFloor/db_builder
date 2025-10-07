"""
股票热度排名数据更新模块
负责从 AKShare 获取股票热度数据并存储到数据库
"""
import sys
import os
import logging
from datetime import datetime
import pandas as pd
import akshare as ak
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
from functools import wraps
import time
import random

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

# 设置日志
log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../logs'))
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"hot_rank_update_{datetime.now().strftime('%Y%m%d')}.log")

# 配置日志
logger = logging.getLogger('hot_rank_update')
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

# 获取数据库连接信息
DB_USER = os.getenv("DB_USER", "si")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "stock_db")

# 构建数据库连接URL
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# 重试装饰器
def retry_with_delay(max_retries=3, initial_delay=5):
    """
    带延迟的重试装饰器
    
    Args:
        max_retries (int): 最大重试次数
        initial_delay (int): 初始延迟时间（秒）
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
                    if retries >= max_retries:
                        logger.error(f"函数 {func.__name__} 在 {max_retries} 次尝试后失败: {str(e)}")
                        raise
                    
                    # 计算下一次重试的延迟时间（指数退避 + 随机抖动）
                    jitter = random.uniform(0.1, 0.5) * delay
                    wait_time = delay + jitter
                    logger.warning(f"函数 {func.__name__} 执行失败，将在 {wait_time:.2f} 秒后重试 ({retries}/{max_retries}): {str(e)}")
                    
                    # 等待
                    time.sleep(wait_time)
                    
                    # 增加下一次的延迟时间
                    delay *= 2
                    
        return wrapper
    return decorator

@retry_with_delay(max_retries=3, initial_delay=10)
def get_stock_list():
    """
    获取股票列表
    
    Returns:
        pandas.DataFrame: 股票列表
    """
    logger.info("获取股票列表...")
    try:
        # 从缓存文件读取股票列表
        cache_dir = os.path.join(os.getcwd(), "StockDownloader", "cache")
        stock_list_file = os.path.join(cache_dir, "stock_list_latest.csv")
        
        if os.path.exists(stock_list_file):
            logger.info(f"从缓存文件 {stock_list_file} 读取股票列表")
            stock_list = pd.read_csv(stock_list_file)
            return stock_list
        
        # 如果缓存文件不存在，从AKShare获取
        logger.info("缓存文件不存在，从AKShare获取股票列表")
        stock_list = ak.stock_zh_a_spot_em()
        
        # 确保缓存目录存在
        os.makedirs(cache_dir, exist_ok=True)
        
        # 保存到缓存文件
        stock_list.to_csv(stock_list_file, index=False)
        logger.info(f"股票列表已保存到缓存文件 {stock_list_file}")
        
        return stock_list
    except Exception as e:
        logger.error(f"获取股票列表失败: {str(e)}")
        raise

@retry_with_delay(max_retries=3, initial_delay=10)
def get_stock_hot_rank(stock_code):
    """
    获取股票热度排名数据
    
    Args:
        stock_code (str): 股票代码
        
    Returns:
        pandas.DataFrame: 股票热度排名数据
    """
    logger.info(f"获取股票 {stock_code} 的热度排名数据...")
    try:
        result = ak.stock_hot_rank_detail_em(symbol=stock_code)
        
        # 检查返回值类型并进行相应处理
        if result is None:
            logger.warning(f"股票 {stock_code} 的热度排名数据为空")
            return pd.DataFrame()
        elif isinstance(result, list):
            if len(result) == 0:
                logger.warning(f"股票 {stock_code} 的热度排名数据为空列表")
                return pd.DataFrame()
            else:
                # 如果是列表，尝试转换为DataFrame
                logger.info(f"将股票 {stock_code} 的列表数据转换为DataFrame")
                df = pd.DataFrame(result)
        elif isinstance(result, pd.DataFrame):
            df = result
        else:
            logger.warning(f"股票 {stock_code} 返回了未知的数据类型: {type(result)}")
            return pd.DataFrame()
        
        logger.info(f"成功获取股票 {stock_code} 的热度排名数据，共 {len(df)} 条记录")
        return df
    except Exception as e:
        logger.error(f"获取股票 {stock_code} 的热度排名数据失败: {str(e)}")
        raise

def save_hot_rank_to_db(engine, stock_code, hot_rank_df):
    """
    保存股票热度排名数据到数据库
    
    Args:
        engine: SQLAlchemy引擎
        stock_code (str): 股票代码
        hot_rank_df (pandas.DataFrame): 股票热度排名数据
    """
    # 检查数据类型和是否为空
    if not isinstance(hot_rank_df, pd.DataFrame):
        logger.warning(f"股票 {stock_code} 的热度排名数据不是DataFrame类型: {type(hot_rank_df)}，跳过保存")
        return
    
    if hot_rank_df.empty:
        logger.warning(f"股票 {stock_code} 的热度排名数据为空，跳过保存")
        return
    
    # 统计更新和插入的数量
    update_count = 0
    insert_count = 0
    
    try:
        with engine.connect() as conn:
            # 开始事务
            with conn.begin():
                for _, row in hot_rank_df.iterrows():
                    # 转换日期格式
                    date = datetime.strptime(row['时间'], '%Y-%m-%d').date()
                    
                    # 检查是否已存在相同日期的数据
                    check_query = text("""
                        SELECT id FROM stock_hot_rank 
                        WHERE stock_code = :stock_code AND date = :date
                    """)
                    
                    result = conn.execute(check_query, {
                        "stock_code": stock_code,
                        "date": date
                    }).fetchone()
                    
                    if result:
                        # 更新现有数据
                        update_query = text("""
                            UPDATE stock_hot_rank 
                            SET rank = :rank, new_fans_ratio = :new_fans_ratio, loyal_fans_ratio = :loyal_fans_ratio
                            WHERE stock_code = :stock_code AND date = :date
                        """)
                        
                        conn.execute(update_query, {
                            "stock_code": stock_code,
                            "date": date,
                            "rank": row['排名'],
                            "new_fans_ratio": round(row['新晋粉丝'], 4),
                            "loyal_fans_ratio": round(row['铁杆粉丝'], 4)
                        })
                        
                        update_count += 1
                    else:
                        # 插入新数据
                        insert_query = text("""
                            INSERT INTO stock_hot_rank (stock_code, rank, new_fans_ratio, loyal_fans_ratio, date)
                            VALUES (:stock_code, :rank, :new_fans_ratio, :loyal_fans_ratio, :date)
                        """)
                        
                        conn.execute(insert_query, {
                            "stock_code": stock_code,
                            "date": date,
                            "rank": row['排名'],
                            "new_fans_ratio": round(row['新晋粉丝'], 4),
                            "loyal_fans_ratio": round(row['铁杆粉丝'], 4)
                        })
                        
                        insert_count += 1
                
        # 输出汇总日志
        logger.info(f"股票 {stock_code} 热度排名数据保存完成，更新{update_count}个，插入{insert_count}个。")
    except SQLAlchemyError as e:
        logger.error(f"保存股票 {stock_code} 的热度排名数据到数据库失败: {str(e)}")
        raise

def update_hot_rank_data(stock_codes=None, max_stocks=50):
    """
    更新股票热度排名数据
    
    Args:
        stock_codes (list): 要更新的股票代码列表，如果为None则更新所有股票
        max_stocks (int): 最大更新股票数量，默认为50
    """
    logger.info("开始更新股票热度排名数据...")
    
    try:
        # 创建数据库引擎
        engine = create_engine(DATABASE_URL)
        
        # 如果未指定股票代码，获取股票列表
        if not stock_codes:
            stock_list = get_stock_list()
            # 只取前max_stocks个股票
            stock_list = stock_list.head(max_stocks)
            stock_codes = stock_list['代码'].tolist()
        
        # 限制股票数量
        if len(stock_codes) > max_stocks:
            logger.info(f"股票数量超过限制，只更新前 {max_stocks} 个股票")
            stock_codes = stock_codes[:max_stocks]
        
        logger.info(f"将更新 {len(stock_codes)} 个股票的热度排名数据")
        
        # 更新每个股票的热度排名数据
        for stock_code in stock_codes:
            try:
                # 获取股票热度排名数据
                hot_rank_df = get_stock_hot_rank(stock_code)
                
                # 保存到数据库
                save_hot_rank_to_db(engine, stock_code, hot_rank_df)
                
                # 避免频繁请求API
                time.sleep(random.uniform(1, 3))
            except Exception as e:
                logger.error(f"更新股票 {stock_code} 的热度排名数据失败: {str(e)}")
                # 继续处理下一个股票
                continue
        
        logger.info("股票热度排名数据更新完成")
    except Exception as e:
        logger.error(f"更新股票热度排名数据失败: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        # 可以在这里指定要更新的股票代码
        # stock_codes = ["000001", "600000", "300059"]
        # update_hot_rank_data(stock_codes)
        
        # 或者更新前50个股票
        update_hot_rank_data(max_stocks=50)
    except Exception as e:
        logger.error(f"程序执行失败: {str(e)}")
        sys.exit(1)