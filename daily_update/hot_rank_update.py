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
        pandas.DataFrame: 股票热度排名数据，包含 '时间', '排名', '新晋粉丝', '铁杆粉丝' 列
    """
    logger.info(f"获取股票 {stock_code} 的热度排名数据...")
    try:
        # 创建一个空的结果 DataFrame，包含所需的列
        result_df = pd.DataFrame(columns=['时间', '排名', '新晋粉丝', '铁杆粉丝'])
        
        # 修正股票代码前缀，北交所股票(BJ开头)需要改为SZ前缀才能获取数据
        symbol = stock_code
        if stock_code.upper().startswith("BJ"):
            symbol = "SZ" + stock_code[2:]
            logger.info(f"北交所股票 {stock_code} 已转换为 {symbol} 以获取数据")
        
        # 调用 akshare 函数获取数据
        try:
            raw_data = ak.stock_hot_rank_detail_em(symbol=symbol)
        except Exception as e:
            logger.warning(f"调用 akshare.stock_hot_rank_detail_em 失败: {str(e)}")
            return result_df
        
        # 检查返回的数据是否为空
        if raw_data is None:
            logger.warning(f"股票 {stock_code} 的热度排名数据为空")
            return result_df
        
        # 处理不同的返回类型情况
        if isinstance(raw_data, pd.DataFrame):
            # 如果返回的是 DataFrame
            if raw_data.empty:
                logger.warning(f"股票 {stock_code} 的热度排名数据为空 DataFrame")
                return result_df
            
            # 检查并处理列名
            if '时间' in raw_data.columns and '排名' in raw_data.columns:
                # 如果包含必要的列，提取需要的列
                for col in ['时间', '排名', '新晋粉丝', '铁杆粉丝']:
                    if col not in raw_data.columns:
                        # 如果缺少某列，添加默认值
                        raw_data[col] = 0 if col != '时间' else datetime.now().strftime('%Y-%m-%d')
                
                # 只保留需要的列
                result_df = raw_data[['时间', '排名', '新晋粉丝', '铁杆粉丝']].copy()
                logger.info(f"成功获取股票 {stock_code} 的热度排名数据，共 {len(result_df)} 条记录")
                return result_df
            else:
                # 如果列名不匹配，尝试根据位置提取
                logger.warning(f"股票 {stock_code} 的热度排名数据列名不匹配: {raw_data.columns.tolist()}")
                try:
                    # 假设第一列是时间，第二列是排名，依此类推
                    if len(raw_data.columns) >= 4:
                        new_df = pd.DataFrame({
                            '时间': raw_data.iloc[:, 0],
                            '排名': raw_data.iloc[:, 1],
                            '新晋粉丝': raw_data.iloc[:, 2],
                            '铁杆粉丝': raw_data.iloc[:, 3]
                        })
                        logger.info(f"通过位置映射成功提取股票 {stock_code} 的热度排名数据")
                        return new_df
                except Exception as e:
                    logger.error(f"尝试通过位置提取股票 {stock_code} 的热度排名数据失败: {str(e)}")
                    return result_df
        elif isinstance(raw_data, tuple) or isinstance(raw_data, list):
            # 如果返回的是元组或列表
            logger.warning(f"股票 {stock_code} 返回了 {type(raw_data)} 类型的数据，尝试转换")
            try:
                # 尝试将元组或列表转换为 DataFrame
                if len(raw_data) >= 1 and isinstance(raw_data[0], pd.DataFrame):
                    # 如果第一个元素是 DataFrame
                    df = raw_data[0]
                    if not df.empty and len(df.columns) >= 4:
                        new_df = pd.DataFrame({
                            '时间': df.iloc[:, 0],
                            '排名': df.iloc[:, 1],
                            '新晋粉丝': df.iloc[:, 2],
                            '铁杆粉丝': df.iloc[:, 3]
                        })
                        logger.info(f"成功从元组/列表中提取股票 {stock_code} 的热度排名数据")
                        return new_df
                elif len(raw_data) >= 4:
                    # 如果元组/列表本身包含足够的元素
                    today = datetime.now().strftime('%Y-%m-%d')
                    new_df = pd.DataFrame({
                        '时间': [today],
                        '排名': [raw_data[0] if isinstance(raw_data[0], (int, float)) else 0],
                        '新晋粉丝': [raw_data[1] if isinstance(raw_data[1], (int, float)) else 0],
                        '铁杆粉丝': [raw_data[2] if isinstance(raw_data[2], (int, float)) else 0]
                    })
                    logger.info(f"成功从元组/列表元素创建股票 {stock_code} 的热度排名数据")
                    return new_df
            except Exception as e:
                logger.error(f"尝试转换股票 {stock_code} 的元组/列表数据失败: {str(e)}")
                return result_df
        else:
            # 其他类型的数据
            logger.warning(f"股票 {stock_code} 返回了不支持的数据类型: {type(raw_data)}")
            return result_df
        
        # 如果所有尝试都失败，返回空 DataFrame
        logger.warning(f"无法处理股票 {stock_code} 的热度排名数据，返回空结果")
        return result_df
    except Exception as e:
        logger.error(f"获取股票 {stock_code} 的热度排名数据失败: {str(e)}")
        # 返回空 DataFrame 而不是抛出异常，让程序继续运行
        return pd.DataFrame(columns=['时间', '排名', '新晋粉丝', '铁杆粉丝'])

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

def update_hot_rank_data(stock_codes=None, max_stocks=None):
    """
    更新股票热度排名数据
    
    Args:
        stock_codes (list): 要更新的股票代码列表，如果为None则更新所有股票
        max_stocks (int): 最大更新股票数量，默认为None表示更新所有股票
    """
    logger.info("开始更新股票热度排名数据...")
    
    try:
        # 创建数据库引擎
        engine = create_engine(DATABASE_URL)
        
        # 如果未指定股票代码，获取股票列表
        if not stock_codes:
            stock_list = get_stock_list()
            # 如果指定了最大数量，则只取前max_stocks个股票
            if max_stocks:
                stock_list = stock_list.head(max_stocks)
            stock_codes = stock_list['代码'].tolist()
        
        # 如果指定了最大数量，则限制股票数量
        if max_stocks and len(stock_codes) > max_stocks:
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
        update_hot_rank_data()
    except Exception as e:
        logger.error(f"程序执行失败: {str(e)}")
        sys.exit(1)