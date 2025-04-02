# src/tasks/update_data_task.py

import os
from datetime import date, datetime, timedelta

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import sys
import os

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))

from StockDownloader.src.core.config import config
from StockDownloader.src.core.logger import logger
from StockDownloader.src.database.models.index import IndexDailyData
from StockDownloader.src.database.models.stock import StockDailyData
from StockDownloader.src.services.data_fetcher import DataFetcher
from StockDownloader.src.services.data_saver import DataSaver
from StockDownloader.src.utils.db_utils import initialize_database_if_needed
from StockDownloader.src.utils.index_utils import get_index_trading_dates, get_stock_trading_dates


def get_latest_date_from_db(engine, table_model):
    """从数据库中获取最新的日期"""
    SessionLocal = sessionmaker(bind=engine)
    db = SessionLocal()
    try:
        latest_record = db.query(table_model).order_by(table_model.date.desc()).first()
        if latest_record:
            return latest_record.date
        else:
            return None
    finally:
        db.close()


def update_data(engine, fetcher, saver, table_model, fetch_function, save_function, symbol_list, symbol_key):
    """基于时间范围更新数据，从上次更新日期到当前日期"""
    today = date.today()
    end_date = today.strftime("%Y%m%d")
    
    # 创建数据库会话
    SessionLocal = sessionmaker(bind=engine)
    db = SessionLocal()
    
    try:
        # 获取数据库中最新的日期
        latest_date = get_latest_date_from_db(engine, table_model)
        
        if latest_date is None:
            # 如果数据库为空，从配置的起始日期开始
            start_date = config.START_DATE
            logger.info(f"数据库为空，从配置的起始日期 {start_date} 开始下载数据")
        else:
            # 从最新日期的下一天开始更新
            next_day = latest_date + timedelta(days=1)
            start_date = next_day.strftime("%Y%m%d")
            logger.info(f"从上次更新日期 {latest_date} 的下一天开始更新数据")
        
        # 如果开始日期等于或晚于结束日期，则无需更新
        if start_date >= end_date:
            logger.info(f"数据库已是最新，无需更新")
            return
        
        logger.info(f"更新数据范围: {start_date} 到 {end_date}")
        
        # 遍历所有股票/指数
        for _, row in symbol_list.iterrows():
            # 确保代码始终以字符串形式处理
            symbol = str(row[symbol_key]).strip()
            
            # 对于纯数字的代码，确保格式正确（如：000001而不是1）
            if symbol.isdigit() and len(symbol) < 6:
                symbol = symbol.zfill(6)  # 补齐6位
            
            try:
                # 获取指定时间范围的数据
                if table_model == IndexDailyData:
                    logger.info(f"获取指数 {symbol} 从 {start_date} 到 {end_date} 的数据")
                else:
                    logger.info(f"获取股票 {symbol} 从 {start_date} 到 {end_date} 的数据")
                
                # 调用相应的数据获取函数
                if table_model == StockDailyData:
                    data = fetch_function(symbol, start_date, end_date, 'hfq')
                else:
                    data = fetch_function(symbol, start_date, end_date)
                
                # 检查返回的数据是否为None或空
                if data is None or data.empty:
                    if table_model == IndexDailyData:
                        logger.warning(f"获取指数 {symbol} 的数据为空，跳过保存")
                    else:
                        logger.warning(f"获取股票 {symbol} 的数据为空，跳过保存")
                    continue
                
                # 保存数据到数据库
                if table_model == IndexDailyData and '名称' in row:
                    save_function(data, symbol, row['名称'])
                else:
                    save_function(data, symbol)
                
                if table_model == IndexDailyData:
                    logger.info(f"指数 {symbol} 数据更新完成")
                else:
                    logger.info(f"股票 {symbol} 数据更新完成")
                    
            except Exception as e:
                if table_model == IndexDailyData:
                    logger.error(f"更新指数 {symbol} 数据出错: {e}")
                else:
                    logger.error(f"更新股票 {symbol} 数据出错: {e}")
    finally:
        db.close()


def update_stock_data():
    """只更新股票数据"""
    initialize_database_if_needed()
    # 确保 DATABASE_URL 不为空，否则抛出异常
    if config.DATABASE_URL is None:
        raise ValueError("数据库连接URL不能为空")
    engine = create_engine(str(config.DATABASE_URL))
    fetcher = DataFetcher()
    saver = DataSaver()

    # 更新股票数据
    cache_dir = os.path.join(os.getcwd(), "cache")
    # 确保缓存目录存在
    from ..utils.file_utils import ensure_directory_exists
    ensure_directory_exists(cache_dir)
    stock_list_file = os.path.join(cache_dir, "stock_list.csv")
    
    # 直接检查文件是否存在，不再检查文件的有效期
    if os.path.exists(stock_list_file):
        logger.info("从缓存读取股票列表")
        stock_list = pd.read_csv(stock_list_file)
    else:
        logger.info("从API获取股票列表")
        stock_list = fetcher.fetch_stock_list()
        saver.save_stock_list_to_csv(stock_list, stock_list_file)
        
    update_data(
        engine,
        fetcher,
        saver,
        StockDailyData,
        fetcher.fetch_stock_daily_data,
        saver.save_stock_daily_data_to_db,
        stock_list,
        "代码"
    )
    
    logger.info("股票数据更新任务完成")


def update_index_data():
    """只更新指数数据"""
    initialize_database_if_needed()
    # 确保 DATABASE_URL 不为空，否则抛出异常
    if config.DATABASE_URL is None:
        raise ValueError("数据库连接URL不能为空")
    engine = create_engine(str(config.DATABASE_URL))
    fetcher = DataFetcher()
    saver = DataSaver()

    # 更新指数数据
    cache_dir = os.path.join(os.getcwd(), "cache")
    # 确保缓存目录存在
    from ..utils.file_utils import ensure_directory_exists
    ensure_directory_exists(cache_dir)
    index_list_file = os.path.join(cache_dir, "index_list.csv")
    
    # 直接检查文件是否存在，不再检查文件的有效期
    if os.path.exists(index_list_file):
        logger.info("从缓存读取指数列表")
        index_list = pd.read_csv(index_list_file)
    else:
        logger.info("从东方财富获取指数列表")
        index_list = fetcher.fetch_index_list()
        saver.save_index_list_to_csv(index_list, index_list_file)
    
    update_data(
        engine,
        fetcher,
        saver,
        IndexDailyData,
        fetcher.fetch_index_daily_data,
        saver.save_index_daily_data_to_db,
        index_list,
        "代码"
    )
    
    logger.info("指数数据更新任务完成")


def update_all_data():
    initialize_database_if_needed()
    # 确保 DATABASE_URL 不为空，否则抛出异常
    if config.DATABASE_URL is None:
        raise ValueError("数据库连接URL不能为空")
    engine = create_engine(str(config.DATABASE_URL))
    fetcher = DataFetcher()
    saver = DataSaver()

    # 更新股票数据
    cache_dir = os.path.join(os.getcwd(), "cache")
    # 确保缓存目录存在
    from ..utils.file_utils import ensure_directory_exists
    ensure_directory_exists(cache_dir)
    stock_list_file = os.path.join(cache_dir, "stock_list.csv")
    
    # 直接检查文件是否存在，不再检查文件的有效期
    if os.path.exists(stock_list_file):
        logger.info("从缓存读取股票列表")
        stock_list = pd.read_csv(stock_list_file)
    else:
        logger.info("从API获取股票列表")
        stock_list = fetcher.fetch_stock_list()
        saver.save_stock_list_to_csv(stock_list, stock_list_file)
        
    update_data(
        engine,
        fetcher,
        saver,
        StockDailyData,
        fetcher.fetch_stock_daily_data,
        saver.save_stock_daily_data_to_db,
        stock_list,
        "代码"
    )

    # 更新指数数据
    cache_dir = os.path.join(os.getcwd(), "cache")
    # 确保缓存目录存在
    from ..utils.file_utils import ensure_directory_exists
    ensure_directory_exists(cache_dir)
    index_list_file = os.path.join(cache_dir, "index_list.csv")
    
    # 直接检查文件是否存在，不再检查文件的有效期
    if os.path.exists(index_list_file):
        logger.info("从缓存读取指数列表")
        index_list = pd.read_csv(index_list_file)
    else:
        logger.info("从东方财富获取指数列表")
        index_list = fetcher.fetch_index_list()
        saver.save_index_list_to_csv(index_list, index_list_file)
        
    update_data(
        engine,
        fetcher,
        saver,
        IndexDailyData,
        fetcher.fetch_index_daily_data,
        saver.save_index_daily_data_to_db,
        index_list,
        "代码"
    )

    logger.info("所有数据更新任务完成")


if __name__ == "__main__":
    update_stock_data()