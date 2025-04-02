# src/tasks/complete_data_task.py
"""
此模块定义了补全特定股票或指数历史数据的任务。
允许用户选择补全特定股票或指数的所有历史数据。
Authors: hovi.hyw & AI
Date: 2024-07-03
"""

import os
from datetime import date, datetime, timedelta

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from ..core.config import config
from ..core.logger import logger
from ..database.models.index import IndexDailyData
from ..database.models.stock import StockDailyData
from ..services.data_fetcher import DataFetcher
from ..services.data_saver import DataSaver
from ..utils.db_utils import initialize_database_if_needed
from ..utils.index_utils import get_index_trading_dates, get_stock_trading_dates


def complete_stock_data(symbol):
    """
    补全特定股票的历史数据。
    直接下载股票的所有历史数据，然后与数据库中已有数据比较，只保存数据库中不存在的记录。
    
    Args:
        symbol (str): 股票代码。
    """
    initialize_database_if_needed()
    # 确保 DATABASE_URL 不为空，否则抛出异常
    if config.DATABASE_URL is None:
        raise ValueError("数据库连接URL不能为空")
    
    engine = create_engine(str(config.DATABASE_URL))
    fetcher = DataFetcher()
    saver = DataSaver()
    
    # 创建数据库会话
    SessionLocal = sessionmaker(bind=engine)
    db = SessionLocal()
    
    try:
        # 获取该股票在数据库中已有的交易日期
        stock_trading_dates = get_stock_trading_dates(db, symbol)
        
        # 获取股票的所有历史数据
        # 使用较早的起始日期，确保获取尽可能完整的历史数据
        start_date = "19900101"  # 从1990年开始，覆盖大部分A股历史
        end_date = datetime.today().strftime("%Y%m%d")  # 当前日期
        
        logger.info(f"获取股票 {symbol} 从 {start_date} 到 {end_date} 的完整历史数据")
        
        try:
            # 获取股票的所有历史数据
            data = fetcher.fetch_stock_daily_data(symbol, start_date, end_date, 'hfq')
            
            # 检查返回的数据是否为None或空
            if data is None or data.empty:
                logger.warning(f"获取股票 {symbol} 的历史数据为空，无法补全")
                return
            
            # 将日期列转换为日期对象，以便与数据库中的日期比较
            data['date'] = pd.to_datetime(data['date']).dt.date
            
            # 筛选出数据库中不存在的记录
            new_data = data[~data['date'].isin(stock_trading_dates)]
            
            if new_data.empty:
                logger.info(f"股票 {symbol} 数据已完整，无需补全")
                return
            
            logger.info(f"股票 {symbol} 有 {len(new_data)} 条新数据需要补全")
            
            # 保存新数据到数据库
            saver.save_stock_daily_data_to_db(new_data, symbol)
            logger.info(f"股票 {symbol} 历史数据补全完成")
            
        except Exception as e:
            logger.error(f"获取或保存股票 {symbol} 的历史数据出错: {e}")
            return
        
        logger.info(f"股票 {symbol} 所有缺失数据补全完成")
    except Exception as e:
        logger.error(f"补全股票 {symbol} 数据出错: {e}")
    finally:
        db.close()


def complete_index_data(symbol):
    """
    补全特定指数的历史数据。
    直接下载指数的所有历史数据，然后与数据库中已有数据比较，只保存数据库中不存在的记录。
    
    Args:
        symbol (str): 指数代码。
    """
    initialize_database_if_needed()
    # 确保 DATABASE_URL 不为空，否则抛出异常
    if config.DATABASE_URL is None:
        raise ValueError("数据库连接URL不能为空")
    
    engine = create_engine(str(config.DATABASE_URL))
    fetcher = DataFetcher()
    saver = DataSaver()
    
    # 创建数据库会话
    SessionLocal = sessionmaker(bind=engine)
    db = SessionLocal()
    
    try:
        # 确保指数代码始终以字符串形式处理，并且保留前导零
        symbol = str(symbol).strip()
        # 对于纯数字的指数代码，确保格式正确（如：000001而不是1）
        if symbol.isdigit() and len(symbol) < 6:
            symbol = symbol.zfill(6)  # 补齐6位
        
        # 获取指数在数据库中的名称
        from ..database.models.info import IndexInfo
        index_info = db.query(IndexInfo).filter(IndexInfo.symbol == symbol).first()
        index_name = index_info.name if index_info else "未知指数"
        
        # 获取该指数在数据库中已有的交易日期
        index_trading_dates = get_index_trading_dates(db, symbol)
        
        # 获取指数的所有历史数据
        # 使用较早的起始日期，确保获取尽可能完整的历史数据
        start_date = "19900101"  # 从1990年开始，覆盖大部分A股历史
        end_date = datetime.today().strftime("%Y%m%d")  # 当前日期
        
        logger.info(f"获取指数 {symbol}({index_name}) 从 {start_date} 到 {end_date} 的完整历史数据")
        
        try:
            # 获取指数的所有历史数据
            data = fetcher.fetch_index_daily_data(symbol, start_date, end_date)
            
            # 检查返回的数据是否为None或空
            if data is None or data.empty:
                logger.warning(f"获取指数 {symbol}({index_name}) 的历史数据为空，无法补全")
                return
            
            # 将日期列转换为日期对象，以便与数据库中的日期比较
            data['日期'] = pd.to_datetime(data['日期']).dt.date
            
            # 筛选出数据库中不存在的记录
            new_data = data[~data['日期'].isin(index_trading_dates)]
            
            if new_data.empty:
                logger.info(f"指数 {symbol}({index_name}) 数据已完整，无需补全")
                return
            
            logger.info(f"指数 {symbol}({index_name}) 有 {len(new_data)} 条新数据需要补全")
            
            # 保存新数据到数据库
            saver.save_index_daily_data_to_db(new_data, symbol, index_name)
            logger.info(f"指数 {symbol}({index_name}) 历史数据补全完成")
            
        except Exception as e:
            logger.error(f"获取或保存指数 {symbol}({index_name}) 的历史数据出错: {e}")
            return
        
        logger.info(f"指数 {symbol}({index_name}) 所有缺失数据补全完成")
    except Exception as e:
        logger.error(f"补全指数 {symbol} 数据出错: {e}")
    finally:
        db.close()


def run_complete_data_task():
    """
    运行补全数据任务，交互式询问用户要补全的数据类型和代码。
    """
    print("\n股票/指数数据补全工具")
    print("-" * 40)
    print("1: 补全特定股票的历史数据")
    print("2: 补全特定指数的历史数据")
    print("0: 返回上级菜单")
    print("-" * 40)
    
    choice = input("请输入您的选择 (0-2): ").strip()
    
    if choice == "1":
        symbol = input("请输入要补全的股票代码: ").strip()
        if symbol:
            logger.info(f"开始补全股票 {symbol} 的历史数据...")
            complete_stock_data(symbol)
            logger.info(f"股票 {symbol} 历史数据补全任务完成")
        else:
            logger.warning("股票代码不能为空")
    elif choice == "2":
        symbol = input("请输入要补全的指数代码: ").strip()
        if symbol:
            logger.info(f"开始补全指数 {symbol} 的历史数据...")
            complete_index_data(symbol)
            logger.info(f"指数 {symbol} 历史数据补全任务完成")
        else:
            logger.warning("指数代码不能为空")
    elif choice == "0":
        return
    else:
        logger.warning(f"无效的选择: {choice}")


if __name__ == "__main__":
    run_complete_data_task()
    print