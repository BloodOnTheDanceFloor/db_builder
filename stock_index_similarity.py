# stock_index_similarity.py
"""
此脚本用于比较股票与指数的年度涨跌幅，找出最接近的指数并记录到数据库中。
将结果以JSON格式存储到stock_info表的similar_indices列中。
"""

import os
import sys
import json
import pandas as pd
from sqlalchemy import create_engine, Column, String, JSON
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import text
from datetime import datetime
import logging
import argparse

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

# 导入项目模块
from StockDownloader.src.core.config import config
from StockDownloader.src.core.logger import logger
from StockDownloader.src.database.models.stock import StockDailyData
from StockDownloader.src.database.models.index import IndexDailyData
from StockDownloader.src.database.models.info import StockInfo
from StockDownloader.src.database.session import get_db

# 使用config中的数据库URL创建引擎
from sqlalchemy import create_engine
# 使用config模块的方法自动选择正确的数据库连接参数
# 该方法会根据运行环境（本地或Docker）自动选择正确的数据库主机
# 在Docker环境中使用'pgdb'作为主机名，在本地环境中使用'localhost'

# 使用config模块的get_database_url方法自动选择正确的数据库连接参数
engine = create_engine(config.get_database_url())

# 添加similar_indices列到StockInfo表
def add_similar_indices_column():
    """
    向StockInfo表添加similar_indices列，用于存储与各年度最接近的指数信息
    """
    try:
        # 检查列是否已存在
        with engine.connect() as conn:
            result = conn.execute(text("SELECT column_name FROM information_schema.columns WHERE table_name='stock_info' AND column_name='similar_indices'"))
            if result.fetchone() is None:
                # 列不存在，添加列
                conn.execute(text("ALTER TABLE stock_info ADD COLUMN similar_indices JSON"))
                logger.info("成功添加similar_indices列到stock_info表")
            else:
                logger.info("similar_indices列已存在于stock_info表中")
    except Exception as e:
        logger.error(f"添加similar_indices列时出错: {e}")
        raise

# 计算年度涨跌幅
def calculate_annual_return(data_df):
    """
    计算每年的涨跌幅
    
    Args:
        data_df: 包含日期和收盘价的DataFrame
        
    Returns:
        dict: 年份到涨跌幅的映射
    """
    # 确保日期列是datetime类型
    data_df['date'] = pd.to_datetime(data_df['date'])
    
    # 添加年份列
    data_df['year'] = data_df['date'].dt.year
    
    # 按年份分组，计算每年第一个交易日和最后一个交易日
    annual_returns = {}
    
    for year, group in data_df.groupby('year'):
        # 按日期排序
        group = group.sort_values('date')
        
        # 获取第一个和最后一个交易日的收盘价
        first_close = group.iloc[0]['close']
        last_close = group.iloc[-1]['close']
        
        # 计算年度涨跌幅
        annual_return = (last_close - first_close) / first_close
        annual_returns[year] = annual_return
    
    return annual_returns

# 查找最接近的指数
def find_most_similar_index(stock_returns, all_index_returns):
    """
    对于股票的每个年度涨跌幅，找出最接近的指数
    
    Args:
        stock_returns: 股票的年度涨跌幅字典
        all_index_returns: 所有指数的年度涨跌幅字典
        
    Returns:
        dict: 年份到最接近指数代码的映射
    """
    similar_indices = {}
    
    for year, stock_return in stock_returns.items():
        min_diff = float('inf')
        most_similar_index = None
        
        for index_symbol, index_returns in all_index_returns.items():
            if year in index_returns:
                diff = abs(stock_return - index_returns[year])
                if diff < min_diff:
                    min_diff = diff
                    most_similar_index = index_symbol
        
        if most_similar_index:
            similar_indices[str(year)] = most_similar_index
    
    return similar_indices

# 主函数
def update_stock_similar_indices(stock_symbol=None):
    """
    更新股票与指数的相似度信息
    
    Args:
        stock_symbol: 指定股票代码，如果为None则更新所有股票
    """
    # 创建数据库会话
    db = None
    try:
        # 添加similar_indices列
        add_similar_indices_column()
        
        # 创建数据库会话
        db = next(get_db())
        
        # 获取所有指数数据并计算年度涨跌幅
        logger.info("获取所有指数数据并计算年度涨跌幅...")
        index_data = db.query(IndexDailyData.symbol, IndexDailyData.date, IndexDailyData.close).all()
        index_df = pd.DataFrame([(item.symbol, item.date, item.close) for item in index_data], 
                               columns=['symbol', 'date', 'close'])
        
        # 按指数分组计算年度涨跌幅
        all_index_returns = {}
        for index_symbol, group in index_df.groupby('symbol'):
            all_index_returns[index_symbol] = calculate_annual_return(group)
        
        # 获取需要处理的股票列表
        if stock_symbol:
            stock_list = db.query(StockInfo).filter(StockInfo.symbol == stock_symbol).all()
        else:
            stock_list = db.query(StockInfo).all()
        
        # 处理每只股票
        for stock in stock_list:
            try:
                logger.info(f"处理股票 {stock.symbol}...")
                
                # 获取股票数据
                stock_data = db.query(StockDailyData.date, StockDailyData.close)\
                              .filter(StockDailyData.symbol == stock.symbol)\
                              .all()
                
                if not stock_data:
                    logger.warning(f"股票 {stock.symbol} 没有交易数据，跳过")
                    continue
                
                # 转换为DataFrame
                stock_df = pd.DataFrame([(item.date, item.close) for item in stock_data], 
                                       columns=['date', 'close'])
                
                # 计算股票的年度涨跌幅
                stock_returns = calculate_annual_return(stock_df)
                
                if not stock_returns:
                    logger.warning(f"股票 {stock.symbol} 没有足够的数据计算年度涨跌幅，跳过")
                    continue
                
                # 找出每年最接近的指数
                similar_indices = find_most_similar_index(stock_returns, all_index_returns)
                
                # 更新数据库
                stock_info = db.query(StockInfo).filter(StockInfo.symbol == stock.symbol).first()
                stock_info.similar_indices = json.dumps(similar_indices)
                
                logger.info(f"股票 {stock.symbol} 的相似指数信息已更新: {similar_indices}")
                
            except Exception as e:
                logger.error(f"处理股票 {stock.symbol} 时出错: {e}")
                continue
        
        # 提交更改
        db.commit()
        logger.info("所有股票的相似指数信息更新完成")
        
    except Exception as e:
        logger.error(f"更新股票相似指数信息时出错: {e}")
        if db is not None:
            db.rollback()
    finally:
        if db is not None:
            db.close()

# 命令行入口
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='更新股票与指数的相似度信息')
    parser.add_argument('--symbol', type=str, help='指定要处理的股票代码，例如：sz002252')
    
    args = parser.parse_args()
    
    if args.symbol:
        logger.info(f"开始处理股票 {args.symbol}...")
        update_stock_similar_indices(args.symbol)
    else:
        logger.info("开始处理所有股票...")
        update_stock_similar_indices()