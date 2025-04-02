# src/utils/index_utils.py
"""
此模块包含与指数相关的工具函数。
主要用于获取指数的交易日期，作为股票数据更新的基准。
Authors: hovi.hyw & AI
Date: 2024-07-03
"""

from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker

from ..core.logger import logger
from ..database.models.index import IndexDailyData
from ..database.session import engine


def get_index_trading_dates(db: Session, index_symbol="000001"):
    """
    获取指定指数的所有交易日期。
    
    Args:
        db (Session): 数据库会话。
        index_symbol (str): 指数代码，默认为上证指数(000001)。
        
    Returns:
        set: 包含所有交易日期的集合。
    """
    try:
        # 查询指定指数的所有交易日期
        dates = db.query(IndexDailyData.date).filter(
            IndexDailyData.symbol == index_symbol
        ).order_by(IndexDailyData.date).all()
        
        # 将查询结果转换为日期集合
        trading_dates = {date[0] for date in dates}
        
        logger.info(f"获取到指数 {index_symbol} 的交易日期 {len(trading_dates)} 个")
        return trading_dates
    except Exception as e:
        logger.error(f"获取指数 {index_symbol} 交易日期出错: {e}")
        return set()


def get_stock_trading_dates(db: Session, stock_symbol):
    """
    获取指定股票的所有交易日期。
    
    Args:
        db (Session): 数据库会话。
        stock_symbol (str): 股票代码。
        
    Returns:
        set: 包含所有交易日期的集合。
    """
    from ..database.models.stock import StockDailyData
    
    try:
        # 查询指定股票的所有交易日期
        dates = db.query(StockDailyData.date).filter(
            StockDailyData.symbol == stock_symbol
        ).order_by(StockDailyData.date).all()
        
        # 将查询结果转换为日期集合
        trading_dates = {date[0] for date in dates}
        
        return trading_dates
    except Exception as e:
        logger.error(f"获取股票 {stock_symbol} 交易日期出错: {e}")
        return set()