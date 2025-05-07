# src/utils/trading_calendar.py
"""
此模块提供交易日历相关的工具函数。
包括判断当前日期是否为交易日的函数。
Authors: hovi.hyw & AI
Date: 2024-07-03
"""

import akshare as ak
import pandas as pd
from datetime import datetime, time
import os

from ..core.logger import logger
from ..core.config import config


def is_trading_day(check_date=None):
    """
    判断指定日期是否是交易日。如果不指定日期，则判断今天。
    
    Args:
        check_date (datetime.date, optional): 要检查的日期。默认为None，表示检查今天。
    
    Returns:
        bool: 如果指定日期是交易日，则返回 True，否则返回 False。
    """
    try:
        # 如果未指定日期，使用今天的日期
        if check_date is None:
            check_date = datetime.today().date()
            
        # 将日期转换为字符串格式
        check_date_str = check_date.strftime("%Y%m%d")
        
        # 使用akshare获取交易日历
        trade_date_df = ak.tool_trade_date_hist_sina()
        # 将日期列转换为字符串格式
        trade_date_df['trade_date'] = trade_date_df['trade_date'].astype(str).str.replace('-', '')
        
        # 检查指定日期是否在交易日列表中
        return check_date_str in trade_date_df['trade_date'].values
    except Exception as e:
        logger.error(f"判断交易日失败: {e}")
        # 如果无法确定，默认为非交易日
        return False


def get_latest_trading_day():
    """
    获取最近的交易日。如果今天是交易日，则返回今天；否则返回最近的一个交易日。
    
    Returns:
        datetime.date: 最近的交易日期。
    """
    try:
        today = datetime.today().date()
        
        # 如果今天是交易日，直接返回今天
        if is_trading_day(today):
            return today
            
        # 否则，向前查找最近的交易日
        trade_date_df = ak.tool_trade_date_hist_sina()
        # 将日期列转换为日期对象
        trade_date_df['date_obj'] = pd.to_datetime(trade_date_df['trade_date']).dt.date
        # 筛选出小于今天的交易日
        past_trading_days = trade_date_df[trade_date_df['date_obj'] < today]['date_obj']
        
        if not past_trading_days.empty:
            # 返回最近的一个交易日
            return past_trading_days.iloc[-1]
        else:
            # 如果没有找到，返回今天
            return today
    except Exception as e:
        logger.error(f"获取最近交易日失败: {e}")
        # 如果无法确定，返回今天
        return today


def get_stock_list_filename_with_datetime():
    """
    获取带有当前日期时间的股票列表文件名。
    
    Returns:
        str: 带有当前日期时间的股票列表文件名。
    """
    now = datetime.now()
    date_time_str = now.strftime("%Y%m%d_%H%M%S")
    return f"stock_list_{date_time_str}.csv"


def get_stock_list_filepath_with_datetime():
    """
    获取带有当前日期时间的股票列表文件完整路径。
    
    Returns:
        str: 带有当前日期时间的股票列表文件完整路径。
    """
    filename = get_stock_list_filename_with_datetime()
    from .file_utils import ensure_directory_exists
    # 确保缓存目录存在
    ensure_directory_exists(config.CACHE_PATH)
    return os.path.join(config.CACHE_PATH, filename)