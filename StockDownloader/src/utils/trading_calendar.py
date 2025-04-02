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


def is_trading_day():
    """
    判断今天是否是交易日。
    
    Returns:
        bool: 如果今天是交易日，则返回 True，否则返回 False。
    """
    try:
        # 获取最近的交易日历
        today = datetime.today().strftime("%Y%m%d")
        # 使用akshare获取交易日历
        trade_date_df = ak.tool_trade_date_hist_sina()
        # 将日期列转换为字符串格式
        trade_date_df['trade_date'] = trade_date_df['trade_date'].astype(str).str.replace('-', '')
        # 检查今天是否在交易日列表中
        return today in trade_date_df['trade_date'].values
    except Exception as e:
        logger.error(f"判断交易日失败: {e}")
        # 如果无法确定，默认为非交易日
        return False


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