# src/services/stock_list_service.py
"""
此模块作为股票列表下载服务的入口点。
它负责在交易日的特定时间执行股票列表下载任务。
Authors: hovi.hyw & AI
Date: 2024-07-03
"""

import time
from datetime import datetime
import os

from ..core.logger import logger
from ..services.data_fetcher import DataFetcher
from ..services.data_saver import DataSaver
from ..utils.trading_calendar import is_trading_day, get_stock_list_filepath_with_datetime
from ..core.config import config


def download_stock_list_task():
    """
    下载股票列表并保存为带有日期时间的文件名。
    """
    try:
        logger.info("开始执行股票列表下载任务...")
        fetcher = DataFetcher()
        saver = DataSaver()
        
        # 获取股票列表
        stock_list = fetcher.fetch_stock_list()
        
        # 获取带有日期时间的文件路径
        file_path = get_stock_list_filepath_with_datetime()
        
        # 保存股票列表
        saver.save_stock_list_to_csv(stock_list, file_path)
        
        # 同时保存一份到标准位置，以便其他功能使用
        from ..utils.file_utils import ensure_directory_exists
        # 确保缓存目录存在
        ensure_directory_exists(config.CACHE_PATH)
        standard_path = os.path.join(config.CACHE_PATH, "stock_list.csv")
        saver.save_stock_list_to_csv(stock_list, standard_path)
        
        logger.info(f"股票列表下载任务执行完成，已保存到 {file_path}")
        return True
    except Exception as e:
        logger.error(f"股票列表下载任务执行失败: {e}")
        return False


def check_and_execute_task():
    """
    检查当前是否是交易日的9:26，如果是则执行下载任务。
    """
    now = datetime.now()
    current_time = now.time()
    
    # 检查是否是交易日
    if is_trading_day():
        # 检查是否是上午9:26
        target_time = datetime.strptime("09:26", "%H:%M").time()
        
        # 允许1分钟的误差范围
        time_diff_seconds = abs(
            (current_time.hour * 3600 + current_time.minute * 60 + current_time.second) - 
            (target_time.hour * 3600 + target_time.minute * 60)
        )
        
        if time_diff_seconds <= 60:  # 在目标时间的1分钟内
            return download_stock_list_task()
    
    return False


def run_service():
    """
    运行股票列表下载服务。
    每分钟检查一次是否需要执行任务。
    """
    logger.info("股票列表下载服务已启动")
    
    while True:
        try:
            result = check_and_execute_task()
            if result:
                logger.info("今日股票列表下载任务已完成")
        except Exception as e:
            logger.error(f"任务检查执行失败: {e}")
        
        # 每分钟检查一次
        time.sleep(60)


if __name__ == "__main__":
    run_service()