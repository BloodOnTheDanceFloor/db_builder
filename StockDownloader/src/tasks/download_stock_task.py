# src/tasks/download_stock_task.py
"""
此模块定义了下载股票数据的后台任务。
使用Celery或其他任务队列，定期从AKShare获取股票日线数据，并保存到数据库。
Authors: hovi.hyw & AI
Date: 2024-07-03
"""

from datetime import datetime

import pandas as pd

from ..core.config import config
from ..core.logger import logger
from ..services.data_fetcher import DataFetcher
from ..services.data_saver import DataSaver


def download_stock_task(symbol: str):
    """
    下载指定股票的日线数据，并保存到数据库。

    Args:
        symbol (str): 股票代码。
    """
    fetcher = DataFetcher()
    saver = DataSaver()

    try:
        stock_data = fetcher.fetch_stock_daily_data(symbol, config.START_DATE, datetime.today().strftime("%Y%m%d"), 'hfq')
        if stock_data is not None and not stock_data.empty:
            saver.save_stock_daily_data_to_db(stock_data, symbol)
            logger.info(f"股票 {symbol} 日数据下载并保存完成")
        else:
            logger.warning(f"未能获取到股票 {symbol} 的数据")
    except Exception as e:
        logger.error(f"下载股票 {symbol} 数据时出错: {e}")


def download_all_stock_data(update_only=False):
    """
    下载所有股票的日线数据，并保存到数据库。
    
    Args:
        update_only (bool, optional): 是否只更新最新数据。默认为False，表示下载全部历史数据。
    """
    fetcher = DataFetcher()
    saver = DataSaver()
    # 获取股票列表
    from ..utils.file_utils import check_file_validity, ensure_directory_exists
    # 确保缓存目录存在
    ensure_directory_exists(config.CACHE_PATH)
    stock_list_file = config.CACHE_PATH + "/stock_list_latest.csv"
    max_age = config.MAX_CSV_AGE_DAYS
    if check_file_validity(stock_list_file, max_age):
        logger.info("从缓存读取股票列表")
        stock_list = pd.read_csv(stock_list_file)
    else:
        logger.info("从API获取股票列表")
        stock_list = fetcher.fetch_stock_list()
        saver.save_stock_list_to_csv(stock_list, stock_list_file)
    
    # 保存股票基本信息到数据库
    logger.info("保存股票基本信息到数据库")
    saver.save_stock_info_to_db(stock_list)

    # 下载股票日数据并保存到数据库
    if update_only:
        # 如果只更新最新数据，则调用update_stock_data函数
        from .update_data_task import update_stock_data
        update_stock_data()
        logger.info("股票数据增量更新任务完成")
    else:
        # 否则下载全部历史数据
        for symbol in stock_list["代码"]:
            download_stock_task(symbol)
        logger.info("所有股票数据下载任务完成")