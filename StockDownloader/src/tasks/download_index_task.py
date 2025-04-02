# src/tasks/download_index_task.py
"""
此模块定义了下载指数数据的后台任务。
使用Celery或其他任务队列，定期从AKShare获取指数日线数据，并保存到数据库。
Authors: hovi.hyw & AI
Date: 2024-07-03
"""

from datetime import datetime

import pandas as pd

from ..core.config import config
from ..core.logger import logger
from ..services.data_fetcher import DataFetcher
from ..services.data_saver import DataSaver


def format_index_code(symbol):
    """确保指数代码为6位数字格式"""
    return str(symbol).zfill(6)


def download_index_data(symbol: str, name: str):
    """
    下载指定指数的日线数据，并保存到数据库。

    Args:
        symbol (str): 指数代码。
        name (str): 指数名称
    """
    fetcher = DataFetcher()
    saver = DataSaver()
    formatted_symbol = format_index_code(symbol)
    try:
        index_data = fetcher.fetch_index_daily_data(
            formatted_symbol,
            config.START_DATE,
            datetime.today().strftime("%Y%m%d")
        )
        if index_data is None:
            logger.warning(f"未能获取到指数 {formatted_symbol}({name}) 的数据")
            return
        saver.save_index_daily_data_to_db(index_data, formatted_symbol, name)
        logger.info(f"指数 {formatted_symbol}({name}) 日数据下载并保存完成")
    except Exception as e:
        logger.error(f"处理指数 {formatted_symbol}({name}) 时出错: {e}")


def download_all_index_data(update_only=False):
    """
    下载所有指数的日线数据，并保存到数据库。
    
    Args:
        update_only (bool, optional): 是否只更新最新数据。默认为False，表示下载全部历史数据。
    """
    fetcher = DataFetcher()
    saver = DataSaver()

    # 获取指数列表
    from ..utils.file_utils import check_file_validity, ensure_directory_exists
    # 确保缓存目录存在
    ensure_directory_exists(config.CACHE_PATH)
    index_list_file = config.CACHE_PATH + "/index_list.csv"
    max_age = config.MAX_CSV_AGE_DAYS
    if check_file_validity(index_list_file, max_age):
        logger.info("从缓存读取指数列表")
        index_list = pd.read_csv(index_list_file)
    else:
        logger.info("从东方财富获取指数列表")
        index_list = fetcher.fetch_index_list()
        saver.save_index_list_to_csv(index_list, index_list_file)
    
    # 保存指数基本信息到数据库
    logger.info("保存指数基本信息到数据库")
    saver.save_index_info_to_db(index_list)

    # 下载指数日数据并保存到数据库
    if update_only:
        # 如果只更新最新数据，则调用update_index_data函数
        from .update_data_task import update_index_data
        update_index_data()
        logger.info("指数数据增量更新任务完成")
    else:
        # 否则下载全部历史数据
        for _, row in index_list.iterrows():
            symbol = row["代码"]
            name = row["名称"]
            download_index_data(symbol, name)

        logger.info("所有指数数据下载任务完成")