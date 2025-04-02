# src/main.py
"""
此模块是应用程序的主入口点。
它负责初始化数据库、启动 FastAPI 应用，并定期执行数据下载任务。
支持通过命令行参数指定特定的数据下载或更新任务。
Authors: hovi.hyw & AI
Date: 2024-07-03
"""

import argparse
import sys
import threading
import time

import uvicorn

from .api.app import app
from .core.logger import logger
from .tasks.download_index_task import download_all_index_data, download_index_data
from .tasks.download_stock_task import download_all_stock_data, download_stock_task
from .services.data_fetcher import DataFetcher
from .services.data_saver import DataSaver
from .tasks.scheduled_tasks import start_scheduled_tasks
from .tasks.complete_data_task import run_complete_data_task
from .utils.db_utils import initialize_database_if_needed


def run_scheduled_tasks():
    """
    定期运行数据下载任务。
    """
    while True:
        logger.info("开始执行定时数据下载任务...")
        download_all_stock_data(update_only=True)
        download_all_index_data(update_only=True)
        logger.info("定时数据下载任务执行完成，等待下次执行...")
        time.sleep(86400)  # 每天执行一次，86400秒 = 1天


def ensure_directories():
    """
    确保必要的目录存在。
    """
    import os
    from .core.config import config
    
    # 确保缓存目录存在
    if not os.path.exists(config.CACHE_PATH):
        logger.info(f"缓存目录 '{config.CACHE_PATH}' 不存在，正在创建...")
        os.makedirs(config.CACHE_PATH, exist_ok=True)
        logger.info(f"缓存目录 '{config.CACHE_PATH}' 创建成功")
    
    # 确保日志目录存在
    log_dir = os.path.dirname(config.LOG_FILE)
    if not os.path.exists(log_dir):
        logger.info(f"日志目录 '{log_dir}' 不存在，正在创建...")
        os.makedirs(log_dir, exist_ok=True)
        logger.info(f"日志目录 '{log_dir}' 创建成功")


def parse_args():
    """
    解析命令行参数。
    
    Returns:
        argparse.Namespace: 解析后的参数对象。
    """
    parser = argparse.ArgumentParser(description="股票数据下载与API服务")
    parser.add_argument(
        "--mode", 
        type=int, 
        choices=range(1, 9),
        help="运行模式：\n"
             "1：只下载指数日线数据\n"
             "2：只下载股票日线数据\n"
             "3：只更新指数日线数据\n"
             "4：只更新股票日线数据\n"
             "5：只下载股票和指数日线数据\n"
             "6：只更新股票和指数日线数据\n"
             "7：更新stock_info以及index_info表\n"
             "8：补全特定股票或指数的历史数据"
    )
    
    return parser.parse_args()


def update_stock_and_index_info():
    """
    更新股票和指数的基本信息表。
    """
    logger.info("开始更新股票和指数基本信息...")
    fetcher = DataFetcher()
    saver = DataSaver()
    
    # 更新股票基本信息
    logger.info("从API获取股票列表")
    stock_list = fetcher.fetch_stock_list()
    logger.info("保存股票基本信息到数据库")
    saver.save_stock_info_to_db(stock_list)
    
    # 更新指数基本信息
    logger.info("从API获取指数列表")
    index_list = fetcher.fetch_index_list()
    logger.info("保存指数基本信息到数据库")
    saver.save_index_info_to_db(index_list)
    
    logger.info("股票和指数基本信息更新完成")


def main():
    """
    主函数。
    根据命令行参数执行特定任务，或者启动完整的API服务。
    """
    # 确保必要的目录存在
    ensure_directories()
    
    # 初始化数据库
    initialize_database_if_needed()
    
    # 解析命令行参数
    args = parse_args()
    
    # 如果指定了运行模式，执行特定任务
    if args.mode is not None:
        if args.mode == 1:  # 只下载指数日线数据
            logger.info("开始下载指数日线数据...")
            download_all_index_data()
            logger.info("指数日线数据下载完成")
        elif args.mode == 2:  # 只下载股票日线数据
            logger.info("开始下载股票日线数据...")
            download_all_stock_data()
            logger.info("股票日线数据下载完成")
        elif args.mode == 3:  # 只更新指数日线数据
            logger.info("开始更新指数日线数据...")
            download_all_index_data(update_only=True)
            logger.info("指数日线数据更新完成")
        elif args.mode == 4:  # 只更新股票日线数据
            logger.info("开始更新股票日线数据...")
            download_all_stock_data(update_only=True)
            logger.info("股票日线数据更新完成")
        elif args.mode == 5:  # 只下载股票和指数日线数据
            logger.info("开始下载股票和指数日线数据...")
            download_all_stock_data()
            logger.info("股票日线数据下载完成，等待1分钟后开始下载指数日线数据...")
            time.sleep(60)  # 等待1分钟
            download_all_index_data()
            logger.info("股票和指数日线数据下载完成")
        elif args.mode == 6:  # 只更新股票和指数日线数据
            logger.info("开始更新股票和指数日线数据...")
            download_all_stock_data(update_only=True)
            logger.info("股票日线数据更新完成，等待1分钟后开始更新指数日线数据...")
            time.sleep(60)  # 等待1分钟
            download_all_index_data(update_only=True)
            logger.info("股票和指数日线数据更新完成")
        elif args.mode == 7:  # 更新stock_info以及index_info表
            update_stock_and_index_info()
        elif args.mode == 8:  # 补全特定股票或指数的历史数据
            run_complete_data_task()
        
        # 执行完特定任务后退出
        sys.exit(0)
    
    # 如果没有指定运行模式，启动完整的API服务和定时任务
    
    # 启动每日数据下载定时任务
    task_thread = threading.Thread(target=run_scheduled_tasks)
    task_thread.daemon = True  # 设置为守护线程，主线程退出时自动结束
    task_thread.start()
    
    # 启动交易日特定时间任务（股票列表下载）
    start_scheduled_tasks()

    # 启动API服务
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()
