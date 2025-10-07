# src/tasks/download_etf_task.py
"""
此模块作为ETF数据下载任务的入口点。
它负责协调ETF列表和ETF日线数据的获取与保存。
Authors: hovi.hyw & AI
Date: 2024-07-03
"""

import time
import random
from datetime import datetime

from ..core.logger import logger
from ..services.etf_service import ETFService


def download_all_etf_data(update_only=False):
    """
    下载所有ETF数据的入口函数。

    Args:
        update_only (bool, optional): 是否仅更新最新数据。默认为False。

    Returns:
        bool: 下载是否成功。
    """
    try:
        logger.info("开始下载ETF数据...")
        start_time = time.time()

        # 创建ETF服务实例
        etf_service = ETFService()

        # 获取ETF列表
        etf_list = etf_service.fetch_etf_list()
        
        # 保存ETF列表到数据库
        etf_service.save_etf_list_to_db(etf_list)
        
        # 获取并保存每个ETF的日线数据
        success_count = 0
        fail_count = 0
        for _, row in etf_list.iterrows():
            symbol = row["代码"]
            name = row["名称"]
            try:
                logger.info(f"下载ETF {symbol} ({name}) 的日线数据...")
                
                # 获取ETF日线数据
                etf_data = etf_service.fetch_etf_daily_data(symbol)
                
                # 保存ETF日线数据到数据库
                etf_service.save_etf_daily_data_to_db(etf_data, symbol)
                
                success_count += 1
                # 添加随机延迟，避免过度访问
                delay_time = 2 + random.uniform(1, 3)
                logger.info(f"等待 {delay_time:.2f} 秒后继续下一个ETF下载...")
                time.sleep(delay_time)
            except Exception as e:
                logger.error(f"下载ETF {symbol} ({name}) 的数据失败: {e}")
                fail_count += 1
                # 失败后增加更长的延迟
                delay_time = 5 + random.uniform(3, 8)
                logger.info(f"下载失败，等待 {delay_time:.2f} 秒后继续...")
                time.sleep(delay_time)
                continue
        
        end_time = time.time()
        elapsed_time = end_time - start_time
        logger.info(f"ETF数据下载完成，耗时 {elapsed_time:.2f} 秒，成功 {success_count} 个，失败 {fail_count} 个")
        
        return True
    except Exception as e:
        logger.error(f"下载ETF数据失败: {e}")
        return False


if __name__ == "__main__":
    download_all_etf_data()