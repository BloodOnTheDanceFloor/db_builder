# src/services/data_fetcher.py
"""
此模块负责从网络API获取股票相关数据。
主要使用AKShare作为数据源，实现了重试机制和异常处理。
Authors: hovi.hyw & AI
Date: 2024-07-03
"""

import time
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from datetime import datetime, timedelta

import akshare as ak

from ..core.config import config
from ..core.exceptions import DataFetchError
from ..core.logger import logger


class DataFetcher:
    """
    数据获取类。
    该类负责从AKShare获取股票、指数和概念板块数据。

    Attributes:
        today (datetime): 当前日期。
    """

    def __init__(self):
        """
        初始化DataFetcher实例。
        """
        self.today = datetime.today()

    @staticmethod
    def fetch_stock_list():
        """
        获取股票列表。

        Returns:
            pandas.DataFrame: 包含股票列表的DataFrame。

        Raises:
            DataFetchError: 如果获取股票列表失败，则抛出此异常。
        """
        try:
            logger.info("Fetching stock list...")
            stock_list = ak.stock_zh_a_spot()
            return stock_list
        except Exception as e:
            logger.error(f"Failed to fetch stock list: {e}")
            raise DataFetchError(f"Failed to fetch stock list: {e}")

    @staticmethod
    def fetch_index_list():
        """
        获取指数列表。

        Returns:
            pandas.DataFrame: 包含指数列表的DataFrame。

        Raises:
            DataFetchError: 如果获取指数列表失败，则抛出此异常。
        """
        try:
            logger.info("Fetching index list from EastMoney...")
            index_list = ak.stock_zh_index_spot_em(symbol=config.INDICES_NAMES)
            return index_list
        except Exception as e:
            logger.error(f"Failed to fetch index list: {e}")
            raise DataFetchError(f"Failed to fetch index list: {e}")

    @staticmethod
    def _fetch_with_timeout(fetch_func, *args, **kwargs):
        """
        带超时的数据获取。

        Args:
            fetch_func (callable): 数据获取函数。
            *args: 参数。
            **kwargs: 关键字参数。

        Returns:
            Any: 数据获取函数的结果。

        Raises:
            DataFetchError: 如果操作超时，则抛出此异常。
        """
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(fetch_func, *args, **kwargs)
            try:
                return future.result(timeout=config.GET_TIMEOUT)
            except TimeoutError:
                raise DataFetchError(f"Operation timed out after {config.GET_TIMEOUT} seconds")

    @staticmethod
    def _fetch_with_retry(fetch_func, *args, max_retries=config.MAX_RETRIES, retry_delay=config.RETRY_DELAY, **kwargs):
        """
        带重试和超时的数据获取。

        Args:
            fetch_func (callable): 数据获取函数。
            *args: 参数。
            max_retries (int): 最大重试次数。
            retry_delay (int): 重试间隔（秒）。
            **kwargs: 关键字参数。

        Returns:
            Any: 数据获取函数的结果。

        Raises:
            DataFetchError: 如果在最大重试次数后仍然失败，则抛出此异常。
        """
        for attempt in range(max_retries):
            try:
                result = DataFetcher._fetch_with_timeout(fetch_func, *args, **kwargs)
                time.sleep(0.5)  # 成功后等待1秒
                return result
            except DataFetchError as e:
                logger.warning(f"Timeout on attempt {attempt + 1}/{max_retries}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    continue
                raise DataFetchError(f"Failed to fetch data after {max_retries} attempts: {str(e)}")
            except Exception as e:
                logger.warning(f"Error on attempt {attempt + 1}/{max_retries}: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    continue
                raise DataFetchError(f"Failed to fetch data after {max_retries} attempts: {e}")

    def fetch_stock_daily_data(self, symbol, start_date, end_date, adjust='hfq'):
        """
        获取股票日线数据。

        Args:
            symbol (str): 股票代码。
            start_date (str): 开始日期，格式为 YYYYMMDD。
            end_date (str): 结束日期，格式为 YYYYMMDD。
            adjust (str): 复权类型，默认为 'hfq'（后复权）。

        Returns:
            pandas.DataFrame: 包含股票日线数据的DataFrame。

        Raises:
            DataFetchError: 如果获取股票日线数据失败，则抛出此异常。
        """
        logger.info(f"Fetching daily data in mode {adjust}: for {symbol} from {start_date} to {end_date}...")
        return self._fetch_with_retry(
            ak.stock_zh_a_daily,
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            adjust=adjust
        )

    def fetch_index_daily_data(self, symbol, start_date, end_date):
        """
        获取指数日数据。

        Args:
            symbol (str): 指数代码。
            start_date (str): 开始日期，格式为 YYYYMMDD。
            end_date (str): 结束日期，格式为 YYYYMMDD。

        Returns:
            pandas.DataFrame: 包含指数日线数据的DataFrame。

        Raises:
            DataFetchError: 如果获取指数日线数据失败，则抛出此异常。
        """
        logger.info(f"Fetching daily data for index {symbol} from {start_date} to {end_date}...")
        try:
            # 确保symbol是字符串格式
            symbol = str(symbol)
            # 对于纯数字的指数代码，确保格式正确（如：000001而不是1）
            if symbol.isdigit() and len(symbol) < 6:
                symbol = symbol.zfill(6)  # 补齐6位
                
            return self._fetch_with_retry(
                ak.index_zh_a_hist,  # 修改为东财的历史数据接口
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                period="daily"
            )
        except Exception as e:
            logger.error(f"Failed to fetch index data for {symbol}: {e}")
            # 返回空DataFrame而不是抛出异常，让调用者决定如何处理
            return pd.DataFrame()


    def get_last_n_days(self, n):
        """
        获取过去 n 天的日期。

        Args:
            n (int): 天数。

        Returns:
            str: 过去 n 天的日期，格式为 YYYYMMDD。
        """
        return (self.today - timedelta(days=n)).strftime("%Y%m%d")
