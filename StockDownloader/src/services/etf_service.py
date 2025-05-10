# src/services/etf_service.py
"""
此模块负责ETF数据的获取和处理。
实现了获取ETF列表和ETF日线数据的功能。
Authors: hovi.hyw & AI
Date: 2024-07-03
"""

import pandas as pd
import akshare as ak
from datetime import datetime

from ..core.config import config
from ..core.exceptions import DataFetchError, DataSaveError
from ..core.logger import logger
from ..database.models.etf import ETFDailyData
from ..database.models.info import ETFInfo
from ..database.session import get_db
from .data_fetcher import DataFetcher
from .data_saver import DataSaver


class ETFService:
    """
    ETF数据服务类。
    该类负责获取ETF列表和ETF日线数据，并将数据保存到数据库。
    """

    def __init__(self):
        """
        初始化ETFService实例。
        """
        # 确保数据库已初始化
        from ..utils.db_utils import initialize_database_if_needed
        initialize_database_if_needed()
        
        self.fetcher = DataFetcher()
        self.saver = DataSaver()
        self.today = datetime.today()

    def fetch_etf_list(self):
        """
        获取ETF列表。

        Returns:
            pandas.DataFrame: 包含ETF列表的DataFrame。

        Raises:
            DataFetchError: 如果获取ETF列表失败，则抛出此异常。
        """
        try:
            logger.info("获取ETF列表...")
            etf_list = ak.fund_etf_spot_em()
            # 只保留代码和名称列
            etf_list = etf_list[["代码", "名称"]]
            return etf_list
        except Exception as e:
            logger.error(f"获取ETF列表失败: {e}")
            raise DataFetchError(f"获取ETF列表失败: {e}")

    def fetch_etf_daily_data(self, symbol, start_date="20240101", end_date="20990101", adjust="hfq"):
        """
        获取ETF日线数据。

        Args:
            symbol (str): ETF代码。
            start_date (str, optional): 开始日期，格式为YYYYMMDD。默认为"20240101"。
            end_date (str, optional): 结束日期，格式为YYYYMMDD。默认为"20990101"。
            adjust (str, optional): 复权方式，可选值为"qfq"（前复权）、"hfq"（后复权）或空（不复权）。默认为"hfq"。

        Returns:
            pandas.DataFrame: 包含ETF日线数据的DataFrame。

        Raises:
            DataFetchError: 如果获取ETF日线数据失败，则抛出此异常。
        """
        try:
            logger.info(f"获取ETF {symbol} 的日线数据...")
            etf_data = ak.fund_etf_hist_em(
                symbol=symbol,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust=adjust
            )
            return etf_data
        except Exception as e:
            logger.error(f"获取ETF {symbol} 的日线数据失败: {e}")
            raise DataFetchError(f"获取ETF {symbol} 的日线数据失败: {e}")

    def save_etf_list_to_db(self, etf_list):
        """
        保存ETF列表到数据库。

        Args:
            etf_list (pandas.DataFrame): 包含ETF列表的DataFrame。

        Raises:
            DataSaveError: 如果保存ETF列表到数据库失败，则抛出此异常。
        """
        try:
            logger.info("保存ETF列表到数据库...")
            db = next(get_db())
            for _, row in etf_list.iterrows():
                symbol = row["代码"]
                name = row["名称"]
                
                # 检查是否已存在
                existing = db.query(ETFInfo).filter_by(symbol=symbol).first()
                if existing:
                    # 更新名称
                    existing.name = name
                else:
                    # 创建新记录
                    db.add(ETFInfo(symbol=symbol, name=name))
            
            db.commit()
            logger.info(f"成功保存 {len(etf_list)} 条ETF信息到数据库")
        except Exception as e:
            db.rollback()
            logger.error(f"保存ETF列表到数据库失败: {e}")
            raise DataSaveError(f"保存ETF列表到数据库失败: {e}")

    def save_etf_daily_data_to_db(self, etf_data, symbol):
        """
        保存ETF日线数据到数据库。

        Args:
            etf_data (pandas.DataFrame): 包含ETF日线数据的DataFrame。
            symbol (str): ETF代码。

        Raises:
            DataSaveError: 如果保存ETF日线数据到数据库失败，则抛出此异常。
        """
        try:
            logger.info(f"保存ETF {symbol} 的日线数据到数据库...")
            db = next(get_db())
            updated_count = 0
            inserted_count = 0
            
            for _, row in etf_data.iterrows():
                # 转换日期格式
                row_date_str = row["日期"]
                row_date = pd.to_datetime(row_date_str, errors='coerce').date()
                if pd.isna(row_date):
                    logger.warning(f"无效的日期格式: {row_date_str}")
                    continue
                
                # 检查是否已存在
                existing = db.query(ETFDailyData).filter_by(symbol=symbol, date=row_date).first()
                if existing:
                    # 更新数据
                    existing.open = row["开盘"]
                    existing.close = row["收盘"]
                    existing.high = row["最高"]
                    existing.low = row["最低"]
                    existing.volume = row["成交量"]
                    existing.amount = row["成交额"]
                    existing.amplitude = row["振幅"]
                    existing.change_rate = row["涨跌幅"]
                    existing.change_amount = row["涨跌额"]
                    existing.turnover_rate = row["换手率"]
                    updated_count += 1
                else:
                    # 创建新记录
                    new_record = ETFDailyData(
                        symbol=symbol,
                        date=row_date,
                        open=row["开盘"],
                        close=row["收盘"],
                        high=row["最高"],
                        low=row["最低"],
                        volume=row["成交量"],
                        amount=row["成交额"],
                        amplitude=row["振幅"],
                        change_rate=row["涨跌幅"],
                        change_amount=row["涨跌额"],
                        turnover_rate=row["换手率"]
                    )
                    db.add(new_record)
                    inserted_count += 1
            
            db.commit()
            logger.info(f"成功保存ETF {symbol} 的日线数据到数据库: 更新 {updated_count} 条, 插入 {inserted_count} 条")
        except Exception as e:
            db.rollback()
            logger.error(f"保存ETF {symbol} 的日线数据到数据库失败: {e}")
            raise DataSaveError(f"保存ETF {symbol} 的日线数据到数据库失败: {e}")

    def update_etf_data(self, update_only=True):
        """
        更新ETF数据。

        Args:
            update_only (bool, optional): 是否仅更新最新数据。默认为True。

        Returns:
            bool: 更新是否成功。
        """
        try:
            # 获取ETF列表
            etf_list = self.fetch_etf_list()
            
            # 保存ETF列表到数据库
            self.save_etf_list_to_db(etf_list)
            
            # 获取并保存每个ETF的日线数据
            for _, row in etf_list.iterrows():
                symbol = row["代码"]
                try:
                    # 获取ETF日线数据
                    etf_data = self.fetch_etf_daily_data(symbol)
                    
                    # 保存ETF日线数据到数据库
                    self.save_etf_daily_data_to_db(etf_data, symbol)
                except Exception as e:
                    logger.error(f"更新ETF {symbol} 的数据失败: {e}")
                    continue
            
            return True
        except Exception as e:
            logger.error(f"更新ETF数据失败: {e}")
            return False


def download_all_etf_data(update_only=True):
    """
    下载所有ETF数据的入口函数。

    Args:
        update_only (bool, optional): 是否仅更新最新数据。默认为True。

    Returns:
        bool: 下载是否成功。
    """
    try:
        etf_service = ETFService()
        return etf_service.update_etf_data(update_only=update_only)
    except Exception as e:
        logger.error(f"下载ETF数据失败: {e}")
        return False