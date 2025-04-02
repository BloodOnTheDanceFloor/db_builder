# src/services/data_saver.py
"""
此模块负责将股票相关数据保存到数据库。
实现了将股票列表、日线数据保存到数据库的功能，并处理保存过程中的异常。
Authors: hovi.hyw & AI
Date: 2024-07-03
"""

import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

from ..core.exceptions import DataSaveError
from ..core.logger import logger
from ..database.models.index import IndexDailyData
from ..database.models.stock import StockDailyData
from ..database.models.info import StockInfo, IndexInfo
from ..database.session import get_db


class DataSaver:
    """
    数据保存类。
    该类负责将股票、指数和概念板块数据保存到数据库。
    """

    def save_stock_list_to_csv(self, stock_list, file_path):
        """
        保存股票列表到 CSV 文件。

        Args:
            stock_list (pandas.DataFrame): 包含股票列表的DataFrame。
            file_path (str): CSV 文件路径。

        Raises:
            DataSaveError: 如果保存股票列表到 CSV 文件失败，则抛出此异常。
        """
        try:
            logger.info(f"Saving stock list to {file_path}...")
            stock_list.to_csv(file_path, index=False)
        except Exception as e:
            logger.error(f"Failed to save stock list to CSV: {e}")
            raise DataSaveError(f"Failed to save stock list to CSV: {e}")

    def save_index_list_to_csv(self, index_list, file_path):
        """
        保存指数列表到 CSV 文件。

        Args:
            index_list (pandas.DataFrame): 包含指数列表的DataFrame。
            file_path (str): CSV 文件路径。

        Raises:
            DataSaveError: 如果保存指数列表到 CSV 文件失败，则抛出此异常。
        """
        try:
            logger.info(f"Saving index list to {file_path}...")
            index_list.to_csv(file_path, index=False)
        except Exception as e:
            logger.error(f"Failed to save index list to CSV: {e}")
            raise DataSaveError(f"Failed to save index list to CSV: {e}")

    def save_stock_daily_data_to_db(self, stock_data, symbol):
        """
        保存股票日数据到数据库，仅更新日期较新的数据。

        Args:
            stock_data (pandas.DataFrame): 包含股票日线数据的DataFrame。
            symbol (str): 股票代码。

        Raises:
            DataSaveError: 如果保存股票日线数据到数据库失败，则抛出此异常。
        """
        try:
            logger.info(f"Saving daily data for stock {symbol} to database...")
            db: Session = next(get_db())
            updated_count = 0
            inserted_count = 0
            for _, row in stock_data.iterrows():
                row_date_str = row["date"]
                row_date = pd.to_datetime(row_date_str, errors='coerce').date()
                if pd.isna(row_date):
                    logger.warning(f"Invalid date format: {row_date_str}")
                    continue

                existing_record = db.query(StockDailyData).filter_by(symbol=symbol, date=row_date).first()
                if existing_record:
                    if row_date > existing_record.date:
                        existing_record.open = row["open"]
                        existing_record.close = row["close"]
                        existing_record.high = row["high"]
                        existing_record.low = row["low"]
                        existing_record.volume = row["volume"]
                        existing_record.amount = row["amount"]
                        existing_record.outstanding_share = row["outstanding_share"]
                        existing_record.turnover = row["turnover"]
                        updated_count += 1
                else:
                    db.add(StockDailyData(
                        symbol=symbol,
                        date=row_date,
                        open=row["open"],
                        close=row["close"],
                        high=row["high"],
                        low=row["low"],
                        volume=row["volume"],
                        amount=row["amount"],
                        outstanding_share=row["outstanding_share"],
                        turnover=row["turnover"]
                    ))
                    inserted_count += 1
            db.commit()
            logger.info(
                f"Updated {updated_count} records and inserted {inserted_count} new records for stock {symbol}.")
        except Exception as e:
            db.rollback()
            logger.error(f"Failed to save daily data for stock {symbol} to database: {e}")
            raise DataSaveError(f"Failed to save daily data for stock {symbol} to database: {e}")

    def save_stock_info_to_db(self, stock_list):
        """
        保存股票基本信息到数据库。

        Args:
            stock_list (pandas.DataFrame): 包含股票列表的DataFrame，必须包含'代码'和'名称'列。

        Raises:
            DataSaveError: 如果保存股票基本信息到数据库失败，则抛出此异常。
        """
        try:
            logger.info("Saving stock info to database...")
            db: Session = next(get_db())
            inserted_count = 0
            updated_count = 0
            
            for _, row in stock_list.iterrows():
                symbol = row["代码"]
                name = row["名称"]
                
                # 检查记录是否已存在
                existing_record = db.query(StockInfo).filter_by(symbol=symbol).first()
                if existing_record:
                    # 如果名称有变化，则更新
                    if existing_record.name != name:
                        existing_record.name = name
                        updated_count += 1
                else:
                    # 添加新记录
                    try:
                        db.add(StockInfo(symbol=symbol, name=name))
                        inserted_count += 1
                    except IntegrityError:
                        # 处理可能的并发插入导致的完整性错误
                        db.rollback()
                        existing_record = db.query(StockInfo).filter_by(symbol=symbol).first()
                        if existing_record and existing_record.name != name:
                            existing_record.name = name
                            updated_count += 1
            
            db.commit()
            logger.info(f"Inserted {inserted_count} and updated {updated_count} stock info records.")
        except Exception as e:
            db.rollback()
            logger.error(f"Failed to save stock info to database: {e}")
            raise DataSaveError(f"Failed to save stock info to database: {e}")

    def save_index_info_to_db(self, index_list):
        """
        保存指数基本信息到数据库。

        Args:
            index_list (pandas.DataFrame): 包含指数列表的DataFrame，必须包含'代码'和'名称'列。

        Raises:
            DataSaveError: 如果保存指数基本信息到数据库失败，则抛出此异常。
        """
        try:
            logger.info("Saving index info to database...")
            db: Session = next(get_db())
            inserted_count = 0
            updated_count = 0
            
            for _, row in index_list.iterrows():
                symbol = str(row["代码"]).zfill(6)  # 确保指数代码为6位
                name = row["名称"]
                
                # 检查记录是否已存在
                existing_record = db.query(IndexInfo).filter_by(symbol=symbol).first()
                if existing_record:
                    # 如果名称有变化，则更新
                    if existing_record.name != name:
                        existing_record.name = name
                        updated_count += 1
                else:
                    # 添加新记录
                    try:
                        db.add(IndexInfo(symbol=symbol, name=name))
                        inserted_count += 1
                    except IntegrityError:
                        # 处理可能的并发插入导致的完整性错误
                        db.rollback()
                        existing_record = db.query(IndexInfo).filter_by(symbol=symbol).first()
                        if existing_record and existing_record.name != name:
                            existing_record.name = name
                            updated_count += 1
            
            db.commit()
            logger.info(f"Inserted {inserted_count} and updated {updated_count} index info records.")
        except Exception as e:
            db.rollback()
            logger.error(f"Failed to save index info to database: {e}")
            raise DataSaveError(f"Failed to save index info to database: {e}")

    def save_index_daily_data_to_db(self, index_data, symbol, index_name):  # 添加 index_name 参数
        """保存指数日数据到数据库"""
        try:
            logger.info(f"Saving daily data for index {symbol}({index_name}) to database...")
            db: Session = next(get_db())
            updated_count = 0
            inserted_count = 0
            for _, row in index_data.iterrows():
                row_date_str = row["日期"]
                row_date = pd.to_datetime(row_date_str, errors='coerce').date()
                if pd.isna(row_date):
                    logger.warning(f"Invalid date format: {row_date_str}")
                    continue

                existing_record = db.query(IndexDailyData).filter_by(symbol=symbol, date=row_date).first()
                if existing_record:
                    if row_date > existing_record.date:
                        existing_record.open = row["开盘"]
                        existing_record.close = row["收盘"]
                        existing_record.high = row["最高"]
                        existing_record.low = row["最低"]
                        existing_record.volume = row["成交量"]
                        existing_record.amount = row["成交额"]
                        existing_record.amplitude = row["振幅"]
                        existing_record.change_rate = row["涨跌幅"]
                        existing_record.change_amount = row["涨跌额"]
                        existing_record.turnover_rate = row["换手率"]
                        updated_count += 1
                else:
                    db.add(IndexDailyData(
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
                    ))
                    inserted_count += 1
            db.commit()
            logger.info(
                f"Updated {updated_count} records and inserted {inserted_count} new records for index {symbol}.")
        except Exception as e:
            db.rollback()
            logger.error(f"Failed to save daily data for index {symbol} to database: {e}")
            raise DataSaveError(f"Failed to save daily data for index {symbol} to database: {e}")
