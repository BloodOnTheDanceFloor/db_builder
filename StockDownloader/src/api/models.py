# src/api/models.py
"""
此模块定义了API的数据模型，使用Pydantic进行数据验证和序列化。
Authors: hovi.hyw & AI
Date: 2024-07-03
"""

from datetime import date

from pydantic import BaseModel


class StockData(BaseModel):
    """
    股票数据模型。
    定义了股票日线数据的结构，用于API的输入和输出。

    Attributes:
        symbol (str): 股票代码。
        date (date): 日期。
        open (float): 开盘价。
        close (float): 收盘价。
        high (float): 最高价。
        low (float): 最低价。
        volume (float): 成交量。
        amount (float): 成交额。
    """
    symbol: str
    date: date
    open: float
    close: float
    high: float
    low: float
    volume: float
    amount: float


class IndexData(BaseModel):
    """
    指数数据模型。
    定义了指数日线数据的结构，用于API的输入和输出。

    Attributes:
        symbol (str): 指数代码。
        date (date): 日期。
        open (float): 开盘价。
        close (float): 收盘价。
        high (float): 最高价。
        low (float): 最低价。
        volume (float): 成交量。
        amount (float): 成交额。
    """
    symbol: str
    date: date
    open: float
    close: float
    high: float
    low: float
    volume: float
    amount: float
