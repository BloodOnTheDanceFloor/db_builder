# src/api/endpoints/stock.py
"""
此模块定义了股票数据相关的API端点。
使用FastAPI框架，提供了获取股票日线数据的接口。
Authors: hovi.hyw & AI
Date: 2024-07-03
"""

from datetime import date

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from ..models import StockData
from ...database.models.stock import StockDailyData
from ...database.session import get_db

router = APIRouter()


@router.get("/stock/{symbol}/{date}", response_model=StockData)
def get_stock_data(symbol: str, date: date, db: Session = Depends(get_db)):
    """
    获取指定股票指定日期的日线数据。

    Args:
        symbol (str): 股票代码。
        date (date): 日期。
        db (Session): 数据库会话。

    Returns:
        StockData: 股票日线数据。

    Raises:
        HTTPException: 如果找不到股票数据，则抛出404异常。
    """
    stock_data = db.query(StockDailyData).filter(StockDailyData.symbol == symbol, StockDailyData.date == date).first()
    if stock_data is None:
        raise HTTPException(status_code=404, detail="Stock data not found")
    return stock_data
