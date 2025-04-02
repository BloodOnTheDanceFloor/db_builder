# src/api/endpoints/index.py
"""
此模块定义了指数数据相关的API端点。
使用FastAPI框架，提供了获取指数日线数据的接口。
Authors: hovi.hyw & AI
Date: 2024-07-03
"""

from datetime import date

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from ..models import IndexData
from ...database.models.index import IndexDailyData
from ...database.session import get_db

router = APIRouter()


@router.get("/index/{symbol}/{date}", response_model=IndexData)
def get_index_data(symbol: str, date: date, db: Session = Depends(get_db)):
    """
    获取指定指数指定日期的日线数据。

    Args:
        symbol (str): 指数代码。
        date (date): 日期。
        db (Session): 数据库会话。

    Returns:
        IndexData: 指数日线数据。

    Raises:
        HTTPException: 如果找不到指数数据，则抛出404异常。
    """
    index_data = db.query(IndexDailyData).filter(IndexDailyData.symbol == symbol, IndexDailyData.date == date).first()
    if index_data is None:
        raise HTTPException(status_code=404, detail="Index data not found")
    return index_data
