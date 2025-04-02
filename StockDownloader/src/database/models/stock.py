from sqlalchemy import Column, String, Float, Date, PrimaryKeyConstraint, BigInteger

from ..base import Base


class StockDailyData(Base):
    __tablename__ = "daily_stock"

    symbol = Column(String, nullable=False)  # 股票代码
    date = Column(Date, nullable=False)  # 日期
    open = Column(Float)  # 开盘价
    close = Column(Float)  # 收盘价
    high = Column(Float)  # 最高价
    low = Column(Float)  # 最低价
    volume = Column(BigInteger)  # 成交量
    amount = Column(BigInteger)  # 成交额
    outstanding_share = Column(Float)  # 流通股本
    turnover = Column(Float)  # 换手率

    __table_args__ = (
        PrimaryKeyConstraint('symbol', 'date'),
    )

    # 定义字段映射关系，用于DataFrame转换
    column_mappings = {
        'date': 'date',
        'open': 'open',
        'close': 'close',
        'high': 'high',
        'low': 'low',
        'volume': 'volume',
        'amount': 'amount',
        'outstanding_share': 'outstanding_share',
        'turnover': 'turnover'
    }

    def __repr__(self):
        return f"<StockDailyData(symbol={self.symbol}, date={self.date})>"
