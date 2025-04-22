from sqlalchemy import Column, String, Float, Date, PrimaryKeyConstraint

from ..base import Base


class DerivedStock(Base):
    __tablename__ = "derived_stock"

    symbol = Column(String(10), nullable=False)  # 股票代码
    date = Column(Date, nullable=False)  # 日期
    real_change = Column(Float)  # 真实涨跌

    __table_args__ = (
        PrimaryKeyConstraint('symbol', 'date'),
    )

    def __repr__(self):
        return f"<DerivedStock(symbol={self.symbol}, date={self.date})>"


class DerivedIndex(Base):
    __tablename__ = "derived_index"

    symbol = Column(String(10), nullable=False)  # 指数代码
    date = Column(Date, nullable=False)  # 日期
    real_change = Column(Float)  # 真实涨跌

    __table_args__ = (
        PrimaryKeyConstraint('symbol', 'date'),
    )

    def __repr__(self):
        return f"<DerivedIndex(symbol={self.symbol}, date={self.date})>"