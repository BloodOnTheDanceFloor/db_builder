from sqlalchemy import Column, String, PrimaryKeyConstraint

from ..base import Base


class StockInfo(Base):
    __tablename__ = "stock_info"

    symbol = Column(String, primary_key=True, nullable=False)  # 股票代码
    name = Column(String(100), nullable=False)  # 股票名称

    def __repr__(self):
        return f"<StockInfo(symbol={self.symbol}, name={self.name})>"


class IndexInfo(Base):
    __tablename__ = "index_info"

    symbol = Column(String, primary_key=True, nullable=False)  # 指数代码
    name = Column(String(100), nullable=False)  # 指数名称

    def __repr__(self):
        return f"<IndexInfo(symbol={self.symbol}, name={self.name})>"