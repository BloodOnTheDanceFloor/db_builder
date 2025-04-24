from sqlalchemy import Column, String, PrimaryKeyConstraint

from ..base import Base


class StockInfo(Base):
    __tablename__ = "stock_info"

    symbol = Column(String, primary_key=True, nullable=False)  # 股票代码
    name = Column(String(100), nullable=False)  # 股票名称
    index_2020 = Column(String(6))  # 2020年所属指数代码
    index_2021 = Column(String(6))  # 2021年所属指数代码
    index_2022 = Column(String(6))  # 2022年所属指数代码
    index_2023 = Column(String(6))  # 2023年所属指数代码
    index_2024 = Column(String(6))  # 2024年所属指数代码

    def __repr__(self):
        return f"<StockInfo(symbol={self.symbol}, name={self.name})>"


class IndexInfo(Base):
    __tablename__ = "index_info"

    symbol = Column(String, primary_key=True, nullable=False)  # 指数代码
    name = Column(String(100), nullable=False)  # 指数名称

    def __repr__(self):
        return f"<IndexInfo(symbol={self.symbol}, name={self.name})>"