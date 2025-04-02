from sqlalchemy import Column, String, Float, Date, PrimaryKeyConstraint

from ..base import Base


class AuctionStock(Base):
    __tablename__ = "auction_stock"

    date = Column(Date, nullable=False)  # 日期
    symbol = Column(String, nullable=False)  # 股票代码
    open_auction = Column(Float)  # 开盘集合竞价
    end_auction = Column(Float)  # 收盘集合竞价

    __table_args__ = (
        PrimaryKeyConstraint('symbol', 'date'),
    )

    def __repr__(self):
        return f"<AuctionStock(symbol={self.symbol}, date={self.date})>"


class AuctionIndex(Base):
    __tablename__ = "auction_index"

    date = Column(Date, nullable=False)  # 日期
    symbol = Column(String, nullable=False)  # 指数代码
    open_auction = Column(Float)  # 开盘集合竞价
    end_auction = Column(Float)  # 收盘集合竞价

    __table_args__ = (
        PrimaryKeyConstraint('symbol', 'date'),
    )

    def __repr__(self):
        return f"<AuctionIndex(symbol={self.symbol}, date={self.date})>"