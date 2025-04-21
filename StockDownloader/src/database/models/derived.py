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

    date = Column(Date, nullable=False, primary_key=True)  # 日期作为主键
    
    # 所有指数列
    idx_000001 = Column(Float)  # 上证指数
    idx_399001 = Column(Float)  # 深证成指
    idx_899050 = Column(Float)
    idx_399006 = Column(Float)  # 创业板指
    idx_000680 = Column(Float)
    idx_000688 = Column(Float)  # 科创50
    idx_399330 = Column(Float)
    idx_000300 = Column(Float)  # 沪深300
    idx_000016 = Column(Float)  # 上证50
    idx_399673 = Column(Float)
    idx_000888 = Column(Float)
    idx_399750 = Column(Float)
    idx_930050 = Column(Float)
    idx_000903 = Column(Float)
    idx_000510 = Column(Float)
    idx_000904 = Column(Float)
    idx_000905 = Column(Float)  # 中证500
    idx_000906 = Column(Float)
    idx_000852 = Column(Float)  # 中证1000
    idx_932000 = Column(Float)
    idx_000985 = Column(Float)
    idx_000010 = Column(Float)
    idx_000009 = Column(Float)
    idx_000132 = Column(Float)
    idx_000133 = Column(Float)
    idx_000003 = Column(Float)
    idx_000012 = Column(Float)
    idx_000013 = Column(Float)
    idx_000011 = Column(Float)
    idx_399002 = Column(Float)
    idx_399850 = Column(Float)
    idx_399005 = Column(Float)
    idx_399003 = Column(Float)
    idx_399106 = Column(Float)
    idx_399004 = Column(Float)
    idx_399007 = Column(Float)
    idx_399008 = Column(Float)
    idx_399293 = Column(Float)
    idx_399019 = Column(Float)
    idx_399020 = Column(Float)
    idx_399100 = Column(Float)
    idx_399550 = Column(Float)

    def __repr__(self):
        return f"<DerivedIndex(date={self.date})>"