"""
股票热度排名数据模型
"""
from sqlalchemy import Column, Integer, String, Float, Date, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class StockHotRank(Base):
    """
    股票热度排名数据模型
    """
    __tablename__ = "stock_hot_rank"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_code = Column(String(10), nullable=False, index=True)
    rank = Column(Integer, nullable=False)
    new_fans_ratio = Column(Float, nullable=False)
    loyal_fans_ratio = Column(Float, nullable=False)
    date = Column(Date, nullable=False, index=True)
    
    # 添加股票代码和日期的联合唯一约束
    __table_args__ = (
        UniqueConstraint('stock_code', 'date', name='uix_stock_code_date'),
    )
    
    def __repr__(self):
        return f"<StockHotRank(stock_code='{self.stock_code}', date='{self.date}', rank={self.rank})>"