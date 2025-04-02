from sqlalchemy import Column, String, Float, Date, BigInteger, Numeric, PrimaryKeyConstraint

from ..base import Base


class IndexDailyData(Base):
    __tablename__ = "daily_index"

    symbol = Column(String, nullable=False)  # 指数代码
    date = Column(Date, nullable=False)  # 日期
    open = Column(Float)  # 开盘
    close = Column(Float)  # 收盘
    high = Column(Float)  # 最高
    low = Column(Float)  # 最低
    volume = Column(BigInteger)  # 成交量（使用BigInteger支持大数值）
    amount = Column(BigInteger)
    amplitude = Column(Float)  # 振幅
    change_rate = Column(Float)  # 涨跌幅
    change_amount = Column(Float)  # 涨跌额
    turnover_rate = Column(Float)  # 换手率

    __table_args__ = (
        PrimaryKeyConstraint('symbol', 'date'),
    )

    # 定义字段映射关系，用于DataFrame转换
    column_mappings = {
        '日期': 'date',
        '开盘': 'open',
        '收盘': 'close',
        '最高': 'high',
        '最低': 'low',
        '成交量': 'volume',
        '成交额': 'amount',
        '振幅': 'amplitude',
        '涨跌幅': 'change_rate',
        '涨跌额': 'change_amount',
        '换手率': 'turnover_rate'
    }

    def __repr__(self):
        return f"<IndexDailyData(symbol={self.symbol}, date={self.date})>"
