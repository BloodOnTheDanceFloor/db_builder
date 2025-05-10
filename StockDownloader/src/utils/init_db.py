# src/utils/init_db.py
"""
此模块负责数据库初始化相关功能。

功能概述：
- 检查数据库中现有的表
- 创建缺失的数据库表
- 智能处理表创建过程，只创建不存在的表

使用方法：
直接调用init_database()函数即可初始化数据库。该函数会自动检查现有表，
并只创建缺失的表，避免重复创建已存在的表结构。

依赖：
- SQLAlchemy ORM
- 项目中定义的数据模型（models）

示例：
```python
from src.utils.init_db import init_database

# 初始化数据库
init_database()
```

Authors: hovi.hyw & AI
Date: 2024-07-03
"""

from sqlalchemy import inspect
from ..database.session import engine, Base
from ..core.logger import logger
# 确保所有模型类都被导入，这样Base.metadata才能包含所有表
from ..database.models import DerivedStock, DerivedIndex, StockDailyData, IndexDailyData, StockInfo, IndexInfo, AuctionStock, AuctionIndex
from ..database.models.info import ETFInfo
from ..database.models.etf import ETFDailyData


def init_database():
    """初始化数据库，创建所有表"""
    try:
        # 检查现有的表
        inspector = inspect(engine)
        existing_tables = inspector.get_table_names()  # 获取数据库中现有的表名
        all_tables = Base.metadata.tables.keys()  # 获取模型中定义的所有表名

        # 找出需要创建的表
        missing_tables = [table for table in all_tables if table not in existing_tables]

        if not missing_tables:
            logger.info("所有表都已存在，跳过创建步骤。")
        else:
            logger.info(f"以下表不存在，将创建这些表: {missing_tables}")
            Base.metadata.create_all(bind=engine)  # 创建缺失的表
            logger.info("数据库表创建成功！")
    except Exception as e:
        logger.error(f"创建数据库表时发生错误: {str(e)}")
        raise

if __name__ == '__main__':
    init_database()