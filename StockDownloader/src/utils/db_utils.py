# src/utils/db_utils.py
"""
此模块包含数据库相关的实用工具函数。
包括检查数据库是否已初始化，以及初始化数据库的函数。
Authors: hovi.hyw & AI
Date: 2024-07-03
"""

from sqlalchemy import inspect

from ..core.logger import logger
from ..database.session import engine
from .init_db import init_database


def check_database_initialized():
    """
    检查数据库是否已经初始化。

    Returns:
        bool: 如果数据库已初始化，则返回 True，否则返回 False。
    """
    inspector = inspect(engine)
    # 检查必要的表是否存在
    required_tables = {'daily_index', 'daily_stock'}  # 使用模型中定义的实际表名
    existing_tables = set(inspector.get_table_names())
    return required_tables.issubset(existing_tables)


def initialize_database_if_needed():
    """
    检查数据库是否初始化，未初始化则进行初始化。
    """
    if not check_database_initialized():
        logger.info("数据库未初始化，开始初始化...")
        init_database()
    else:
        logger.info("数据库已初始化，跳过初始化步骤")


if __name__ == '__main__':
    # 示例用法
    initialize_database_if_needed()
    if check_database_initialized():
        print("数据库已初始化或初始化成功。")
    else:
        print("数据库初始化检查失败。")
