# scripts/init_db.py
from sqlalchemy import inspect
from ..database.session import engine, Base
from ..core.logger import logger


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