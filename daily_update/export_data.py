# export_data.py
"""
此模块负责将数据库中的特定时间段数据导出到SQL文件。
支持导出股票、指数和ETF的日线数据，可以指定导出的时间范围。
使用SQLAlchemy直接查询数据库并生成SQL文件，无需依赖外部命令。
Authors: hovi.hyw & AI 
Date: 2024-07-03
"""

import os
import sys
import argparse
from datetime import datetime, timedelta
import logging
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

# 设置日志
log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), 'logs'))
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"export_data_{datetime.now().strftime('%Y%m%d')}.log")

# 配置日志
logger = logging.getLogger('export_data')
logger.setLevel(logging.INFO)
# 清除已有的处理器，避免重复
if logger.handlers:
    for handler in logger.handlers:
        logger.removeHandler(handler)
# 添加处理器
file_handler = logging.FileHandler(log_file, encoding='utf-8')
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(console_handler)

# 加载环境变量
env_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'StockDownloader/.env'))
if os.path.exists(env_path):
    load_dotenv(env_path)
    logger.info(f"已加载环境变量文件: {env_path}")
else:
    logger.error(f"环境变量文件不存在: {env_path}")
    sys.exit(1)

def get_database_url():
    """
    根据环境变量构建数据库连接URL
    
    Returns:
        str: 数据库连接URL
    """
    # 获取数据库连接信息
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_name = os.getenv("DB_NAME")
    db_host = os.getenv("DB_HOST", "localhost")
    
    # 构建连接URL
    return f"postgresql://{db_user}:{db_password}@{db_host}:5432/{db_name}"

def export_table_data(table_name, start_date, output_file):
    """
    导出指定表在特定日期范围内的数据到SQL文件
    使用SQLAlchemy直接查询数据库并生成SQL文件
    
    Args:
        table_name (str): 表名
        start_date (str): 开始日期，格式为YYYY-MM-DD
        output_file (str): 输出文件路径
    
    Returns:
        bool: 导出是否成功
    """
    try:
        # 创建数据库引擎
        engine = create_engine(get_database_url())
        
        # 创建会话
        Session = sessionmaker(bind=engine)
        session = Session()
        
        logger.info(f"开始导出表 {table_name} 从 {start_date} 开始的数据到 {output_file}")
        
        # 查询数据
        query = text(f"SELECT * FROM {table_name} WHERE date >= :start_date ORDER BY symbol, date")
        result = session.execute(query, {"start_date": start_date})
        
        # 获取列名
        columns = result.keys()
        
        # 将结果转换为DataFrame
        df = pd.DataFrame(result.fetchall(), columns=columns)
        
        # 如果没有数据，记录警告并返回
        if df.empty:
            logger.warning(f"表 {table_name} 在 {start_date} 之后没有数据")
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(f"-- 表 {table_name} 在 {start_date} 之后没有数据\n")
            return True
        
        # 生成INSERT语句
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(f"-- 导出表 {table_name} 从 {start_date} 开始的数据\n")
            f.write(f"-- 导出时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            # 按行生成INSERT语句
            for _, row in df.iterrows():
                columns_str = ", ".join(columns)
                values = []
                
                for col in columns:
                    value = row[col]
                    # 处理不同类型的值
                    if value is None:
                        values.append("NULL")
                    elif isinstance(value, (int, float)):
                        values.append(str(value))
                    elif isinstance(value, datetime) or isinstance(value, date):
                        values.append(f"'{value}'")
                    else:
                        # 转义字符串中的单引号
                        values.append(f"'{str(value).replace("'", "''")}'") 
                
                values_str = ", ".join(values)
                insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({values_str});\n"
                f.write(insert_sql)
        
        logger.info(f"成功导出表 {table_name} 的数据到 {output_file}")
        return True
        
    except Exception as e:
        logger.error(f"导出表 {table_name} 时发生错误: {str(e)}")
        return False
    finally:
        # 关闭会话
        if 'session' in locals():
            session.close()

def export_data(days, output_dir=None):
    """
    导出指定天数的数据到SQL文件
    
    Args:
        days (int): 要导出的天数，从当前日期往前推
        output_dir (str, optional): 输出目录，默认为当前目录下的export_data文件夹
    
    Returns:
        bool: 导出是否成功
    """
    try:
        # 计算开始日期
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days)
        start_date_str = start_date.strftime("%Y-%m-%d")
        
        # 设置输出目录
        if output_dir is None:
            output_dir = os.path.join(os.path.dirname(__file__), f"export_data_{end_date.strftime('%Y%m%d')}")
        
        os.makedirs(output_dir, exist_ok=True)
        logger.info(f"输出目录: {output_dir}")
        
        # 要导出的表
        tables = ["daily_stock", "daily_index", "daily_etf"]
        
        # 导出每个表的数据
        success = True
        for table in tables:
            output_file = os.path.join(output_dir, f"{table}_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.sql")
            if not export_table_data(table, start_date_str, output_file):
                success = False
        
        if success:
            logger.info(f"所有数据导出成功，时间范围: {start_date_str} 到 {end_date.strftime('%Y-%m-%d')}")
        else:
            logger.warning("部分数据导出失败，请检查日志")
        
        return success
    
    except Exception as e:
        logger.error(f"导出数据时发生错误: {str(e)}")
        return False

def main():
    """
    主函数，解析命令行参数并执行导出操作
    """
    parser = argparse.ArgumentParser(description="导出数据库中的特定时间段数据到SQL文件")
    parser.add_argument("-d", "--days", type=int, required=True, help="要导出的天数，从当前日期往前推")
    parser.add_argument("-o", "--output-dir", type=str, help="输出目录，默认为当前目录下的export_data_YYYYMMDD文件夹")
    
    args = parser.parse_args()
    
    if args.days <= 0:
        logger.error("天数必须大于0")
        sys.exit(1)
    
    success = export_data(args.days, args.output_dir)
    
    if not success:
        sys.exit(1)

if __name__ == "__main__":
    main()