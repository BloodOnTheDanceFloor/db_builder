"""Daily Update 模块

该模块是项目的自动化数据更新服务，负责在每个交易日自动更新股票、指数和ETF的最新数据。
模块设计为可靠、容错的自动化服务，支持多次重试和错误恢复。

主要功能:
1. 自动数据更新
   - 股票日线数据更新
   - 指数日线数据更新
   - ETF数据更新

2. 智能更新控制
   - 交易日判断
   - 数据时效性检查
   - 增量更新策略

3. 错误处理
   - 自动重试机制（最大重试3次）
   - 延迟策略（指数递增延迟）
   - 详细日志记录

运行机制:
1. 交易日检查：通过新浪财经API获取交易日历，判断当前日期是否为交易日
2. 数据更新需求检查：分别检查股票、指数和ETF数据的更新需求
3. 并行更新执行：
   - 股票数据使用独立线程更新
   - 指数和ETF数据串行更新
   - 实现资源竞争控制

性能优化:
1. 并行处理：股票数据使用独立线程
2. 智能更新：采用增量更新策略，避免重复更新
3. 资源管理：优化内存使用，管理连接池

环境要求:
- Python 3.x
- 依赖包：akshare, pandas, python-dotenv
- 环境变量：需要配置数据库连接信息

使用方法:
1. 确保环境变量配置正确（参考.env.example）
2. 直接运行脚本：python daily_update.py
3. 或通过Docker运行：docker-compose up daily-update

作者: hovi & Claude 3.5 Sonnet
日期: 2024-01
"""

import sys
import os
import time
import threading
from datetime import datetime, timedelta
import akshare as ak
from functools import wraps
import random
import logging
from dotenv import load_dotenv
import pandas as pd

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

# 设置日志
log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../logs'))
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"daily_update_{datetime.now().strftime('%Y%m%d')}.log")

# 配置日志 - 只配置当前模块的logger，避免与StockDownloader的logger冲突
logger = logging.getLogger('daily_update')
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
env_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../StockDownloader/.env'))
if os.path.exists(env_path):
    load_dotenv(env_path)
    logger.info(f"已加载环境变量文件: {env_path}")
    logger.info(f"数据库用户: {os.getenv('DB_USER')}")
else:
    logger.error(f"环境变量文件不存在: {env_path}")

# 导入StockDownloader模块
from StockDownloader.src.tasks.download_stock_task import download_all_stock_data
from StockDownloader.src.tasks.download_index_task import download_all_index_data
from StockDownloader.src.tasks.download_etf_task import download_all_etf_data