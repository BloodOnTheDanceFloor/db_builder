# src/core/config.py
"""
此模块负责管理应用程序的配置信息。
它从环境变量和 .env 文件中加载配置，并提供一个全局可用的配置对象。
Authors: hovi.hyw & AI
Date: 2024-07-03
"""

import os

from dotenv import load_dotenv

# 获取项目根目录的绝对路径
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))
REPO_ROOT = os.path.abspath(os.path.join(PROJECT_ROOT, "../"))

# 加载环境变量 (由 Docker Compose 处理)


class Settings:
    """
    配置类。
    该类存储应用程序的所有配置信息，包括数据库连接信息、日志级别、API 重试次数等。

    Attributes:
        PROJECT_ROOT (str): 项目根目录的绝对路径。
        DATABASE_URL (str): 数据库连接 URL。
        LOG_LEVEL (str): 日志级别。
        MAX_CSV_AGE_DAYS (int): 股票列表 CSV 文件最大有效天数。
        MAX_RETRIES (int): API 请求最大重试次数。
        RETRY_DELAY (int): API 请求重试间隔（秒）。
        GET_TIMEOUT (int): API 请求超时时间（秒）。
        MAX_THREADS (int): 最大线程数。

    """
    # 基础路径配置
    PROJECT_ROOT = PROJECT_ROOT
    REPO_ROOT = REPO_ROOT

    # 在 Docker 环境中，直接使用容器内的路径，否则使用项目相对路径
    CACHE_PATH = os.getenv("CACHE_PATH", os.path.join(PROJECT_ROOT, "cache"))
    LOG_DIR = os.getenv("LOG_DIR", os.path.join(REPO_ROOT, "logs"))
    LOG_FILE = os.getenv("LOG_FILE", os.path.join(LOG_DIR, "stockdownloader.log"))

    # 数据库配置
        # 数据库设置
    @staticmethod
    def get_database_url():
        """
        根据运行环境自动选择正确的数据库连接字符串
        优先使用环境变量中的配置，如果未设置则根据运行环境自动选择
        """
        # 从环境变量获取数据库URL
        db_url = os.getenv("DATABASE_URL") or os.getenv("POSTGRES_CONNECTION_STRING")
        if db_url:
            return db_url
            
        # 如果环境变量中没有设置，则根据环境自动选择
        # 检查是否在Docker环境中运行
        is_docker = os.path.exists('/.dockerenv') or os.environ.get('DOCKER_CONTAINER') == 'true'
        
        # 获取数据库连接信息（全部从环境变量中读取，提供合理的默认值）
        db_user = os.getenv("DB_USER", "postgres")
        db_password = os.getenv("DB_PASSWORD", "postgres")
        db_name = os.getenv("DB_NAME", "stock_db")
        
        # 根据环境选择主机名，优先使用环境变量中的配置
        db_host = os.getenv("DB_HOST")
        if not db_host:
            # 如果环境变量中未设置主机名，则根据运行环境自动选择
            if is_docker:
                db_host = os.getenv("DB_HOST_DOCKER", "pgdb")
            else:
                db_host = os.getenv("DB_HOST_LOCAL", "localhost")
            
        return f"postgresql://{db_user}:{db_password}@{db_host}:5432/{db_name}"
    
    DATABASE_URL: str = get_database_url()

    # 日志配置
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    # AkShare API 配置
    AKSHARE_API_KEY = os.getenv("AKSHARE_API_KEY", "")

    # 列表更新频率
    MAX_CSV_AGE_DAYS = int(os.getenv("MAX_CSV_AGE_DAYS", 100))

    # 数据更新频率（天）
    DATA_UPDATE_INTERVAL = int(os.getenv("DATA_UPDATE_INTERVAL", 100))

    # stock_zh_a_daily\index_zh_a_hist重试次数和间隔
    MAX_RETRIES = int(os.getenv("MAX_RETRIES", 3))
    RETRY_DELAY = int(os.getenv("RETRY_DELAY", 5))
    GET_TIMEOUT = int(os.getenv("GET_TIMEOUT", 10))

    # 并行线程数
    MAX_THREADS = int(os.getenv("MAX_THREADS", 10))

    # 下载配置
    INDICES_NAMES= os.getenv("INDICES_NAMES", "沪深重要指数")
    START_DATE = os.getenv("START_DATE","19900101")


# 实例化配置对象
config = Settings()
