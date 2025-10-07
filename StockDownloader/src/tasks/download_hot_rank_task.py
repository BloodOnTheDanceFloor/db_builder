# src/tasks/download_hot_rank_task.py
"""
此模块定义了下载股票热度排名数据的任务。
从AKShare获取股票热度排名数据，并保存到数据库。
"""

from datetime import datetime
import time
import random
import pandas as pd
import akshare as ak
from sqlalchemy import text

from ..core.config import config
from ..core.logger import logger
from ..database.session import engine, SessionLocal
from ..database.models.hot_rank import StockHotRank
from ..services.stock_list_service import get_stock_list

def download_hot_rank_data(stock_code):
    """
    下载指定股票的热度排名数据，并保存到数据库。

    Args:
        stock_code (str): 股票代码。
    """
    try:
        logger.info(f"获取股票 {stock_code} 的热度排名数据...")
        result = ak.stock_hot_rank_detail_em(symbol=stock_code)
        
        # 检查返回值类型并进行相应处理
        if result is None:
            logger.warning(f"股票 {stock_code} 的热度排名数据为空")
            return
        elif isinstance(result, list):
            if len(result) == 0:
                logger.warning(f"股票 {stock_code} 的热度排名数据为空列表")
                return
            else:
                # 如果是列表，尝试转换为DataFrame
                logger.info(f"将股票 {stock_code} 的列表数据转换为DataFrame")
                df = pd.DataFrame(result)
        elif isinstance(result, pd.DataFrame):
            df = result
            if df.empty:
                logger.warning(f"未能获取到股票 {stock_code} 的热度排名数据")
                return
        else:
            logger.warning(f"股票 {stock_code} 返回了未知的数据类型: {type(result)}")
            return
        
        logger.info(f"成功获取股票 {stock_code} 的热度排名数据，共 {len(df)} 条记录")
        
        # 保存到数据库
        with SessionLocal() as session:
            for _, row in df.iterrows():
                # 转换日期格式
                date = datetime.strptime(row['时间'], '%Y-%m-%d').date()
                
                # 检查是否已存在相同日期的数据
                existing = session.query(StockHotRank).filter(
                    StockHotRank.stock_code == stock_code,
                    StockHotRank.date == date
                ).first()
                
                if existing:
                    # 更新现有数据
                    existing.rank = row['排名']
                    existing.new_fans_ratio = row['新晋粉丝']
                    existing.loyal_fans_ratio = row['铁杆粉丝']
                    logger.info(f"更新股票 {stock_code} 在 {date} 的热度排名数据")
                else:
                    # 插入新数据
                    new_record = StockHotRank(
                        stock_code=stock_code,
                        date=date,
                        rank=row['排名'],
                        new_fans_ratio=row['新晋粉丝'],
                        loyal_fans_ratio=row['铁杆粉丝']
                    )
                    session.add(new_record)
                    logger.info(f"插入股票 {stock_code} 在 {date} 的热度排名数据")
            
            session.commit()
        
        logger.info(f"股票 {stock_code} 热度排名数据保存完成")
    except Exception as e:
        logger.error(f"下载股票 {stock_code} 热度排名数据时出错: {e}")
        raise

def download_all_hot_rank_data(update_only=False):
    """
    下载所有股票的热度排名数据，并保存到数据库。
    
    Args:
        update_only (bool, optional): 是否只更新最新数据。默认为False，表示下载全部历史数据。
        max_stocks (int, optional): 最大下载股票数量。默认为50。
    """
    try:
        # 获取股票列表
        stock_list = get_stock_list()
        
        if stock_list is None or len(stock_list) == 0:
            logger.error("获取股票列表失败")
            return
        
        total_stocks = len(stock_list)
        logger.info(f"开始下载 {total_stocks} 个股票的热度排名数据")
        
        # 下载每个股票的热度排名数据
        for i, stock_code in enumerate(stock_list, 1):
            if not stock_code:
                continue
                
            try:
                logger.info(f"处理第 {i}/{total_stocks} 个股票: {stock_code}")
                download_hot_rank_data(stock_code)
                
                # 添加随机延迟，避免频繁请求
                delay = random.uniform(1, 3)
                time.sleep(delay)
            except Exception as e:
                logger.error(f"处理股票 {stock_code} 时出错: {e}")
                # 继续处理下一个股票
                continue
        
        logger.info(f"所有股票热度排名数据下载完成")
    except Exception as e:
        logger.error(f"下载股票热度排名数据时出错: {e}")
        raise

if __name__ == "__main__":
    # 测试下载单个股票的热度排名数据
    # download_hot_rank_data("000001")
    
    # 测试下载所有股票的热度排名数据
    download_all_hot_rank_data(max_stocks=10)