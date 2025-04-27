import pandas as pd
from sqlalchemy.orm import Session
from datetime import datetime
from typing import Dict, List, Tuple

from StockDownloader.src.database.session import SessionLocal
from StockDownloader.src.database.models.derived import DerivedStock, DerivedIndex
from StockDownloader.src.database.models.info import StockInfo


def calculate_correlation_for_stock(stock_symbol: str, year: int, db: Session) -> Tuple[str, Dict[str, int], Dict[str, int]]:
    """
    计算指定股票在指定年份与各指数的相关度，返回相关度最高的指数代码
    相关度计算方法：
    1. 获取股票在指定年份的所有交易日的real_change
    2. 对每个交易日，计算股票的real_change与各指数的real_change的差值
    3. 根据差值对指数进行排序，差值最小的得1分，第二小的得2分，依此类推
    4. 累计所有交易日的分数，计算每个指数的平均得分（总得分除以有效交易日数）
    5. 平均得分最低的指数即为相关度最高的指数
    
    返回值：
    - 相关度最高的指数代码
    - 所有指数的得分字典
    - 所有指数的有效交易日数字典
    """
    # 获取指定年份的开始和结束日期
    start_date = datetime(year, 1, 1)
    end_date = datetime(year, 12, 31)
    
    # 获取股票在指定年份的所有交易日数据
    stock_data = db.query(DerivedStock).filter(
        DerivedStock.symbol == stock_symbol,
        DerivedStock.date >= start_date,
        DerivedStock.date <= end_date
    ).all()
    
    if not stock_data:
        print(f"没有找到股票 {stock_symbol} 在 {year} 年的数据")
        return None, {}, {}
    
    # 获取所有股票交易日期
    stock_dates = [data.date for data in stock_data]
    
    # 创建股票日期到real_change的映射，只保留有效的real_change值
    stock_changes = {}
    for data in stock_data:
        if data.real_change is not None:
            stock_changes[data.date] = data.real_change
    
    if not stock_changes:
        print(f"股票 {stock_symbol} 在 {year} 年没有有效的real_change数据")
        return None, {}, {}
    
    # 获取所有指数代码
    index_symbols = db.query(DerivedIndex.symbol).distinct().all()
    index_symbols = [symbol[0] for symbol in index_symbols]
    
    if not index_symbols:
        print("没有找到任何指数数据")
        return None, {}, {}
    
    # 初始化指数得分和有效交易日计数
    index_scores = {symbol: 0 for symbol in index_symbols}
    index_valid_days = {symbol: 0 for symbol in index_symbols}
    
    # 对每个交易日计算相关度得分
    for date in stock_changes.keys():  # 只遍历有效的股票交易日
        # 获取当天所有指数的real_change
        index_changes = db.query(DerivedIndex).filter(
            DerivedIndex.date == date
        ).all()
        
        # 计算差值并排序，只考虑有有效real_change的指数
        differences = []
        valid_indices = set()  # 记录当天有有效数据的指数
        
        for idx in index_changes:
            if idx.real_change is not None:
                diff = abs(idx.real_change - stock_changes[date])
                differences.append((idx.symbol, diff))
                valid_indices.add(idx.symbol)
        
        # 如果当天没有有效的指数数据，跳过这一天
        if not differences:
            continue
        
        # 按差值排序
        differences.sort(key=lambda x: x[1])
        
        # 分配得分（相同差值得相同分数）
        current_score = 1
        prev_diff = None
        for i, (symbol, diff) in enumerate(differences):
            if prev_diff is not None and diff != prev_diff:
                current_score = i + 1
            index_scores[symbol] += current_score
            index_valid_days[symbol] += 1  # 记录该指数有效的交易日数
            prev_diff = diff
    
    # 过滤掉没有足够有效交易日的指数
    valid_indices = {}
    for symbol, score in index_scores.items():
        if index_valid_days[symbol] > 0:
            # 计算平均得分（总得分除以有效交易日数）
            avg_score = score / index_valid_days[symbol]
            valid_indices[symbol] = avg_score
    
    if not valid_indices:
        print(f"没有找到与股票 {stock_symbol} 在 {year} 年有共同交易日的指数")
        return None, {}, {}
    
    # 找出平均得分最低的指数
    best_index = min(valid_indices.items(), key=lambda x: x[1])[0] if valid_indices else None
    
    return best_index, index_scores, index_valid_days


def update_stock_index_correlation(stock_symbol: str = None, years: List[int] = None):
    """
    更新股票与指数的相关度
    如果指定了stock_symbol，则只更新该股票的相关度
    如果指定了years，则只更新这些年份的相关度
    """
    if years is None:
        years = [2020, 2021, 2022, 2023, 2024]
    
    db = SessionLocal()
    try:
        # 如果指定了股票代码，只处理该股票
        if stock_symbol:
            stocks = [db.query(StockInfo).filter(StockInfo.symbol == stock_symbol).first()]
            if not stocks[0]:
                print(f"股票 {stock_symbol} 不存在于stock_info表中")
                return
        else:
            # 否则处理所有股票
            stocks = db.query(StockInfo).all()
        
        # 检查是否是单独运行（通过__main__调用）
        is_main_run = __name__ == "__main__"

        print("开始更新股票指数相关度...")  # 添加开始信息

        for stock in stocks:
            print(f"处理股票: {stock.symbol} ({stock.name})")
            for year in years:
                # 检查当前年份是否已经超过当前年份
                current_year = datetime.now().year
                if year > current_year:
                    print(f"跳过未来年份: {year}")
                    continue

                # 计算相关度最高的指数
                best_index, index_scores, index_valid_days = calculate_correlation_for_stock(stock.symbol, year, db)

                if best_index:
                    # 更新stock_info表
                    setattr(stock, f"index_{year}", best_index)
                    print(f"  {year}年最相关指数: {best_index}")

                    # 如果是单独运行，打印所有指数的得分情况
                    if is_main_run:
                        print(f"\n  {year}年所有指数得分情况:")
                        print("  " + "-"*60)
                        print("  {:^10} {:^10} {:^15} {:^10} {:^5}".format("指数代码", "得分", "有效交易日数", "平均得分", ""))
                        print("  " + "-"*60)

                        # 按平均得分排序（从低到高，低分表示相关性更高）
                        sorted_indices = []
                        for symbol, score in index_scores.items():
                            if index_valid_days[symbol] > 0:
                                avg_score = score / index_valid_days[symbol]
                                sorted_indices.append((symbol, score, index_valid_days[symbol], avg_score))

                        # 按平均得分排序
                        sorted_indices.sort(key=lambda x: x[3])

                        for i, (symbol, score, valid_days, avg_score) in enumerate(sorted_indices):
                            # 标记最相关的指数
                            mark = "*" if symbol == best_index else " "
                            # 添加排名
                            rank = i + 1
                            print("  {:10} {:10d} {:15d} {:10.2f} {}".format(symbol, score, valid_days, avg_score, mark))

                        print("  " + "-"*60)
                        print(f"  * 表示相关度最高的指数\n")
                else:
                    print(f"  未找到{year}年的相关指数")

            # 提交更改
            db.commit()

        print("股票指数相关度更新完成.")  # 添加完成信息
    finally:
        db.close()


if __name__ == "__main__":
    # 更新所有股票的所有年份
    update_stock_index_correlation()
