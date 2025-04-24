"""
简易程序，测试连接stock_db.derived_stock和stock_db.derived_index表并进行CRUD操作
"""
import datetime
from sqlalchemy.orm import Session

# 导入StockDownloader中的相关模块
from StockDownloader.src.database.session import get_db, engine
from StockDownloader.src.database.models.derived import DerivedStock, DerivedIndex
from StockDownloader.src.utils.db_utils import initialize_database_if_needed

def test_derived_stock_crud():
    """测试derived_stock表的CRUD操作"""
    print("=== 测试derived_stock表的CRUD操作 ===")
    
    # 获取数据库会话
    db = next(get_db())
    
    try:
        # 创建测试数据
        test_stock = DerivedStock(
            symbol="000001",
            date=datetime.date(2023, 1, 1),
            real_change=0.0123
        )
        
        # 创建操作 (Create)
        print("创建记录...")
        db.add(test_stock)
        db.commit()
        print(f"创建成功: {test_stock}")
        
        # 读取操作 (Read)
        print("\n读取记录...")
        result = db.query(DerivedStock).filter(
            DerivedStock.symbol == "000001",
            DerivedStock.date == datetime.date(2023, 1, 1)
        ).first()
        print(f"读取结果: {result}")
        
        # 更新操作 (Update)
        print("\n更新记录...")
        result.real_change = 0.0456
        db.commit()
        updated = db.query(DerivedStock).filter(
            DerivedStock.symbol == "000001",
            DerivedStock.date == datetime.date(2023, 1, 1)
        ).first()
        print(f"更新后结果: {updated}")
        
        # 删除操作 (Delete)
        print("\n删除记录...")
        db.delete(updated)
        db.commit()
        check = db.query(DerivedStock).filter(
            DerivedStock.symbol == "000001",
            DerivedStock.date == datetime.date(2023, 1, 1)
        ).first()
        print(f"删除后查询结果: {check}")
        
    except Exception as e:
        db.rollback()
        print(f"操作失败: {e}")
    finally:
        db.close()
def test_derived_index_crud():
    """测试derived_index表的CRUD操作"""
    print("\n=== 测试derived_index表的CRUD操作 ===")
    
    # 获取数据库会话
    db = next(get_db())
    
    try:
        # 创建测试数据 - 上证指数
        test_date = datetime.date(2023, 1, 1)
        test_index1 = DerivedIndex(
            symbol="000001",  # 上证指数
            date=test_date,
            real_change=0.0123
        )
        
        # 创建测试数据 - 深证成指
        test_index2 = DerivedIndex(
            symbol="399001",  # 深证成指
            date=test_date,
            real_change=0.0234
        )
        
        # 创建测试数据 - 沪深300
        test_index3 = DerivedIndex(
            symbol="000300",  # 沪深300
            date=test_date,
            real_change=0.0345
        )
        
        # 创建操作 (Create)
        print("创建记录...")
        db.add_all([test_index1, test_index2, test_index3])
        db.commit()
        print(f"创建成功: {test_index1}, {test_index2}, {test_index3}")
        
        # 读取操作 (Read)
        print("\n读取记录...")
        results = db.query(DerivedIndex).filter(
            DerivedIndex.date == test_date
        ).all()
        print(f"读取结果数量: {len(results)}")
        for result in results:
            print(f"指数 {result.symbol} 的真实涨跌幅: {result.real_change}")
        
        # 更新操作 (Update)
        print("\n更新记录...")
        test_index1 = db.query(DerivedIndex).filter(
            DerivedIndex.symbol == "000001",
            DerivedIndex.date == test_date
        ).first()
        test_index1.real_change = 0.0456
        db.commit()
        updated = db.query(DerivedIndex).filter(
            DerivedIndex.symbol == "000001",
            DerivedIndex.date == test_date
        ).first()
        print(f"更新后上证指数涨跌幅: {updated.real_change}")
        
        # 删除操作 (Delete)
        print("\n删除记录...")
        for result in results:
            db.delete(result)
        db.commit()
        check = db.query(DerivedIndex).filter(
            DerivedIndex.date == test_date
        ).all()
        print(f"删除后查询结果数量: {len(check)}")
        
    except Exception as e:
        db.rollback()
        print(f"操作失败: {e}")
    finally:
        db.close()

def main():
    """主函数"""
    print("开始测试derived_stock和derived_index表的CRUD操作")
    
    # 确保数据库已初始化
    print("检查并初始化数据库...")
    initialize_database_if_needed()
    
    # 测试derived_stock表
    test_derived_stock_crud()
    
    # 测试derived_index表
    test_derived_index_crud()
    
    print("\n测试完成")

if __name__ == "__main__":
    main()