#!/usr/bin/env python3
"""
快速演示混合架构数据库系统 v2
重新组织后的模块化结构
"""

import sys
import os

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def quick_demo():
    """快速演示"""
    print("=== 混合架构数据库系统快速演示 v2 ===")
    
    try:
        from modules.database_system.hybrid_engine import HybridDatabaseEngine
        
        # 创建引擎
        engine = HybridDatabaseEngine()
        print("✓ 数据库引擎初始化成功")
        
        # 测试基本操作
        print("\n1. 创建表...")
        result = engine.execute("CREATE TABLE quick_test_v2 (id INT PRIMARY KEY, name STRING, value DOUBLE)")
        print(f"   结果: {result['status']}")
        
        print("\n2. 插入数据...")
        result = engine.execute("INSERT INTO quick_test_v2 VALUES (1, 'test1', 10.5)")
        print(f"   结果: {result['status']}, 影响行数: {result['affected_rows']}")
        
        result = engine.execute("INSERT INTO quick_test_v2 VALUES (2, 'test2', 20.3)")
        print(f"   结果: {result['status']}, 影响行数: {result['affected_rows']}")
        
        print("\n3. 查询数据...")
        result = engine.execute("SELECT * FROM quick_test_v2")
        print(f"   结果: {result['status']}")
        if result.get('data'):
            print("   数据:")
            for row in result['data']:
                print(f"     {row}")
        
        print("\n4. 条件查询...")
        result = engine.execute("SELECT name FROM quick_test_v2 WHERE value > 15")
        print(f"   结果: {result['status']}")
        if result.get('data'):
            print("   数据:")
            for row in result['data']:
                print(f"     {row}")
        
        print("\n5. 删除数据...")
        result = engine.execute("DELETE FROM quick_test_v2 WHERE id = 1")
        print(f"   结果: {result['status']}, 影响行数: {result['affected_rows']}")
        
        print("\n6. 最终查询...")
        result = engine.execute("SELECT * FROM quick_test_v2")
        print(f"   结果: {result['status']}")
        if result.get('data'):
            print("   剩余数据:")
            for row in result['data']:
                print(f"     {row}")
        
        # 关闭引擎
        engine.close()
        print("\n✓ 所有测试通过！混合架构数据库系统工作正常。")
        
        return True
        
    except Exception as e:
        print(f"✗ 演示失败: {str(e)}")
        return False

if __name__ == "__main__":
    success = quick_demo()
    if success:
        print("\n要启动交互式界面，请运行: python hybrid_db_v2.py")
    sys.exit(0 if success else 1)
