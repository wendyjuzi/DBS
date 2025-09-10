#!/usr/bin/env python3
"""逐步测试每个功能"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_step_by_step():
    """逐步测试"""
    print("=== 逐步测试数据库功能 ===")
    
    try:
        from hybrid_db_final import HybridDatabaseEngine
        
        engine = HybridDatabaseEngine()
        print("✓ 引擎创建成功")
        
        # 1. 创建表
        print("\n1. 创建表...")
        result = engine.execute("CREATE TABLE students (id INT, name STRING, age INT)")
        print(f"   结果: {result['status']}")
        if result['status'] == 'error':
            print(f"   错误: {result.get('error')}")
            return False
        
        # 2. 单条插入
        print("\n2. 单条插入...")
        result = engine.execute("INSERT INTO students VALUES (1, 'Alice', 20)")
        print(f"   结果: {result['status']}, 影响行数: {result['affected_rows']}")
        if result['status'] == 'error':
            print(f"   错误: {result.get('error')}")
            return False
        
        # 3. 查询
        print("\n3. 查询数据...")
        result = engine.execute("SELECT * FROM students")
        print(f"   结果: {result['status']}, 行数: {result['affected_rows']}")
        if result['status'] == 'error':
            print(f"   错误: {result.get('error')}")
            return False
        
        if result.get('data'):
            print("   数据:")
            for row in result['data']:
                print(f"     {row}")
        
        # 4. 批量插入
        print("\n4. 批量插入...")
        result = engine.execute("INSERT INTO students VALUES (2, 'Bob', 19), (3, 'Charlie', 21)")
        print(f"   结果: {result['status']}, 影响行数: {result['affected_rows']}")
        if result['status'] == 'error':
            print(f"   错误: {result.get('error')}")
            return False
        
        # 5. 再次查询
        print("\n5. 再次查询...")
        result = engine.execute("SELECT * FROM students")
        print(f"   结果: {result['status']}, 行数: {result['affected_rows']}")
        if result.get('data'):
            print("   所有数据:")
            for row in result['data']:
                print(f"     {row}")
        
        engine.close()
        print("\n✓ 所有测试通过！")
        return True
        
    except Exception as e:
        print(f"✗ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_step_by_step()
    if success:
        print("\n🎉 数据库系统确实工作正常！")
    else:
        print("\n❌ 数据库系统有问题，需要修复。")
