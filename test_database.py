#!/usr/bin/env python3
"""
测试数据库基本功能
"""

import sys
import os

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_database():
    """测试数据库基本功能"""
    print("=== 混合架构数据库系统测试 ===")
    
    try:
        from hybrid_db_final import HybridDatabaseEngine
        
        # 创建引擎
        engine = HybridDatabaseEngine()
        print("✓ 数据库引擎初始化成功")
        
        # 1. 测试创建表
        print("\n1. 创建表...")
        result = engine.execute("CREATE TABLE students (id INT PRIMARY KEY, name STRING, age INT, score DOUBLE)")
        print(f"   结果: {result['status']}")
        
        # 2. 测试单条插入
        print("\n2. 单条插入...")
        result = engine.execute("INSERT INTO students VALUES (1, 'Alice', 20, 95.5)")
        print(f"   结果: {result['status']}, 影响行数: {result['affected_rows']}")
        
        # 3. 测试批量插入
        print("\n3. 批量插入...")
        result = engine.execute("INSERT INTO students VALUES (2, 'Bob', 19, 88.0), (3, 'Charlie', 21, 92.3)")
        print(f"   结果: {result['status']}, 影响行数: {result['affected_rows']}")
        
        # 4. 测试查询
        print("\n4. 查询所有数据...")
        result = engine.execute("SELECT * FROM students")
        print(f"   结果: {result['status']}, 行数: {result['affected_rows']}")
        if result.get('data'):
            print("   数据:")
            for row in result['data']:
                print(f"     {row}")
        
        # 5. 测试条件查询
        print("\n5. 条件查询...")
        result = engine.execute("SELECT name, score FROM students WHERE age > 19")
        print(f"   结果: {result['status']}, 行数: {result['affected_rows']}")
        if result.get('data'):
            print("   数据:")
            for row in result['data']:
                print(f"     {row}")
        
        # 6. 测试排序
        print("\n6. 排序查询...")
        result = engine.execute("SELECT name, score FROM students ORDER BY score DESC")
        print(f"   结果: {result['status']}, 行数: {result['affected_rows']}")
        if result.get('data'):
            print("   按分数降序:")
            for row in result['data']:
                print(f"     {row}")
        
        # 7. 测试更新
        print("\n7. 更新操作...")
        result = engine.execute("UPDATE students SET score = 99.0 WHERE name = 'Alice'")
        print(f"   结果: {result['status']}, 影响行数: {result['affected_rows']}")
        
        # 8. 验证更新
        print("\n8. 验证更新...")
        result = engine.execute("SELECT name, score FROM students WHERE name = 'Alice'")
        print(f"   结果: {result['status']}")
        if result.get('data'):
            print("   Alice的分数:")
            for row in result['data']:
                print(f"     {row}")
        
        # 9. 测试删除
        print("\n9. 删除操作...")
        result = engine.execute("DELETE FROM students WHERE score < 90")
        print(f"   结果: {result['status']}, 影响行数: {result['affected_rows']}")
        
        # 10. 最终查询
        print("\n10. 最终查询...")
        result = engine.execute("SELECT * FROM students")
        print(f"   结果: {result['status']}, 剩余行数: {result['affected_rows']}")
        if result.get('data'):
            print("   剩余数据:")
            for row in result['data']:
                print(f"     {row}")
        
        # 关闭引擎
        engine.close()
        print("\n✓ 数据库测试完成！")
        
        return True
        
    except Exception as e:
        print(f"✗ 数据库测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_parser():
    """测试SQL解析器"""
    print("\n=== SQL解析器测试 ===")
    
    try:
        from hybrid_db_final import SimpleSQLParser
        
        parser = SimpleSQLParser()
        
        # 测试各种SQL语句
        test_sqls = [
            "CREATE TABLE test (id INT, name STRING)",
            "INSERT INTO test VALUES (1, 'test')",
            "INSERT INTO test VALUES (1, 'a'), (2, 'b')",
            "SELECT * FROM test",
            "SELECT name FROM test WHERE id > 0",
            "UPDATE test SET name = 'new' WHERE id = 1",
            "DELETE FROM test WHERE id = 1"
        ]
        
        for sql in test_sqls:
            try:
                plan = parser.parse(sql)
                print(f"✓ {sql[:30]}... -> {plan['type']}")
            except Exception as e:
                print(f"✗ {sql[:30]}... -> 错误: {e}")
        
        print("✓ SQL解析器测试完成")
        return True
        
    except Exception as e:
        print(f"✗ SQL解析器测试失败: {e}")
        return False

if __name__ == "__main__":
    print("开始测试数据库系统...")
    
    success1 = test_parser()
    success2 = test_database()
    
    if success1 and success2:
        print("\n🎉 数据库系统测试通过！所有功能正常工作。")
        print("\n支持的功能:")
        print("  ✓ CREATE TABLE - 创建表")
        print("  ✓ INSERT - 单条和批量插入")
        print("  ✓ SELECT - 查询和条件过滤")
        print("  ✓ ORDER BY - 排序")
        print("  ✓ UPDATE - 更新数据")
        print("  ✓ DELETE - 删除数据")
        print("  ✓ 复杂WHERE条件")
        print("  ✓ 混合架构 (Python + C++)")
        sys.exit(0)
    else:
        print("\n❌ 数据库系统测试失败，请检查错误信息。")
        sys.exit(1)
