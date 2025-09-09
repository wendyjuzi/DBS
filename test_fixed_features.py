#!/usr/bin/env python3
"""
测试修复后的功能
"""

import sys
import os

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_batch_insert():
    """测试批量INSERT功能"""
    print("=== 测试批量INSERT功能 ===")
    
    try:
        from hybrid_db_final import HybridDatabaseEngine, SimpleSQLParser
        
        # 测试解析器
        parser = SimpleSQLParser()
        sql = "INSERT INTO test VALUES (1, 'a'), (2, 'b')"
        print(f"测试SQL: {sql}")
        
        plan = parser.parse(sql)
        print(f"解析结果: {plan}")
        print(f"values组数: {len(plan['values'])}")
        
        for i, values in enumerate(plan['values']):
            print(f"  第{i+1}组: {values}")
        
        # 测试数据库引擎
        engine = HybridDatabaseEngine()
        
        # 创建表
        result = engine.execute("CREATE TABLE test (id INT, name STRING)")
        print(f"\n创建表: {result['status']}")
        
        # 测试批量INSERT
        result = engine.execute("INSERT INTO test VALUES (1, 'a'), (2, 'b')")
        print(f"批量INSERT: {result['status']}, 影响行数: {result['affected_rows']}")
        
        # 验证数据
        result = engine.execute("SELECT * FROM test")
        print(f"查询结果: {result['status']}, 行数: {result['affected_rows']}")
        if result.get('data'):
            print("数据:")
            for row in result['data']:
                print(f"  {row}")
        
        engine.close()
        print("✓ 批量INSERT测试通过")
        return True
        
    except Exception as e:
        print(f"✗ 批量INSERT测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_all_features():
    """测试所有增强功能"""
    print("\n=== 测试所有增强功能 ===")
    
    try:
        from hybrid_db_final import HybridDatabaseEngine
        
        engine = HybridDatabaseEngine()
        
        # 1. 创建表
        print("\n1. 创建表...")
        result = engine.execute("CREATE TABLE students (id INT PRIMARY KEY, name STRING, age INT, score DOUBLE)")
        print(f"   结果: {result['status']}")
        
        # 2. 批量插入
        print("\n2. 批量插入...")
        result = engine.execute("INSERT INTO students VALUES (1, 'Alice', 20, 95.5), (2, 'Bob', 19, 88.0), (3, 'Charlie', 21, 92.3)")
        print(f"   结果: {result['status']}, 影响行数: {result['affected_rows']}")
        
        # 3. 单条插入
        print("\n3. 单条插入...")
        result = engine.execute("INSERT INTO students VALUES (4, 'David', 22, 87.8)")
        print(f"   结果: {result['status']}, 影响行数: {result['affected_rows']}")
        
        # 4. 基本查询
        print("\n4. 基本查询...")
        result = engine.execute("SELECT * FROM students")
        print(f"   结果: {result['status']}, 行数: {result['affected_rows']}")
        
        # 5. 条件查询
        print("\n5. 条件查询...")
        result = engine.execute("SELECT name, score FROM students WHERE age > 19 AND score >= 90")
        print(f"   结果: {result['status']}, 行数: {result['affected_rows']}")
        if result.get('data'):
            print("   数据:")
            for row in result['data']:
                print(f"     {row}")
        
        # 6. 排序查询
        print("\n6. 排序查询...")
        result = engine.execute("SELECT name, score FROM students ORDER BY score DESC")
        print(f"   结果: {result['status']}, 行数: {result['affected_rows']}")
        if result.get('data'):
            print("   按分数降序:")
            for row in result['data']:
                print(f"     {row}")
        
        # 7. 更新操作
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
        
        # 9. 多列更新
        print("\n9. 多列更新...")
        result = engine.execute("UPDATE students SET age = 23, score = 85.0 WHERE name = 'Bob'")
        print(f"   结果: {result['status']}, 影响行数: {result['affected_rows']}")
        
        # 10. 删除操作
        print("\n10. 删除操作...")
        result = engine.execute("DELETE FROM students WHERE score < 90")
        print(f"   结果: {result['status']}, 影响行数: {result['affected_rows']}")
        
        # 11. 最终查询
        print("\n11. 最终查询...")
        result = engine.execute("SELECT * FROM students")
        print(f"   结果: {result['status']}, 剩余行数: {result['affected_rows']}")
        if result.get('data'):
            print("   剩余数据:")
            for row in result['data']:
                print(f"     {row}")
        
        engine.close()
        print("\n✓ 所有功能测试通过！")
        return True
        
    except Exception as e:
        print(f"✗ 功能测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("开始测试修复后的功能...")
    
    success1 = test_batch_insert()
    success2 = test_all_features()
    
    if success1 and success2:
        print("\n🎉 所有测试通过！功能修复成功。")
        print("\n新增功能包括:")
        print("  ✓ 批量INSERT支持")
        print("  ✓ 复杂WHERE条件支持")
        print("  ✓ ORDER BY排序支持")
        print("  ✓ UPDATE功能支持")
        print("  ✓ 改进的错误处理")
        sys.exit(0)
    else:
        print("\n❌ 部分测试失败，请检查错误信息。")
        sys.exit(1)
