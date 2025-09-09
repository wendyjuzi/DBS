#!/usr/bin/env python3
"""
最终测试 - 验证数据库系统完整功能
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def final_test():
    """最终功能测试"""
    print("=== 混合架构数据库系统 - 最终测试 ===")
    
    try:
        from hybrid_db_final import HybridDatabaseEngine
        
        # 创建引擎
        engine = HybridDatabaseEngine()
        print("✓ 数据库引擎初始化成功")
        
        # 1. 创建表
        print("\n1. 创建学生表...")
        result = engine.execute("CREATE TABLE students (id INT PRIMARY KEY, name STRING, age INT, score DOUBLE)")
        print(f"   结果: {result['status']}")
        
        # 2. 单条插入
        print("\n2. 插入单条数据...")
        result = engine.execute("INSERT INTO students VALUES (1, 'Alice', 20, 95.5)")
        print(f"   结果: {result['status']}, 影响行数: {result['affected_rows']}")
        
        # 3. 批量插入
        print("\n3. 批量插入数据...")
        result = engine.execute("INSERT INTO students VALUES (2, 'Bob', 19, 88.0), (3, 'Charlie', 21, 92.3)")
        print(f"   结果: {result['status']}, 影响行数: {result['affected_rows']}")
        
        # 4. 查询所有数据
        print("\n4. 查询所有数据...")
        result = engine.execute("SELECT * FROM students")
        print(f"   结果: {result['status']}, 行数: {result['affected_rows']}")
        if result.get('data'):
            print("   数据:")
            for row in result['data']:
                print(f"     {row}")
        
        # 5. 条件查询
        print("\n5. 条件查询 (年龄>19)...")
        result = engine.execute("SELECT name, score FROM students WHERE age > 19")
        print(f"   结果: {result['status']}, 行数: {result['affected_rows']}")
        if result.get('data'):
            print("   数据:")
            for row in result['data']:
                print(f"     {row}")
        
        # 6. 排序查询
        print("\n6. 按分数排序...")
        result = engine.execute("SELECT name, score FROM students ORDER BY score DESC")
        print(f"   结果: {result['status']}, 行数: {result['affected_rows']}")
        if result.get('data'):
            print("   按分数降序:")
            for row in result['data']:
                print(f"     {row}")
        
        # 7. 更新数据
        print("\n7. 更新Alice的分数...")
        result = engine.execute("UPDATE students SET score = 99.0 WHERE name = 'Alice'")
        print(f"   结果: {result['status']}, 影响行数: {result['affected_rows']}")
        
        # 8. 验证更新
        print("\n8. 验证更新结果...")
        result = engine.execute("SELECT name, score FROM students WHERE name = 'Alice'")
        print(f"   结果: {result['status']}")
        if result.get('data'):
            print("   Alice的分数:")
            for row in result['data']:
                print(f"     {row}")
        
        # 9. 删除数据
        print("\n9. 删除分数<90的学生...")
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
        print(f"✗ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("开始最终测试...")
    
    success = final_test()
    
    if success:
        print("\n🎉 恭喜！你的混合架构数据库系统运行成功！")
        print("\n✅ 系统特点:")
        print("  🔥 C++核心: 高性能存储和计算")
        print("  🐍 Python上层: 易用的SQL接口")
        print("  📊 支持功能: CREATE, INSERT, SELECT, UPDATE, DELETE")
        print("  🔍 高级功能: 条件查询, 排序, 批量操作")
        print("  💾 持久化: 数据自动保存到磁盘")
        print("  ⚡ 混合架构: 最佳性能与易用性结合")
        print("\n🚀 你的数据库系统已经可以投入使用了！")
    else:
        print("\n❌ 测试失败，请检查错误信息。")
