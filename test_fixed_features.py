#!/usr/bin/env python3
"""
测试修复后的功能
"""

import sys
import os

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_varchar_support():
    """测试VARCHAR类型支持"""
    print("1. 测试VARCHAR类型支持...")
    try:
        from hybrid_db_final import HybridDatabaseEngine
        
        engine = HybridDatabaseEngine()
        
        # 测试VARCHAR类型
        result = engine.execute("CREATE TABLE test_varchar (id INT, name VARCHAR(50), description TEXT)")
        if result["status"] == "success":
            print("   ✓ VARCHAR类型支持正常")
        else:
            print(f"   ✗ VARCHAR类型支持失败: {result.get('error', '未知错误')}")
            return False
        
        # 测试插入数据
        result = engine.execute("INSERT INTO test_varchar VALUES (1, 'test_name', 'test_description')")
        if result["status"] == "success":
            print("   ✓ VARCHAR数据插入成功")
        else:
            print(f"   ✗ VARCHAR数据插入失败: {result.get('error', '未知错误')}")
            return False
        
        # 测试查询数据
        result = engine.execute("SELECT * FROM test_varchar")
        if result["status"] == "success" and len(result["data"]) == 1:
            print("   ✓ VARCHAR数据查询成功")
        else:
            print(f"   ✗ VARCHAR数据查询失败: {result.get('error', '未知错误')}")
            return False
        
        engine.close()
        return True
        
    except Exception as e:
        print(f"   ✗ VARCHAR类型测试失败: {e}")
        return False

def test_table_management():
    """测试表管理功能"""
    print("2. 测试表管理功能...")
    try:
        from hybrid_db_final import HybridDatabaseEngine
        
        engine = HybridDatabaseEngine()
        
        # 清理可能存在的旧表
        try:
            engine.execute("DROP TABLE IF EXISTS test_table")
        except:
            pass
        
        # 测试创建表
        result = engine.execute("CREATE TABLE test_table (id INT PRIMARY KEY, name STRING, age INT)")
        if result["status"] == "success":
            print("   ✓ 表创建成功")
        else:
            print(f"   ✗ 表创建失败: {result.get('error', '未知错误')}")
            return False
        
        # 测试表是否存在
        tables = engine.get_tables()
        if "test_table" in tables:
            print("   ✓ 表存在检查正常")
        else:
            print("   ✗ 表存在检查失败")
            return False
        
        # 测试表结构查询
        schema = engine.get_table_schema("test_table")
        if schema and schema["name"] == "test_table":
            print("   ✓ 表结构查询正常")
        else:
            print("   ✗ 表结构查询失败")
            return False
        
        engine.close()
        return True
        
    except Exception as e:
        print(f"   ✗ 表管理测试失败: {e}")
        return False

def test_full_workflow():
    """测试完整工作流程"""
    print("3. 测试完整工作流程...")
    try:
        from hybrid_db_final import HybridDatabaseEngine
        
        engine = HybridDatabaseEngine()
        
        # 清理可能存在的旧表
        try:
            engine.execute("DROP TABLE IF EXISTS workflow_test")
        except:
            pass
        
        # 1. 创建表
        result = engine.execute("CREATE TABLE workflow_test (id INT PRIMARY KEY, name VARCHAR(30), score DOUBLE)")
        if result["status"] != "success":
            print(f"   ✗ 创建表失败: {result.get('error', '未知错误')}")
            return False
        print("   ✓ 步骤1: 创建表成功")
        
        # 2. 插入数据
        test_data = [
            (1, "Alice", 95.5),
            (2, "Bob", 88.0),
            (3, "Charlie", 92.3)
        ]
        
        for id_val, name, score in test_data:
            result = engine.execute(f"INSERT INTO workflow_test VALUES ({id_val}, '{name}', {score})")
            if result["status"] != "success":
                print(f"   ✗ 插入数据失败: {result.get('error', '未知错误')}")
                return False
        print("   ✓ 步骤2: 插入数据成功")
        
        # 3. 查询所有数据
        result = engine.execute("SELECT * FROM workflow_test")
        if result["status"] != "success" or len(result["data"]) != 3:
            print(f"   ✗ 查询所有数据失败: {result.get('error', '未知错误')}")
            return False
        print("   ✓ 步骤3: 查询所有数据成功")
        
        # 4. 条件查询
        result = engine.execute("SELECT name FROM workflow_test WHERE score >= 90")
        if result["status"] != "success" or len(result["data"]) != 2:
            print(f"   ✗ 条件查询失败: {result.get('error', '未知错误')}")
            return False
        print("   ✓ 步骤4: 条件查询成功")
        
        # 5. 删除数据
        result = engine.execute("DELETE FROM workflow_test WHERE id = 2")
        if result["status"] != "success" or result["affected_rows"] != 1:
            print(f"   ✗ 删除数据失败: {result.get('error', '未知错误')}")
            return False
        print("   ✓ 步骤5: 删除数据成功")
        
        # 6. 验证删除
        result = engine.execute("SELECT * FROM workflow_test")
        if result["status"] != "success" or len(result["data"]) != 2:
            print(f"   ✗ 验证删除失败: {result.get('error', '未知错误')}")
            return False
        print("   ✓ 步骤6: 验证删除成功")
        
        engine.close()
        return True
        
    except Exception as e:
        print(f"   ✗ 完整工作流程测试失败: {e}")
        return False

def main():
    """主测试函数"""
    print("=== 修复后功能测试 ===")
    print()
    
    tests = [
        ("VARCHAR类型支持", test_varchar_support),
        ("表管理功能", test_table_management),
        ("完整工作流程", test_full_workflow)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"测试 {test_name}...")
        if test_func():
            passed += 1
        print()
    
    print("=== 测试结果 ===")
    print(f"通过: {passed}/{total}")
    
    if passed == total:
        print("✓ 所有功能测试通过！修复成功。")
        print()
        print("现在可以正常使用:")
        print("  - VARCHAR(长度) 类型支持")
        print("  - 表状态管理正常")
        print("  - 完整的数据操作流程")
        print()
        print("运行系统:")
        print("  python hybrid_db_final.py")
    else:
        print("✗ 部分功能测试失败，请检查错误信息。")
        return False
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
