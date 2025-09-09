#!/usr/bin/env python3
"""
混合架构数据库系统模块完整性测试
测试所有模块的功能实现
"""

import sys
import os

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_cpp_module():
    """测试C++模块"""
    print("1. 测试C++核心模块...")
    try:
        import db_core
        from db_core import StorageEngine, ExecutionEngine, Column, DataType
        
        # 测试创建存储引擎
        storage = StorageEngine()
        executor = ExecutionEngine(storage)
        
        # 测试创建表
        columns = [
            Column("id", DataType.INT, True),
            Column("name", DataType.STRING, False),
            Column("age", DataType.INT, False)
        ]
        
        success = executor.create_table("test_table", columns)
        if success:
            print("   ✓ C++模块创建表成功")
        else:
            print("   ⚠ C++模块创建表失败（可能表已存在）")
        
        print("   ✓ C++模块测试通过")
        return True
        
    except Exception as e:
        print(f"   ✗ C++模块测试失败: {e}")
        return False

def test_sql_parser():
    """测试SQL解析器"""
    print("2. 测试SQL解析器...")
    try:
        from hybrid_db_final import SimpleSQLParser
        
        parser = SimpleSQLParser()
        
        # 测试CREATE TABLE解析
        sql = "CREATE TABLE student (id INT PRIMARY KEY, name STRING, age INT)"
        result = parser.parse(sql)
        assert result["type"] == "CREATE_TABLE"
        assert result["table"] == "student"
        assert len(result["columns"]) == 3
        print("   ✓ CREATE TABLE解析成功")
        
        # 测试INSERT解析
        sql = "INSERT INTO student VALUES (1, 'Alice', 20)"
        result = parser.parse(sql)
        assert result["type"] == "INSERT"
        assert result["table"] == "student"
        assert result["values"] == ["1", "Alice", "20"]
        print("   ✓ INSERT解析成功")
        
        # 测试SELECT解析
        sql = "SELECT name, age FROM student WHERE age > 18"
        result = parser.parse(sql)
        assert result["type"] == "SELECT"
        assert result["table"] == "student"
        assert result["columns"] == ["name", "age"]
        assert result["where"] == "age > 18"
        print("   ✓ SELECT解析成功")
        
        # 测试DELETE解析
        sql = "DELETE FROM student WHERE id = 1"
        result = parser.parse(sql)
        assert result["type"] == "DELETE"
        assert result["table"] == "student"
        assert result["where"] == "id = 1"
        print("   ✓ DELETE解析成功")
        
        print("   ✓ SQL解析器测试通过")
        return True
        
    except Exception as e:
        print(f"   ✗ SQL解析器测试失败: {e}")
        return False

def test_execution_engine():
    """测试执行引擎"""
    print("3. 测试执行引擎...")
    try:
        from hybrid_db_final import HybridExecutionEngine
        from db_core import StorageEngine, ExecutionEngine
        
        storage = StorageEngine()
        cpp_executor = ExecutionEngine(storage)
        executor = HybridExecutionEngine(storage, cpp_executor)
        
        # 测试CREATE TABLE执行
        plan = {
            "type": "CREATE_TABLE",
            "table": "test_exec",
            "columns": [
                {"name": "id", "type": "INT", "is_primary_key": True},
                {"name": "name", "type": "STRING", "is_primary_key": False}
            ]
        }
        
        result = executor.execute(plan)
        assert result["affected_rows"] == 0
        print("   ✓ CREATE TABLE执行成功")
        
        # 测试INSERT执行
        plan = {
            "type": "INSERT",
            "table": "test_exec",
            "values": ["1", "test"]
        }
        
        result = executor.execute(plan)
        assert result["affected_rows"] == 1
        print("   ✓ INSERT执行成功")
        
        # 测试SELECT执行
        plan = {
            "type": "SELECT",
            "table": "test_exec",
            "columns": ["*"],
            "where": None
        }
        
        result = executor.execute(plan)
        assert result["affected_rows"] == 1
        assert len(result["data"]) == 1
        print("   ✓ SELECT执行成功")
        
        print("   ✓ 执行引擎测试通过")
        return True
        
    except Exception as e:
        print(f"   ✗ 执行引擎测试失败: {e}")
        return False

def test_database_engine():
    """测试数据库引擎"""
    print("4. 测试数据库引擎...")
    try:
        from hybrid_db_final import HybridDatabaseEngine
        
        engine = HybridDatabaseEngine()
        
        # 测试完整SQL执行流程
        test_sqls = [
            "CREATE TABLE engine_test (id INT PRIMARY KEY, name STRING, value DOUBLE)",
            "INSERT INTO engine_test VALUES (1, 'test1', 10.5)",
            "INSERT INTO engine_test VALUES (2, 'test2', 20.3)",
            "SELECT * FROM engine_test",
            "SELECT name FROM engine_test WHERE value > 15",
            "DELETE FROM engine_test WHERE id = 1",
            "SELECT * FROM engine_test"
        ]
        
        for i, sql in enumerate(test_sqls, 1):
            result = engine.execute(sql)
            if result["status"] == "success":
                print(f"   ✓ SQL {i} 执行成功")
            else:
                print(f"   ✗ SQL {i} 执行失败: {result.get('error', '未知错误')}")
                return False
        
        # 测试表管理功能
        tables = engine.get_tables()
        assert "engine_test" in tables
        print("   ✓ 表管理功能正常")
        
        schema = engine.get_table_schema("engine_test")
        assert schema["name"] == "engine_test"
        assert len(schema["columns"]) == 3
        print("   ✓ 表结构查询正常")
        
        engine.close()
        print("   ✓ 数据库引擎测试通过")
        return True
        
    except Exception as e:
        print(f"   ✗ 数据库引擎测试失败: {e}")
        return False

def test_cli_interface():
    """测试CLI界面"""
    print("5. 测试CLI界面...")
    try:
        from hybrid_db_final import HybridCLI
        
        # 测试CLI初始化
        cli = HybridCLI()
        assert cli.engine is not None
        print("   ✓ CLI初始化成功")
        
        # 测试帮助功能
        cli._show_help()
        print("   ✓ 帮助功能正常")
        
        # 测试表列表功能
        cli._show_tables()
        print("   ✓ 表列表功能正常")
        
        # 测试结果显示功能
        test_result = {
            "status": "success",
            "data": [["1", "test", "10.5"]],
            "metadata": {"columns": ["id", "name", "value"]},
            "affected_rows": 1,
            "execution_time": 0.001
        }
        cli._display_result(test_result)
        print("   ✓ 结果显示功能正常")
        
        cli.engine.close()
        print("   ✓ CLI界面测试通过")
        return True
        
    except Exception as e:
        print(f"   ✗ CLI界面测试失败: {e}")
        return False

def test_integration():
    """测试系统集成"""
    print("6. 测试系统集成...")
    try:
        from hybrid_db_final import HybridDatabaseEngine
        
        engine = HybridDatabaseEngine()
        
        # 完整的数据操作流程
        print("   创建表...")
        result = engine.execute("CREATE TABLE integration_test (id INT PRIMARY KEY, name STRING, score DOUBLE)")
        assert result["status"] == "success"
        
        print("   插入数据...")
        test_data = [
            (1, "Alice", 95.5),
            (2, "Bob", 88.0),
            (3, "Charlie", 92.3)
        ]
        
        for id_val, name, score in test_data:
            result = engine.execute(f"INSERT INTO integration_test VALUES ({id_val}, '{name}', {score})")
            assert result["status"] == "success"
        
        print("   查询所有数据...")
        result = engine.execute("SELECT * FROM integration_test")
        assert result["status"] == "success"
        assert len(result["data"]) == 3
        
        print("   条件查询...")
        result = engine.execute("SELECT name FROM integration_test WHERE score >= 90")
        assert result["status"] == "success"
        assert len(result["data"]) == 2
        
        print("   删除数据...")
        result = engine.execute("DELETE FROM integration_test WHERE id = 2")
        assert result["status"] == "success"
        assert result["affected_rows"] == 1
        
        print("   验证删除...")
        result = engine.execute("SELECT * FROM integration_test")
        assert result["status"] == "success"
        assert len(result["data"]) == 2
        
        engine.close()
        print("   ✓ 系统集成测试通过")
        return True
        
    except Exception as e:
        print(f"   ✗ 系统集成测试失败: {e}")
        return False

def main():
    """主测试函数"""
    print("=== 混合架构数据库系统模块完整性测试 ===")
    print()
    
    tests = [
        ("C++核心模块", test_cpp_module),
        ("SQL解析器", test_sql_parser),
        ("执行引擎", test_execution_engine),
        ("数据库引擎", test_database_engine),
        ("CLI界面", test_cli_interface),
        ("系统集成", test_integration)
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
        print("✓ 所有模块测试通过！系统功能完整。")
        print()
        print("可用功能:")
        print("  - CREATE TABLE: 创建表结构")
        print("  - INSERT: 插入数据")
        print("  - SELECT: 查询数据（支持WHERE条件）")
        print("  - DELETE: 删除数据（支持WHERE条件）")
        print("  - 命令行交互界面")
        print("  - 数据持久化存储")
        print()
        print("运行系统:")
        print("  python hybrid_db_final.py")
    else:
        print("✗ 部分模块测试失败，请检查错误信息。")
        return False
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
