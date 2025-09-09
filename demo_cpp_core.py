#!/usr/bin/env python3
"""
演示C++核心功能
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def demo_cpp_core():
    """演示C++核心功能"""
    print("=== C++核心功能演示 ===")
    
    try:
        # 直接导入C++模块
        from db_core import StorageEngine, ExecutionEngine, Column, DataType
        
        print("✓ C++模块导入成功")
        
        # 创建存储引擎
        storage = StorageEngine()
        executor = ExecutionEngine(storage)
        print("✓ C++存储引擎和执行引擎创建成功")
        
        # 定义表结构
        columns = [
            Column("id", DataType.INT, True),
            Column("name", DataType.STRING, False),
            Column("age", DataType.INT, False),
            Column("score", DataType.DOUBLE, False)
        ]
        
        # 创建表
        success = executor.create_table("students", columns)
        print(f"✓ 创建表: {'成功' if success else '失败'}")
        
        # 插入数据
        data1 = ["1", "Alice", "20", "95.5"]
        data2 = ["2", "Bob", "19", "88.0"]
        
        success1 = executor.insert("students", data1)
        success2 = executor.insert("students", data2)
        print(f"✓ 插入数据: 第1条{'成功' if success1 else '失败'}, 第2条{'成功' if success2 else '失败'}")
        
        # 全表扫描
        rows = executor.seq_scan("students")
        print(f"✓ 全表扫描: 找到 {len(rows)} 行数据")
        
        for i, row in enumerate(rows):
            print(f"  行{i+1}: {row.get_values()}")
        
        # 条件过滤
        def age_filter(values):
            return int(values[2]) > 19  # age > 19
        
        filtered_rows = executor.filter("students", age_filter)
        print(f"✓ 条件过滤: 找到 {len(filtered_rows)} 行满足条件")
        
        for i, row in enumerate(filtered_rows):
            print(f"  过滤行{i+1}: {row.get_values()}")
        
        # 列投影
        projected_data = executor.project("students", filtered_rows, ["name", "score"])
        print(f"✓ 列投影: 投影 {len(projected_data)} 行数据")
        
        for i, row in enumerate(projected_data):
            print(f"  投影行{i+1}: {row}")
        
        # 刷盘
        storage.flush_all_dirty_pages()
        print("✓ 数据已刷盘到磁盘")
        
        print("\n🎉 C++核心功能演示完成！")
        print("\nC++核心特点:")
        print("  ✓ 高性能数据处理")
        print("  ✓ 4KB页管理")
        print("  ✓ 序列化/反序列化")
        print("  ✓ 内存优化")
        print("  ✓ 磁盘持久化")
        
        return True
        
    except Exception as e:
        print(f"✗ C++核心演示失败: {e}")
        import traceback
        traceback.print_exc()
        return False

def demo_hybrid_architecture():
    """演示混合架构"""
    print("\n=== 混合架构演示 ===")
    
    try:
        from hybrid_db_final import HybridDatabaseEngine
        
        engine = HybridDatabaseEngine()
        print("✓ 混合架构数据库引擎创建成功")
        
        # 使用SQL接口
        result = engine.execute("CREATE TABLE demo (id INT, name STRING, value DOUBLE)")
        print(f"✓ SQL创建表: {result['status']}")
        
        result = engine.execute("INSERT INTO demo VALUES (1, 'test', 3.14)")
        print(f"✓ SQL插入: {result['status']}, 影响行数: {result['affected_rows']}")
        
        result = engine.execute("SELECT * FROM demo")
        print(f"✓ SQL查询: {result['status']}, 行数: {result['affected_rows']}")
        if result.get('data'):
            print(f"  数据: {result['data']}")
        
        engine.close()
        print("✓ 混合架构演示完成！")
        
        print("\n混合架构优势:")
        print("  ✓ Python: 易用的SQL接口")
        print("  ✓ C++: 高性能数据处理")
        print("  ✓ 最佳性能与易用性结合")
        
        return True
        
    except Exception as e:
        print(f"✗ 混合架构演示失败: {e}")
        return False

if __name__ == "__main__":
    print("开始演示C++核心和混合架构...")
    
    success1 = demo_cpp_core()
    success2 = demo_hybrid_architecture()
    
    if success1 and success2:
        print("\n🎉 所有演示成功！你的数据库系统确实是基于C++实现的混合架构。")
    else:
        print("\n❌ 部分演示失败。")
