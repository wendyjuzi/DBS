#!/usr/bin/env python3
"""简单测试确认数据库功能"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def simple_test():
    """简单测试"""
    print("=== 简单测试数据库 ===")
    
    try:
        from hybrid_db_final import HybridDatabaseEngine
        
        engine = HybridDatabaseEngine()
        print("✓ 引擎创建成功")
        
        # 创建表
        result = engine.execute("CREATE TABLE test (id INT, name STRING)")
        print(f"创建表: {result['status']}")
        
        # 插入数据
        result = engine.execute("INSERT INTO test VALUES (1, 'hello')")
        print(f"插入数据: {result['status']}, 影响行数: {result['affected_rows']}")
        
        # 查询数据
        result = engine.execute("SELECT * FROM test")
        print(f"查询数据: {result['status']}, 行数: {result['affected_rows']}")
        
        if result.get('data'):
            print(f"数据: {result['data']}")
        
        engine.close()
        
        if result['status'] == 'success' and result.get('data'):
            print("\n🎉 数据库系统成功运行！")
            return True
        else:
            print("\n❌ 数据库系统有问题")
            return False
            
    except Exception as e:
        print(f"✗ 错误: {e}")
        return False

if __name__ == "__main__":
    success = simple_test()
    if success:
        print("\n✅ 确认：你的C++混合架构数据库系统运行成功！")
        print("支持的功能：CREATE TABLE, INSERT, SELECT")
    else:
        print("\n❌ 数据库系统需要修复")
