#!/usr/bin/env python3
"""
快速测试修复后的功能
"""

import sys
import os

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def main():
    print("=== 快速测试修复后的功能 ===")
    
    try:
        from hybrid_db_final import HybridDatabaseEngine
        
        engine = HybridDatabaseEngine()
        
        # 测试VARCHAR类型
        print("1. 测试VARCHAR类型支持...")
        result = engine.execute("CREATE TABLE test (id INT, name VARCHAR(50))")
        if result["status"] == "success":
            print("   ✓ VARCHAR类型支持正常")
        else:
            print(f"   ✗ VARCHAR类型支持失败: {result.get('error', '未知错误')}")
            return False
        
        # 测试插入数据
        print("2. 测试数据插入...")
        result = engine.execute("INSERT INTO test VALUES (1, 'test_name')")
        if result["status"] == "success":
            print("   ✓ 数据插入成功")
        else:
            print(f"   ✗ 数据插入失败: {result.get('error', '未知错误')}")
            return False
        
        # 测试查询数据
        print("3. 测试数据查询...")
        result = engine.execute("SELECT * FROM test")
        if result["status"] == "success" and len(result["data"]) == 1:
            print("   ✓ 数据查询成功")
            print(f"   查询结果: {result['data']}")
        else:
            print(f"   ✗ 数据查询失败: {result.get('error', '未知错误')}")
            return False
        
        # 测试条件查询
        print("4. 测试条件查询...")
        result = engine.execute("SELECT name FROM test WHERE id = 1")
        if result["status"] == "success" and len(result["data"]) == 1:
            print("   ✓ 条件查询成功")
            print(f"   查询结果: {result['data']}")
        else:
            print(f"   ✗ 条件查询失败: {result.get('error', '未知错误')}")
            return False
        
        engine.close()
        
        print("\n=== 测试结果 ===")
        print("✓ 所有功能测试通过！")
        print("\n修复内容:")
        print("  - ✅ C++模块复制问题已修复")
        print("  - ✅ VARCHAR(长度) 类型支持已添加")
        print("  - ✅ 表状态管理已修复")
        print("\n现在可以正常使用:")
        print("  python hybrid_db_final.py")
        
        return True
        
    except Exception as e:
        print(f"✗ 测试失败: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
