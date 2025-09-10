#!/usr/bin/env python3
"""详细调试错误"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def debug_detailed():
    """详细调试"""
    print("=== 详细调试数据库错误 ===")
    
    try:
        from hybrid_db_final import HybridDatabaseEngine
        
        engine = HybridDatabaseEngine()
        print("✓ 引擎创建成功")
        
        # 测试创建表
        print("\n1. 测试创建表...")
        result = engine.execute("CREATE TABLE test (id INT, name STRING)")
        print(f"   完整结果: {result}")
        
        if result['status'] == 'error':
            print(f"   错误详情: {result.get('error', '未知错误')}")
            
            # 检查是否是表已存在的问题
            if "已存在" in str(result.get('error', '')):
                print("   -> 表已存在，尝试删除后重新创建")
                # 这里可以添加删除表的逻辑
            else:
                print("   -> 其他错误，需要进一步调试")
        
        engine.close()
        
    except Exception as e:
        print(f"✗ 异常: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_detailed()
