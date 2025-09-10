#!/usr/bin/env python3
"""调试错误信息"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from hybrid_db_final import HybridDatabaseEngine
    
    engine = HybridDatabaseEngine()
    print("引擎创建成功")
    
    # 测试创建表
    result = engine.execute("CREATE TABLE test (id INT, name STRING)")
    print(f"创建表结果: {result}")
    
    if result['status'] == 'error':
        print(f"错误详情: {result.get('error', '未知错误')}")
    
    engine.close()
    
except Exception as e:
    print(f"异常: {e}")
    import traceback
    traceback.print_exc()
