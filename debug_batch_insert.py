#!/usr/bin/env python3
"""调试批量INSERT问题"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from hybrid_db_final import HybridDatabaseEngine, SimpleSQLParser

# 测试解析器
parser = SimpleSQLParser()

print("=== 测试批量INSERT解析 ===")

# 测试解析
sql = "INSERT INTO test VALUES (1, 'a'), (2, 'b')"
print(f"SQL: {sql}")

try:
    plan = parser.parse(sql)
    print(f"解析结果: {plan}")
    print(f"values类型: {type(plan['values'])}")
    print(f"values内容: {plan['values']}")
    print(f"values长度: {len(plan['values'])}")
    
    for i, values in enumerate(plan['values']):
        print(f"  第{i+1}组: {values} (长度: {len(values)})")
        
except Exception as e:
    print(f"解析错误: {e}")
    import traceback
    traceback.print_exc()

print("\n=== 测试数据库引擎 ===")

engine = HybridDatabaseEngine()

try:
    # 创建表
    result = engine.execute("CREATE TABLE test (id INT, name STRING)")
    print(f"创建表: {result['status']}")
    
    # 测试批量INSERT
    result = engine.execute("INSERT INTO test VALUES (1, 'a'), (2, 'b')")
    print(f"批量INSERT: {result}")
    
except Exception as e:
    print(f"执行错误: {e}")
    import traceback
    traceback.print_exc()
finally:
    engine.close()
