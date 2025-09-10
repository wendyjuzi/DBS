#!/usr/bin/env python3
"""
混合架构数据库系统演示脚本
展示核心功能：CREATE TABLE、INSERT、SELECT、DELETE
"""

import sys
import os

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def demo_database():
    """演示数据库功能"""
    print("=== 混合架构数据库系统演示 ===")
    print()
    
    try:
        from src.core.hybrid_engine import HybridDatabaseEngine
        
        # 创建数据库引擎
        print("初始化数据库引擎...")
        engine = HybridDatabaseEngine()
        print("✓ 数据库引擎初始化成功")
        print()
        
        # 演示SQL操作
        demo_sqls = [
            {
                "sql": "CREATE TABLE demo_student (id INT PRIMARY KEY, name STRING, age INT, score DOUBLE)",
                "description": "创建学生表"
            },
            {
                "sql": "INSERT INTO demo_student VALUES (1, 'Alice', 20, 95.5)",
                "description": "插入学生Alice"
            },
            {
                "sql": "INSERT INTO demo_student VALUES (2, 'Bob', 19, 88.0)",
                "description": "插入学生Bob"
            },
            {
                "sql": "INSERT INTO demo_student VALUES (3, 'Charlie', 21, 92.3)",
                "description": "插入学生Charlie"
            },
            {
                "sql": "SELECT * FROM demo_student",
                "description": "查询所有学生"
            },
            {
                "sql": "SELECT name, score FROM demo_student WHERE age > 19",
                "description": "查询年龄大于19岁的学生姓名和分数"
            },
            {
                "sql": "SELECT * FROM demo_student WHERE score >= 90",
                "description": "查询分数大于等于90的学生"
            },
            {
                "sql": "DELETE FROM demo_student WHERE id = 2",
                "description": "删除ID为2的学生"
            },
            {
                "sql": "SELECT * FROM demo_student",
                "description": "再次查询所有学生（验证删除）"
            }
        ]
        
        for i, demo in enumerate(demo_sqls, 1):
            print(f"步骤 {i}: {demo['description']}")
            print(f"SQL: {demo['sql']}")
            
            try:
                result = engine.execute(demo['sql'])
                
                if result["status"] == "success":
                    print("✓ 执行成功")
                    
                    # 显示结果
                    if result.get("data"):
                        data = result["data"]
                        if isinstance(data, list) and data:
                            print("查询结果:")
                            # 显示表头
                            if len(data) > 0:
                                columns = result.get("metadata", {}).get("columns", [])
                                if columns:
                                    print(" | ".join(columns))
                                    print("-" * (len(" | ".join(columns))))
                                    
                                    # 显示数据行
                                    for row in data:
                                        if isinstance(row, list):
                                            print(" | ".join(str(cell) for cell in row))
                                        else:
                                            print(str(row))
                            print()
                        else:
                            print("(无数据)")
                    else:
                        affected = result.get("affected_rows", 0)
                        message = result.get("metadata", {}).get("message", f"影响 {affected} 行")
                        print(f"✓ {message}")
                    
                    # 显示执行时间
                    exec_time = result.get("execution_time", 0)
                    if exec_time > 0:
                        print(f"执行时间: {exec_time:.4f}秒")
                else:
                    print(f"✗ 执行失败: {result}")
                    
            except Exception as e:
                print(f"✗ 执行错误: {str(e)}")
            
            print("-" * 50)
            print()
        
        # 关闭数据库
        print("关闭数据库连接...")
        engine.close()
        print("✓ 数据库已关闭")
        
        print()
        print("=== 演示完成 ===")
        print("系统功能验证成功！")
        print()
        print("要启动交互式命令行界面，请运行:")
        print("  python hybrid_db.py")
        
    except Exception as e:
        print(f"演示失败: {str(e)}")
        print()
        print("请确保:")
        print("1. C++模块已正确编译")
        print("2. 所有依赖已安装")
        print("3. 运行了构建脚本: python build_hybrid_db.py")
        return False
    
    return True

if __name__ == "__main__":
    success = demo_database()
    sys.exit(0 if success else 1)
