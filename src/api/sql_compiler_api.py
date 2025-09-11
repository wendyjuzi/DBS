"""
使用 modules/sql_compiler 的完整SQL编译器API
"""

import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

# 添加项目根目录到路径
proj_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(proj_root))

from modules.sql_compiler.lexical.lexer import Lexer
from modules.sql_compiler.syntax.parser import Parser, ParseError
from modules.sql_compiler.semantic.semantic import SemanticAnalyzer, Catalog
from modules.sql_compiler.planner.planner import Planner
from src.core.executor.hybrid_executor import HybridExecutionEngine
from src.utils.exceptions import ExecutionError, SQLSyntaxError


class SQLCompilerAPI:
    """使用完整SQL编译器的数据库API"""
    
    def __init__(self):
        # 初始化SQL编译器组件
        self.catalog = Catalog()
        self.semantic_analyzer = SemanticAnalyzer(self.catalog)
        
        # 初始化C++执行引擎
        try:
            import db_core
            self.storage_engine = db_core.StorageEngine()
            self.execution_engine = db_core.ExecutionEngine(self.storage_engine)
            self.hybrid_executor = HybridExecutionEngine(self.storage_engine, self.execution_engine)
            print("[SQL_COMPILER] C++执行引擎初始化成功")
        except ImportError as e:
            print(f"[SQL_COMPILER] C++执行引擎初始化失败: {e}")
            raise ExecutionError("C++执行引擎不可用")
    
    def execute(self, sql: str) -> Dict[str, Any]:
        """
        执行SQL语句
        
        Args:
            sql: SQL语句字符串
            
        Returns:
            执行结果字典
        """
        print(f"[SQL_COMPILER] 执行SQL: {sql.strip()}")
        
        try:
            # 1. 词法分析
            lexer = Lexer(sql)
            tokens, errors = lexer.tokenize()
            
            if errors:
                error_msg = f"词法分析错误: {errors[0]}"
                print(f"[SQL_COMPILER] {error_msg}")
                raise SQLSyntaxError(error_msg)
            
            print(f"[SQL_COMPILER] 词法分析成功，生成 {len(tokens)} 个token")
            
            # 2. 语法分析
            parser = Parser(tokens)
            ast_list = parser.parse()
            
            print(f"[SQL_COMPILER] 语法分析成功，生成 {len(ast_list)} 个AST节点")
            
            # 3. 语义分析
            semantic_errors = 0
            for ast in ast_list:
                try:
                    self.semantic_analyzer.analyze(ast)
                    print(f"[SQL_COMPILER] 语义检查通过: {ast.node_type}")
                except Exception as e:
                    print(f"[SQL_COMPILER] 语义检查失败: {e}")
                    semantic_errors += 1
            
            if semantic_errors > 0:
                raise SQLSyntaxError(f"语义分析失败，检测到 {semantic_errors} 个错误")
            
            # 4. 执行计划生成
            ast_list_dict = [ast.to_dict() for ast in ast_list]
            planner = Planner(ast_list_dict, enable_optimization=True)
            plans = planner.generate_plan()
            
            print(f"[SQL_COMPILER] 执行计划生成成功，生成 {len(plans)} 个计划")
            
            # 5. 执行计划
            results = []
            for plan in plans:
                print(f"[SQL_COMPILER] 执行计划: {plan}")
                # 将LogicalPlan对象转换为字典格式
                plan_dict = plan.to_dict()
                result = self.hybrid_executor.execute(plan_dict)
                results.append(result)
            
            # 返回最后一个结果（通常是主要结果）
            if results:
                return results[-1]
            else:
                return {"status": "success", "affected_rows": 0, "data": []}
                
        except ParseError as e:
            print(f"[SQL_COMPILER] 语法分析错误: {e}")
            raise SQLSyntaxError(f"语法分析错误: {e}")
        except Exception as e:
            print(f"[SQL_COMPILER] 执行错误: {e}")
            raise ExecutionError(f"SQL执行错误: {e}")
    
    def flush(self):
        """刷盘所有脏页"""
        try:
            self.storage_engine.flush_all_dirty_pages()
            print("[SQL_COMPILER] 数据刷盘完成")
        except Exception as e:
            print(f"[SQL_COMPILER] 刷盘失败: {e}")
    
    def get_catalog_info(self) -> Dict[str, Any]:
        """获取系统目录信息"""
        try:
            table_names = self.storage_engine.get_table_names()
            catalog_info = {}
            for table_name in table_names:
                columns = self.storage_engine.get_table_columns(table_name)
                catalog_info[table_name] = {
                    "columns": columns,
                    "has_index": self.storage_engine.has_index(table_name),
                    "index_size": self.storage_engine.get_index_size(table_name)
                }
            return catalog_info
        except Exception as e:
            print(f"[SQL_COMPILER] 获取目录信息失败: {e}")
            return {}


def create_sql_compiler_api() -> SQLCompilerAPI:
    """创建SQL编译器API实例"""
    return SQLCompilerAPI()


if __name__ == "__main__":
    # 测试SQL编译器API
    api = create_sql_compiler_api()
    
    # 测试基本功能
    print("=== SQL编译器API测试 ===")
    
    # 创建表（适配现有语法分析器，不支持PRIMARY KEY）
    print("\n1. 创建表")
    result = api.execute("CREATE TABLE test_table(id INT, name STRING, age INT);")
    print("结果:", result)
    
    # 插入数据
    print("\n2. 插入数据")
    result = api.execute("INSERT INTO test_table VALUES (1, 'Alice', 20);")
    print("结果:", result)
    
    # 查询数据
    print("\n3. 查询数据")
    result = api.execute("SELECT * FROM test_table;")
    print("结果:", result)
    
    # 获取目录信息
    print("\n4. 目录信息")
    catalog_info = api.get_catalog_info()
    print("目录信息:", catalog_info)
    
    # 刷盘
    print("\n5. 刷盘")
    api.flush()
    
    print("\n=== 测试完成 ===")
