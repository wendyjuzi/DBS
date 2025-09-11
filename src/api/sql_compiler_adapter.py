"""
SQL编译器适配器
将 modules/sql_compiler 的输出格式转换为执行器期望的格式
不修改编译器本身，只做格式转换
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

# 导入混合存储引擎
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from hybrid_storage_engine import HybridStorageEngine


class SQLCompilerAdapter:
    """SQL编译器适配器 - 不修改编译器，只做格式转换"""
    
    def __init__(self, use_hybrid_storage: bool = True, cache_capacity: int = 100, cache_strategy: str = "LRU"):
        # 初始化SQL编译器组件
        self.catalog = Catalog()
        self.semantic_analyzer = SemanticAnalyzer(self.catalog)
        
        # 初始化存储引擎
        if use_hybrid_storage:
            try:
                # 使用混合存储引擎（集成OS存储缓存系统）
                self.hybrid_storage = HybridStorageEngine(
                    cache_capacity=cache_capacity,
                    cache_strategy=cache_strategy,
                    enable_cpp_acceleration=True
                )
                print("[ADAPTER] 混合存储引擎初始化成功")
                
                # 为了兼容现有接口，创建传统的执行引擎
                import db_core
                self.storage_engine = db_core.StorageEngine()
                self.execution_engine = db_core.ExecutionEngine(self.storage_engine)
                self.hybrid_executor = HybridExecutionEngine(self.storage_engine, self.execution_engine)
                print("[ADAPTER] C++执行引擎初始化成功")
                
            except Exception as e:
                print(f"[ADAPTER] 混合存储引擎初始化失败: {e}")
                # 回退到传统C++引擎
                try:
                    import db_core
                    self.storage_engine = db_core.StorageEngine()
                    self.execution_engine = db_core.ExecutionEngine(self.storage_engine)
                    self.hybrid_executor = HybridExecutionEngine(self.storage_engine, self.execution_engine)
                    self.hybrid_storage = None
                    print("[ADAPTER] 回退到传统C++执行引擎")
                except ImportError as e:
                    print(f"[ADAPTER] C++执行引擎初始化失败: {e}")
                    raise ExecutionError("C++执行引擎不可用")
        else:
            # 使用传统C++引擎
            try:
                import db_core
                self.storage_engine = db_core.StorageEngine()
                self.execution_engine = db_core.ExecutionEngine(self.storage_engine)
                self.hybrid_executor = HybridExecutionEngine(self.storage_engine, self.execution_engine)
                self.hybrid_storage = None
                print("[ADAPTER] C++执行引擎初始化成功")
            except ImportError as e:
                print(f"[ADAPTER] C++执行引擎初始化失败: {e}")
                raise ExecutionError("C++执行引擎不可用")
    
    def _convert_plan_to_executor_format(self, compiler_plan) -> Dict[str, Any]:
        """
        将SQL编译器的计划格式转换为执行器期望的格式
        不修改编译器，只做格式转换
        """
        plan_dict = compiler_plan.to_dict()
        plan_type = plan_dict.get("type")
        
        print(f"[ADAPTER] 转换计划类型: {plan_type}")
        
        if plan_type == "CreateTable":
            # 转换CREATE TABLE计划
            return {
                "type": "CREATE_TABLE",
                "table": plan_dict["props"]["table"],
                "columns": plan_dict["props"]["columns"]
            }
        
        elif plan_type == "Insert":
            # 转换INSERT计划
            # 从children中提取values
            values = []
            for child in plan_dict.get("children", []):
                if child.get("type") == "Values":
                    rows = child.get("props", {}).get("rows", [])
                    if rows:
                        values = rows[0]  # 取第一行数据
                    break
            
            return {
                "type": "INSERT",
                "table": plan_dict["props"]["table"],
                "values": values
            }
        
        elif plan_type in ["Select", "Project"]:
            # 转换SELECT/Project计划
            # 从children中查找实际的表扫描操作
            table_name = ""
            columns = []
            conditions = []
            joins = []
            group_by = []
            order_by = []
            
            # 递归查找表名和条件
            def find_table_info(node):
                nonlocal table_name, conditions, joins, group_by, order_by
                
                if node.get("type") == "SeqScan":
                    table_name = node.get("props", {}).get("table", "")
                    # 提取WHERE条件
                    seq_scan_props = node.get("props", {})
                    if "conditions" in seq_scan_props:
                        conditions = seq_scan_props["conditions"]
                    elif "condition" in seq_scan_props:
                        # 单个条件转换为列表
                        conditions = [seq_scan_props["condition"]]
                elif node.get("type") in ["InnerJoin", "LeftJoin", "RightJoin"]:
                    join_info = {
                        "type": node.get("type", "InnerJoin"),
                        "table": node.get("props", {}).get("right_table", ""),
                        "on": node.get("props", {}).get("condition", "")
                    }
                    joins.append(join_info)
                elif node.get("type") == "GroupBy":
                    group_by = node.get("props", {}).get("group_columns", [])
                elif node.get("type") == "Sort":
                    order_by = node.get("props", {}).get("order_columns", [])
                
                # 递归处理子节点
                for child in node.get("children", []):
                    find_table_info(child)
            
            # 查找表信息
            find_table_info(plan_dict)
            
            # 获取投影列
            if plan_type == "Project":
                columns = plan_dict.get("props", {}).get("columns", [])
            else:
                columns = plan_dict.get("props", {}).get("columns", [])
            
            # 将conditions转换为filter_conditions格式
            filter_conditions = []
            if conditions:
                for condition in conditions:
                    filter_conditions.append({
                        "column": condition.get("left", ""),
                        "op": condition.get("op", "="),
                        "value": condition.get("right", "")
                    })
            
            result = {
                "type": "SELECT",
                "table": table_name,
                "columns": columns,
                "filter": filter_conditions
            }
            
            # 添加高级功能信息
            if joins:
                result["joins"] = joins
            if group_by:
                result["group_by"] = group_by
            if order_by:
                result["order_by"] = order_by
            
            return result
        
        elif plan_type == "Update":
            # 转换UPDATE计划
            return {
                "type": "UPDATE",
                "table": plan_dict["props"]["table"],
                "set_clause": plan_dict["props"].get("set_clause", {}),
                "where_clause": plan_dict["props"].get("where_clause", {})
            }
        
        elif plan_type == "Delete":
            # 转换DELETE计划
            return {
                "type": "DELETE",
                "table": plan_dict["props"]["table"],
                "where_clause": plan_dict["props"].get("where_clause", {})
            }
        
        elif plan_type == "DropTable":
            # 转换DROP TABLE计划
            return {
                "type": "DROP_TABLE",
                "table": plan_dict["props"]["table"]
            }
        
        elif plan_type in ["InnerJoin", "LeftJoin", "RightJoin"]:
            # 转换JOIN计划
            return {
                "type": "SELECT",
                "tables": [plan_dict["props"].get("left_table", ""), plan_dict["props"].get("right_table", "")],
                "joins": [{
                    "type": plan_type.replace("Join", "").upper(),
                    "table": plan_dict["props"].get("right_table", ""),
                    "on": plan_dict["props"].get("condition", "")
                }],
                "columns": plan_dict["props"].get("columns", [])
            }
        
        elif plan_type == "GroupBy":
            # 转换GROUP BY计划
            return {
                "type": "SELECT",
                "table": plan_dict["props"].get("table", ""),
                "columns": plan_dict["props"].get("columns", []),
                "group_by": plan_dict["props"].get("group_columns", [])
            }
        
        elif plan_type == "Sort":
            # 转换ORDER BY计划
            return {
                "type": "SELECT",
                "table": plan_dict["props"].get("table", ""),
                "columns": plan_dict["props"].get("columns", []),
                "order_by": plan_dict["props"].get("order_columns", [])
            }
        
        else:
            # 未知类型，直接返回原始格式
            print(f"[ADAPTER] 未知计划类型: {plan_type}，使用原始格式")
            return plan_dict
    
    def execute(self, sql: str) -> Dict[str, Any]:
        """
        执行SQL语句
        使用SQL编译器进行解析，然后转换为执行器格式
        """
        # 预处理SQL语句：去除首尾空白，标准化换行符
        sql = sql.strip()
        print(f"[ADAPTER] 执行SQL: {sql}")
        
        try:
            # 1. 词法分析
            lexer = Lexer(sql)
            tokens, errors = lexer.tokenize()
            
            if errors:
                error_msg = f"词法分析错误: {errors[0]}"
                print(f"[ADAPTER] {error_msg}")
                raise SQLSyntaxError(error_msg)
            
            print(f"[ADAPTER] 词法分析成功，生成 {len(tokens)} 个token")
            
            # 2. 语法分析
            parser = Parser(tokens)
            ast_list = parser.parse()
            
            print(f"[ADAPTER] 语法分析成功，生成 {len(ast_list)} 个AST节点")
            
            # 3. 语义分析
            semantic_errors = 0
            for ast in ast_list:
                try:
                    self.semantic_analyzer.analyze(ast)
                    print(f"[ADAPTER] 语义检查通过: {ast.node_type}")
                except Exception as e:
                    print(f"[ADAPTER] 语义检查失败: {e}")
                    semantic_errors += 1
            
            if semantic_errors > 0:
                raise SQLSyntaxError(f"语义分析失败，检测到 {semantic_errors} 个错误")
            
            # 4. 执行计划生成
            ast_list_dict = [ast.to_dict() for ast in ast_list]
            planner = Planner(ast_list_dict, enable_optimization=True)
            compiler_plans = planner.generate_plan()
            
            print(f"[ADAPTER] 编译器计划生成成功，生成 {len(compiler_plans)} 个计划")
            
            # 5. 转换计划格式并执行
            results = []
            for compiler_plan in compiler_plans:
                print(f"[ADAPTER] 编译器计划: {compiler_plan}")
                
                # 转换为执行器格式
                executor_plan = self._convert_plan_to_executor_format(compiler_plan)
                print(f"[ADAPTER] 转换后计划: {executor_plan}")
                
                # 执行计划
                result = self.hybrid_executor.execute(executor_plan)
                results.append(result)
            
            # 返回最后一个结果（通常是主要结果）
            if results:
                return results[-1]
            else:
                return {"status": "success", "affected_rows": 0, "data": []}
                
        except ParseError as e:
            print(f"[ADAPTER] 语法分析错误: {e}")
            raise SQLSyntaxError(f"语法分析错误: {e}")
        except Exception as e:
            print(f"[ADAPTER] 执行错误: {e}")
            raise ExecutionError(f"SQL执行错误: {e}")
    
    def flush(self):
        """刷盘所有脏页"""
        try:
            self.storage_engine.flush_all_dirty_pages()
            print("[ADAPTER] 数据刷盘完成")
        except Exception as e:
            print(f"[ADAPTER] 刷盘失败: {e}")
    
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
            print(f"[ADAPTER] 获取目录信息失败: {e}")
            return {}
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """获取缓存统计信息"""
        if self.hybrid_storage:
            return self.hybrid_storage.get_cache_stats()
        else:
            return {"message": "混合存储引擎未启用"}
    
    def flush_cache(self):
        """刷盘缓存"""
        if self.hybrid_storage:
            self.hybrid_storage.flush_all_dirty_pages()
        else:
            self.storage_engine.flush_all_dirty_pages()


def create_sql_compiler_adapter(use_hybrid_storage: bool = True, 
                               cache_capacity: int = 100, 
                               cache_strategy: str = "LRU") -> SQLCompilerAdapter:
    """创建SQL编译器适配器实例"""
    return SQLCompilerAdapter(
        use_hybrid_storage=use_hybrid_storage,
        cache_capacity=cache_capacity,
        cache_strategy=cache_strategy
    )


if __name__ == "__main__":
    # 测试SQL编译器适配器
    adapter = create_sql_compiler_adapter()
    
    # 测试基本功能
    print("=== SQL编译器适配器测试 ===")
    
    # 使用唯一表名
    import time
    timestamp = int(time.time())
    table_name = f"test_table_{timestamp}"
    
    # 创建表
    print(f"\n1. 创建表 {table_name}")
    result = adapter.execute(f"CREATE TABLE {table_name}(id INT, name STRING, age INT);")
    print("结果:", result)
    
    # 插入数据
    print("\n2. 插入数据")
    result = adapter.execute(f"INSERT INTO {table_name}(id, name, age) VALUES (1, 'Alice', 20);")
    print("结果:", result)
    
    # 查询数据
    print("\n3. 查询数据")
    result = adapter.execute(f"SELECT * FROM {table_name};")
    print("结果:", result)
    
    # 获取目录信息
    print("\n4. 目录信息")
    catalog_info = adapter.get_catalog_info()
    print("目录信息:", catalog_info)
    
    # 刷盘
    print("\n5. 刷盘")
    adapter.flush()
    
    print("\n=== 测试完成 ===")
