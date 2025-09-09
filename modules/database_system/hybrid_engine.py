"""
混合架构数据库引擎 - Python集成C++核心
"""

from typing import Any, Dict, List
from .parser.simple_sql_parser import SimpleSQLParser
from .executor.hybrid_executor import HybridExecutionEngine
from ..sql_compiler.planner.query_planner import QueryPlanner
from ..sql_compiler.planner.query_optimizer import QueryOptimizer
from ...src.utils.exceptions import DatabaseError, SQLSyntaxError, ExecutionError


class HybridDatabaseEngine:
    """混合架构数据库引擎：Python上层+C++核心"""

    def __init__(self):
        """初始化混合数据库引擎"""
        try:
            # 导入C++模块
            from db_core import StorageEngine, ExecutionEngine
            
            # 初始化C++引擎
            self.storage = StorageEngine()
            self.cpp_executor = ExecutionEngine(self.storage)
            
            # 初始化Python组件
            self.parser = SimpleSQLParser()
            self.planner = QueryPlanner()
            self.optimizer = QueryOptimizer()
            self.executor = HybridExecutionEngine(self.storage, self.cpp_executor)
            
        except ImportError as e:
            raise DatabaseError(f"无法导入C++核心模块: {str(e)}。请先编译C++模块。")

    def execute(self, sql: str) -> Dict[str, Any]:
        """
        执行SQL语句
        
        Args:
            sql: SQL语句字符串
            
        Returns:
            执行结果字典，包含状态、数据和元数据
            
        Raises:
            SQLSyntaxError: SQL语法错误
            DatabaseError: 数据库执行错误
        """
        try:
            # 1. 解析SQL
            ast = self.parser.parse(sql)
            if not ast:
                raise SQLSyntaxError("SQL解析失败")
            
            # 2. 语义分析
            self._semantic_analysis(ast)
            
            # 3. 生成执行计划
            plan = self.planner.generate_plan(ast, self.executor)
            
            # 4. 优化执行计划
            optimized_plan = self.optimizer.optimize(plan)
            
            # 5. 执行
            result = self.executor.execute(optimized_plan)
            
            return {
                "status": "success",
                "data": result.get("data", []),
                "metadata": result.get("metadata", {}),
                "affected_rows": result.get("affected_rows", 0),
                "execution_time": result.get("execution_time", 0)
            }
            
        except Exception as e:
            if isinstance(e, (SQLSyntaxError, ExecutionError, DatabaseError)):
                raise
            else:
                raise DatabaseError(f"执行SQL时发生错误: {str(e)}")
    
    def _semantic_analysis(self, ast: Dict[str, Any]):
        """
        语义分析
        
        Args:
            ast: 解析后的抽象语法树
        """
        # 检查表是否存在
        # 检查列是否存在
        # 检查权限
        # 检查数据类型兼容性
        pass
    
    def get_tables(self) -> List[str]:
        """获取所有表名"""
        return self.executor.get_tables()
    
    def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        """获取表结构"""
        return self.executor.get_table_schema(table_name)

    def close(self):
        """关闭数据库连接，刷盘所有脏页"""
        self.executor.flush_all_dirty_pages()