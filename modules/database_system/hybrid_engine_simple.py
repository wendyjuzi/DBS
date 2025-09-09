"""
混合架构数据库引擎 - 简化版本
"""

from typing import Any, Dict, List
from .parser.simple_sql_parser_simple import SimpleSQLParser
from .executor.hybrid_executor_simple import HybridExecutionEngine


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
            self.executor = HybridExecutionEngine(self.storage, self.cpp_executor)
            
        except ImportError as e:
            raise Exception(f"无法导入C++核心模块: {str(e)}。请先编译C++模块。")

    def execute(self, sql: str) -> Dict[str, Any]:
        """
        执行SQL语句
        
        Args:
            sql: SQL语句字符串
            
        Returns:
            执行结果字典，包含状态、数据和元数据
        """
        try:
            # 1. 解析SQL
            ast = self.parser.parse(sql)
            if not ast:
                raise Exception("SQL解析失败")
            
            # 2. 执行
            result = self.executor.execute(ast)
            
            return {
                "status": "success",
                "data": result.get("data", []),
                "metadata": result.get("metadata", {}),
                "affected_rows": result.get("affected_rows", 0),
                "execution_time": result.get("execution_time", 0)
            }
            
        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "data": [],
                "metadata": {},
                "affected_rows": 0,
                "execution_time": 0
            }
    
    def get_tables(self) -> List[str]:
        """获取所有表名"""
        return self.executor.get_tables()
    
    def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        """获取表结构"""
        return self.executor.get_table_schema(table_name)

    def close(self):
        """关闭数据库连接，刷盘所有脏页"""
        self.executor.flush_all_dirty_pages()
