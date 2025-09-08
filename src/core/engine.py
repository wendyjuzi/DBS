"""
数据库核心引擎
"""

from typing import Any, Dict, List, Optional
from abc import ABC, abstractmethod

from .parser.sql_parser import SQLParser
from .catalog.system_catalog import SystemCatalog
from .executor.query_executor import QueryExecutor
from .planner.query_planner import QueryPlanner
from .optimizer.query_optimizer import QueryOptimizer
from ..storage.engine import StorageEngine
from ..utils.exceptions import DatabaseError, SQLSyntaxError, ExecutionError


class DatabaseEngine:
    """数据库核心引擎"""
    
    def __init__(self, storage_engine: StorageEngine):
        """
        初始化数据库引擎
        
        Args:
            storage_engine: 存储引擎实例
        """
        self.storage_engine = storage_engine
        self.parser = SQLParser()
        self.catalog = SystemCatalog()
        self.planner = QueryPlanner()
        self.optimizer = QueryOptimizer()
        self.executor = QueryExecutor(storage_engine, self.catalog)
        
        # 初始化系统目录
        self._initialize_system_catalog()
    
    def _initialize_system_catalog(self):
        """初始化系统目录"""
        # 创建系统表（如information_schema等）
        pass
    
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
            plan = self.planner.generate_plan(ast, self.catalog)
            
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
            if isinstance(e, (SQLSyntaxError, DatabaseError)):
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
        return self.catalog.get_tables()
    
    def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        """获取表结构"""
        return self.catalog.get_table_schema(table_name)
    
    def create_database(self, db_name: str) -> bool:
        """创建数据库"""
        try:
            # 在系统目录中记录新数据库
            self.catalog.create_database(db_name)
            return True
        except Exception as e:
            raise DatabaseError(f"创建数据库失败: {str(e)}")
    
    def drop_database(self, db_name: str) -> bool:
        """删除数据库"""
        try:
            self.catalog.drop_database(db_name)
            return True
        except Exception as e:
            raise DatabaseError(f"删除数据库失败: {str(e)}")
    
    def close(self):
        """关闭数据库引擎"""
        if hasattr(self.storage_engine, 'close'):
            self.storage_engine.close()
