"""
混合架构执行引擎 - 简化版本
"""

import time
from typing import Any, Dict, List, Optional, Callable


class HybridExecutionEngine:
    """混合架构执行引擎，Python调度C++算子"""

    def __init__(self, cpp_storage_engine, cpp_execution_engine):
        """
        初始化混合执行引擎
        
        Args:
            cpp_storage_engine: C++存储引擎实例
            cpp_execution_engine: C++执行引擎实例
        """
        self.storage = cpp_storage_engine
        self.executor = cpp_execution_engine
        # 元数据缓存（表名→列名列表，避免重复调用C++）
        self.table_columns = {}

    def execute(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """
        执行查询计划
        
        Args:
            plan: 查询计划字典
            
        Returns:
            执行结果字典
        """
        start_time = time.time()
        
        try:
            plan_type = plan.get("type")
            
            if plan_type == "CREATE_TABLE":
                result = self._execute_create_table(plan)
            elif plan_type == "INSERT":
                result = self._execute_insert(plan)
            elif plan_type == "SELECT":
                result = self._execute_select(plan)
            elif plan_type == "DELETE":
                result = self._execute_delete(plan)
            else:
                raise Exception(f"不支持的查询计划类型: {plan_type}")
            
            execution_time = time.time() - start_time
            result["execution_time"] = execution_time
            
            return result
            
        except Exception as e:
            raise Exception(f"执行查询时发生错误: {str(e)}")

    def _execute_create_table(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """执行CREATE TABLE计划"""
        table_name = plan["table"]
        columns_def = plan["columns"]
        
        # 转换列定义为C++格式
        cpp_columns = []
        for col_def in columns_def:
            from db_core import Column, DataType
            
            # 映射数据类型
            if col_def["type"] == "INT":
                col_type = DataType.INT
            elif col_def["type"] == "STRING":
                col_type = DataType.STRING
            elif col_def["type"] == "DOUBLE":
                col_type = DataType.DOUBLE
            else:
                raise Exception(f"不支持的数据类型: {col_def['type']}")
            
            cpp_columns.append(Column(
                col_def["name"],
                col_type,
                col_def.get("is_primary_key", False)
            ))
        
        # 调用C++执行引擎
        success = self.executor.create_table(table_name, cpp_columns)
        
        if success:
            # 更新元数据缓存
            self.table_columns[table_name] = [col["name"] for col in columns_def]
            return {
                "affected_rows": 0,
                "metadata": {"message": f"表 '{table_name}' 创建成功"}
            }
        else:
            raise Exception(f"表 '{table_name}' 已存在")

    def _execute_insert(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """执行INSERT计划"""
        table_name = plan["table"]
        values = plan["values"]
        
        # 校验表是否存在
        if table_name not in self.table_columns:
            raise Exception(f"表 '{table_name}' 不存在")
        
        # 校验列数匹配
        expected_cols = len(self.table_columns[table_name])
        if len(values) != expected_cols:
            raise Exception(f"列数不匹配，期望 {expected_cols} 列，实际 {len(values)} 列")
        
        # 调用C++执行引擎
        success = self.executor.insert(table_name, values)
        
        if success:
            return {"affected_rows": 1}
        else:
            raise Exception("插入失败（行数据过大或存储空间不足）")

    def _execute_select(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """执行SELECT计划"""
        table_name = plan["table"]
        target_columns = plan.get("columns", ["*"])
        where_clause = plan.get("where")
        
        # 校验表是否存在
        if table_name not in self.table_columns:
            raise Exception(f"表 '{table_name}' 不存在")
        
        # 处理目标列（* → 所有列）
        if target_columns == ["*"]:
            target_columns = self.table_columns[table_name]
        
        # 构建过滤函数
        predicate = self._build_predicate(table_name, where_clause)
        
        # 调用C++算子：SeqScan → Filter → Project
        scanned_rows = self.executor.seq_scan(table_name)
        filtered_rows = self.executor.filter(table_name, predicate)
        projected_data = self.executor.project(table_name, filtered_rows, target_columns)
        
        return {
            "data": projected_data,
            "affected_rows": len(projected_data),
            "metadata": {"columns": target_columns}
        }

    def _execute_delete(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """执行DELETE计划"""
        table_name = plan["table"]
        where_clause = plan.get("where")
        
        # 校验表是否存在
        if table_name not in self.table_columns:
            raise Exception(f"表 '{table_name}' 不存在")
        
        # 构建过滤函数
        predicate = self._build_predicate(table_name, where_clause)
        
        # 调用C++执行引擎
        deleted_count = self.executor.delete_rows(table_name, predicate)
        
        return {
            "affected_rows": deleted_count,
            "metadata": {"message": f"删除了 {deleted_count} 行"}
        }

    def _build_predicate(self, table_name: str, where_clause: Optional[str]) -> Callable[[List[str]], bool]:
        """
        将WHERE条件字符串转为C++可调用的过滤函数
        
        Args:
            table_name: 表名
            where_clause: WHERE条件字符串
            
        Returns:
            过滤函数
        """
        if not where_clause:
            return lambda x: True  # 无WHERE条件，返回所有行

        # 获取表的列信息
        col_names = self.table_columns.get(table_name, [])
        
        # 替换列名为Row.values的下标（如"age > 18" → "x[1] > 18"）
        processed_clause = where_clause
        for col_idx, col_name in enumerate(col_names):
            if col_name in processed_clause:
                # 避免替换子字符串（如"age" vs "age1"）
                processed_clause = processed_clause.replace(f"{col_name} ", f"x[{col_idx}] ")
                processed_clause = processed_clause.replace(f" {col_name}", f" x[{col_idx}]")
                processed_clause = processed_clause.replace(f"'{col_name}'", f"x[{col_idx}]")  # 处理字符串比较

        # 构造lambda函数（注意：仅支持简单条件，如col OP val）
        try:
            predicate = eval(f"lambda x: {processed_clause}")
            return predicate
        except Exception as e:
            raise Exception(f"WHERE条件解析错误: {str(e)}")

    def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        """获取表结构"""
        if table_name not in self.table_columns:
            raise Exception(f"表 '{table_name}' 不存在")
        
        return {
            "name": table_name,
            "columns": self.table_columns[table_name]
        }

    def get_tables(self) -> List[str]:
        """获取所有表名"""
        return list(self.table_columns.keys())

    def flush_all_dirty_pages(self):
        """刷盘所有脏页"""
        self.storage.flush_all_dirty_pages()
