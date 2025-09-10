#!/usr/bin/env python3
"""
混合架构数据库系统 - 最终版本
完全独立的实现，不依赖复杂的模块结构
"""

import sys
import os
import re
import time
from typing import Any, Dict, List, Optional, Callable

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


class SimpleSQLParser:
    """简化SQL解析器，支持CREATE/INSERT/SELECT/DELETE语句"""

    def parse(self, sql: str) -> Dict[str, Any]:
        """解析SQL语句"""
        sql = sql.strip().rstrip(";")
        if not sql:
            raise Exception("空SQL语句")
            
        sql_upper = sql.upper()
        
        try:
            if sql_upper.startswith("CREATE TABLE"):
                return self._parse_create_table(sql)
            elif sql_upper.startswith("INSERT"):
                return self._parse_insert(sql)
            elif sql_upper.startswith("SELECT"):
                return self._parse_select(sql)
            elif sql_upper.startswith("UPDATE"):
                return self._parse_update(sql)
            elif sql_upper.startswith("DELETE"):
                return self._parse_delete(sql)
            else:
                raise Exception(f"不支持的SQL语句类型: {sql.split()[0]}")
        except Exception as e:
            if "不支持的SQL语句类型" in str(e):
                raise
            else:
                raise Exception(f"SQL解析错误: {str(e)}")

    def _parse_create_table(self, sql: str) -> Dict[str, Any]:
        """解析CREATE TABLE语句"""
        pattern = r"CREATE\s+TABLE\s+(\w+)\s*\((.*)\)"
        match = re.match(pattern, sql, re.IGNORECASE | re.DOTALL)
        
        if not match:
            raise Exception("CREATE TABLE语句格式错误")
        
        table_name = match.group(1)
        columns_def = match.group(2)
        
        columns = []
        col_parts = self._split_columns(columns_def)
        
        for col_def in col_parts:
            col_def = col_def.strip()
            if not col_def:
                continue
                
            parts = col_def.split()
            if len(parts) < 2:
                raise Exception(f"列定义格式错误: {col_def}")
            
            col_name = parts[0]
            col_type = parts[1].upper()
            is_primary_key = "PRIMARY" in col_def.upper()
            
            # 处理带长度的类型，如VARCHAR(20)
            if "(" in col_type:
                col_type = col_type.split("(")[0]
            
            if col_type not in ["INT", "STRING", "VARCHAR", "DOUBLE", "FLOAT", "TEXT"]:
                raise Exception(f"不支持的数据类型: {col_type}")
            
            if col_type in ["VARCHAR", "TEXT"]:
                col_type = "STRING"
            elif col_type == "FLOAT":
                col_type = "DOUBLE"
            
            columns.append({
                "name": col_name,
                "type": col_type,
                "is_primary_key": is_primary_key
            })

        if not columns:
            raise Exception("CREATE TABLE语句中缺少列定义")

        return {
            "type": "CREATE_TABLE",
            "table": table_name,
            "columns": columns
        }

    def _split_columns(self, columns_str: str) -> List[str]:
        """分割列定义字符串"""
        parts = []
        current = ""
        paren_count = 0
        
        for char in columns_str:
            if char == '(':
                paren_count += 1
                current += char
            elif char == ')':
                paren_count -= 1
                current += char
            elif char == ',' and paren_count == 0:
                parts.append(current.strip())
                current = ""
            else:
                current += char
        
        if current.strip():
            parts.append(current.strip())
        
        return parts

    def _parse_insert(self, sql: str) -> Dict[str, Any]:
        """解析INSERT语句，支持单条和批量插入"""
        # 支持单条插入: INSERT INTO table VALUES (val1, val2, ...)
        single_pattern = r"INSERT\s+INTO\s+(\w+)\s+VALUES\s*\((.*)\)"
        single_match = re.match(single_pattern, sql, re.IGNORECASE | re.DOTALL)
        
        if single_match:
            table_name = single_match.group(1)
            values_str = single_match.group(2)
            values = self._parse_value_list(values_str)
            
            if not values:
                raise Exception("INSERT语句中缺少VALUES")
            
            return {
                "type": "INSERT",
                "table": table_name,
                "values": [values]  # 包装成列表以支持批量
            }
        
        # 支持批量插入: INSERT INTO table VALUES (val1, val2), (val3, val4), ...
        batch_pattern = r"INSERT\s+INTO\s+(\w+)\s+VALUES\s*(.*)"
        batch_match = re.match(batch_pattern, sql, re.IGNORECASE | re.DOTALL)
        
        if batch_match:
            table_name = batch_match.group(1)
            values_str = batch_match.group(2)
            
            # 解析多个值组
            values_list = self._parse_multiple_value_lists(values_str)
            
            if not values_list:
                raise Exception("INSERT语句中缺少VALUES")
            
            return {
                "type": "INSERT",
                "table": table_name,
                "values": values_list
            }
        
        raise Exception("INSERT语句格式错误")

    def _parse_value_list(self, value_str: str) -> List[str]:
        """解析值列表"""
        values = []
        current_value = ""
        in_quotes = False
        quote_char = None
        
        i = 0
        while i < len(value_str):
            char = value_str[i]
            
            if char in ["'", '"'] and not in_quotes:
                in_quotes = True
                quote_char = char
                current_value += char
            elif char == quote_char and in_quotes:
                in_quotes = False
                quote_char = None
                current_value += char
            elif char == ',' and not in_quotes:
                values.append(current_value.strip())
                current_value = ""
            else:
                current_value += char
            i += 1
        
        if current_value.strip():
            values.append(current_value.strip())
        
        # 清理值（去除引号）
        cleaned_values = []
        for val in values:
            val = val.strip()
            if (val.startswith("'") and val.endswith("'")) or (val.startswith('"') and val.endswith('"')):
                val = val[1:-1]
            cleaned_values.append(val)
        
        return cleaned_values

    def _parse_multiple_value_lists(self, values_str: str) -> List[List[str]]:
        """解析多个值列表，支持批量插入"""
        values_list = []
        
        # 手动解析括号组
        i = 0
        while i < len(values_str):
            # 跳过空白字符
            while i < len(values_str) and values_str[i].isspace():
                i += 1
            
            if i >= len(values_str):
                break
                
            # 找到左括号
            if values_str[i] != '(':
                i += 1
                continue
            
            # 找到匹配的右括号
            start = i + 1  # 跳过左括号
            paren_count = 1
            in_quotes = False
            quote_char = None
            i += 1
            
            while i < len(values_str) and paren_count > 0:
                char = values_str[i]
                
                if char in ['"', "'"] and not in_quotes:
                    in_quotes = True
                    quote_char = char
                elif char == quote_char and in_quotes:
                    in_quotes = False
                    quote_char = None
                elif char == '(' and not in_quotes:
                    paren_count += 1
                elif char == ')' and not in_quotes:
                    paren_count -= 1
                
                i += 1
            
            if paren_count == 0:
                # 提取括号内的内容
                group_content = values_str[start:i-1].strip()
                if group_content:
                    values = self._parse_value_list(group_content)
                    values_list.append(values)
        
        return values_list

    def _parse_select(self, sql: str) -> Dict[str, Any]:
        """解析SELECT语句，支持复杂WHERE条件和ORDER BY"""
        # 支持ORDER BY子句
        order_by_pattern = r"SELECT\s+(.*?)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.*?))?(?:\s+ORDER\s+BY\s+(.*))?"
        order_match = re.match(order_by_pattern, sql, re.IGNORECASE | re.DOTALL)
        
        if order_match:
            columns_str = order_match.group(1).strip()
            table_name = order_match.group(2)
            where_clause = order_match.group(3)
            order_by_clause = order_match.group(4)
            
            if columns_str == "*":
                target_columns = ["*"]
            else:
                target_columns = [col.strip() for col in columns_str.split(",")]
            
            return {
                "type": "SELECT",
                "table": table_name,
                "columns": target_columns,
                "where": where_clause.strip() if where_clause else None,
                "order_by": order_by_clause.strip() if order_by_clause else None
            }
        
        # 基本SELECT语句（向后兼容）
        basic_pattern = r"SELECT\s+(.*?)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.*))?"
        basic_match = re.match(basic_pattern, sql, re.IGNORECASE | re.DOTALL)
        
        if basic_match:
            columns_str = basic_match.group(1).strip()
            table_name = basic_match.group(2)
            where_clause = basic_match.group(3)
            
            if columns_str == "*":
                target_columns = ["*"]
            else:
                target_columns = [col.strip() for col in columns_str.split(",")]
            
            return {
                "type": "SELECT",
                "table": table_name,
                "columns": target_columns,
                "where": where_clause.strip() if where_clause else None
            }
        
        raise Exception("SELECT语句格式错误")

    def _parse_delete(self, sql: str) -> Dict[str, Any]:
        """解析DELETE语句"""
        pattern = r"DELETE\s+FROM\s+(\w+)(?:\s+WHERE\s+(.*))?"
        match = re.match(pattern, sql, re.IGNORECASE | re.DOTALL)
        
        if not match:
            raise Exception("DELETE语句格式错误")
        
        table_name = match.group(1)
        where_clause = match.group(2)
        
        return {
            "type": "DELETE",
            "table": table_name,
            "where": where_clause.strip() if where_clause else None
        }

    def _parse_update(self, sql: str) -> Dict[str, Any]:
        """解析UPDATE语句"""
        # 支持格式: UPDATE table SET col1=val1, col2=val2 WHERE condition
        pattern = r"UPDATE\s+(\w+)\s+SET\s+(.*?)(?:\s+WHERE\s+(.*))?"
        match = re.match(pattern, sql, re.IGNORECASE | re.DOTALL)
        
        if not match:
            raise Exception("UPDATE语句格式错误")
        
        table_name = match.group(1)
        set_clause = match.group(2)
        where_clause = match.group(3)
        
        # 解析SET子句
        updates = self._parse_set_clause(set_clause)
        
        return {
            "type": "UPDATE",
            "table": table_name,
            "updates": updates,
            "where": where_clause.strip() if where_clause else None
        }

    def _parse_set_clause(self, set_clause: str) -> Dict[str, str]:
        """解析SET子句，返回列名到值的映射"""
        updates = {}
        
        # 分割多个赋值语句
        assignments = []
        current_assignment = ""
        paren_count = 0
        in_quotes = False
        quote_char = None
        
        for char in set_clause:
            if char in ['"', "'"] and not in_quotes:
                in_quotes = True
                quote_char = char
                current_assignment += char
            elif char == quote_char and in_quotes:
                in_quotes = False
                quote_char = None
                current_assignment += char
            elif char == ',' and not in_quotes and paren_count == 0:
                assignments.append(current_assignment.strip())
                current_assignment = ""
            else:
                if char == '(':
                    paren_count += 1
                elif char == ')':
                    paren_count -= 1
                current_assignment += char
        
        if current_assignment.strip():
            assignments.append(current_assignment.strip())
        
        # 解析每个赋值语句
        for assignment in assignments:
            if '=' not in assignment:
                raise Exception(f"无效的赋值语句: {assignment}")
            
            parts = assignment.split('=', 1)
            if len(parts) != 2:
                raise Exception(f"无效的赋值语句: {assignment}")
            
            column_name = parts[0].strip()
            value = parts[1].strip()
            
            # 清理值（去除引号）
            if (value.startswith("'") and value.endswith("'")) or (value.startswith('"') and value.endswith('"')):
                value = value[1:-1]
            
            updates[column_name] = value
        
        return updates


class HybridExecutionEngine:
    """混合架构执行引擎，Python调度C++算子"""

    def __init__(self, cpp_storage_engine, cpp_execution_engine):
        self.storage = cpp_storage_engine
        self.executor = cpp_execution_engine
        self.table_columns = {}

    def execute(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """执行查询计划"""
        start_time = time.time()
        
        try:
            plan_type = plan.get("type")
            
            if plan_type == "CREATE_TABLE":
                result = self._execute_create_table(plan)
            elif plan_type == "INSERT":
                result = self._execute_insert(plan)
            elif plan_type == "SELECT":
                result = self._execute_select(plan)
            elif plan_type == "UPDATE":
                result = self._execute_update(plan)
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
        
        success = self.executor.create_table(table_name, cpp_columns)
        
        if success:
            self.table_columns[table_name] = [col["name"] for col in columns_def]
            return {
                "affected_rows": 0,
                "metadata": {"message": f"表 '{table_name}' 创建成功"}
            }
        else:
            raise Exception(f"表 '{table_name}' 已存在")

    def _execute_insert(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """执行INSERT计划，支持批量插入"""
        table_name = plan["table"]
        values_list = plan["values"]  # 现在是一个列表，支持批量插入
        
        if table_name not in self.table_columns:
            raise Exception(f"表 '{table_name}' 不存在")
        
        expected_cols = len(self.table_columns[table_name])
        total_affected = 0
        failed_inserts = []
        
        # 处理批量插入
        for i, values in enumerate(values_list):
            if len(values) != expected_cols:
                failed_inserts.append(f"第{i+1}行: 列数不匹配，期望 {expected_cols} 列，实际 {len(values)} 列")
                continue
            
            success = self.executor.insert(table_name, values)
            if success:
                total_affected += 1
            else:
                failed_inserts.append(f"第{i+1}行: 插入失败（行数据过大或存储空间不足）")
        
        # 返回结果
        if failed_inserts:
            error_msg = f"批量插入部分失败: {'; '.join(failed_inserts)}"
            if total_affected > 0:
                return {
                    "affected_rows": total_affected,
                    "warnings": error_msg
                }
            else:
                raise Exception(error_msg)
        
        return {"affected_rows": total_affected}

    def _execute_select(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """执行SELECT计划，支持ORDER BY排序"""
        table_name = plan["table"]
        target_columns = plan.get("columns", ["*"])
        where_clause = plan.get("where")
        order_by_clause = plan.get("order_by")
        
        if table_name not in self.table_columns:
            raise Exception(f"表 '{table_name}' 不存在")
        
        if target_columns == ["*"]:
            target_columns = self.table_columns[table_name]
        
        predicate = self._build_predicate(table_name, where_clause)
        
        # 执行查询：SeqScan → Filter → Project
        scanned_rows = self.executor.seq_scan(table_name)
        filtered_rows = self.executor.filter(table_name, predicate)
        projected_data = self.executor.project(table_name, filtered_rows, target_columns)
        
        # 处理ORDER BY排序
        if order_by_clause:
            projected_data = self._apply_order_by(projected_data, order_by_clause, table_name)
        
        return {
            "data": projected_data,
            "affected_rows": len(projected_data),
            "metadata": {"columns": target_columns}
        }

    def _apply_order_by(self, data: List[List[str]], order_by_clause: str, table_name: str) -> List[List[str]]:
        """应用ORDER BY排序"""
        if not data:
            return data
        
        # 解析ORDER BY子句
        order_parts = order_by_clause.strip().split()
        if len(order_parts) < 2:
            raise Exception("ORDER BY子句格式错误")
        
        column_name = order_parts[0]
        direction = order_parts[1].upper() if len(order_parts) > 1 else "ASC"
        
        if direction not in ["ASC", "DESC"]:
            raise Exception("ORDER BY方向必须是ASC或DESC")
        
        # 获取列索引
        col_names = self.table_columns.get(table_name, [])
        try:
            col_index = col_names.index(column_name)
        except ValueError:
            raise Exception(f"ORDER BY列 '{column_name}' 不存在")
        
        # 排序数据
        def sort_key(row):
            value = row[col_index]
            # 尝试转换为数值进行排序
            try:
                if '.' in value:
                    return float(value)
                else:
                    return int(value)
            except ValueError:
                return value  # 保持字符串排序
        
        sorted_data = sorted(data, key=sort_key, reverse=(direction == "DESC"))
        return sorted_data

    def _execute_update(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """执行UPDATE计划"""
        table_name = plan["table"]
        updates = plan["updates"]
        where_clause = plan.get("where")
        
        if table_name not in self.table_columns:
            raise Exception(f"表 '{table_name}' 不存在")
        
        col_names = self.table_columns[table_name]
        
        # 验证更新的列是否存在
        for col_name in updates.keys():
            if col_name not in col_names:
                raise Exception(f"列 '{col_name}' 不存在")
        
        # 构建WHERE条件
        predicate = self._build_predicate(table_name, where_clause)
        
        # 获取所有行
        all_rows = self.executor.seq_scan(table_name)
        updated_count = 0
        
        # 更新符合条件的行
        for row in all_rows:
            if predicate(row.get_values()):
                # 创建新的行数据
                new_values = list(row.get_values())
                
                # 应用更新
                for col_name, new_value in updates.items():
                    col_index = col_names.index(col_name)
                    new_values[col_index] = new_value
                
                # 删除旧行（逻辑删除）
                row.mark_deleted()
                
                # 插入新行
                success = self.executor.insert(table_name, new_values)
                if success:
                    updated_count += 1
        
        return {
            "affected_rows": updated_count,
            "metadata": {"message": f"更新了 {updated_count} 行"}
        }

    def _execute_delete(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """执行DELETE计划"""
        table_name = plan["table"]
        where_clause = plan.get("where")
        
        if table_name not in self.table_columns:
            raise Exception(f"表 '{table_name}' 不存在")
        
        predicate = self._build_predicate(table_name, where_clause)
        deleted_count = self.executor.delete_rows(table_name, predicate)
        
        return {
            "affected_rows": deleted_count,
            "metadata": {"message": f"删除了 {deleted_count} 行"}
        }

    def _build_predicate(self, table_name: str, where_clause: Optional[str]) -> Callable[[List[str]], bool]:
        """将WHERE条件字符串转为C++可调用的过滤函数，支持复杂条件"""
        if not where_clause:
            return lambda x: True

        col_names = self.table_columns.get(table_name, [])
        
        # 预处理WHERE子句，支持AND、OR、NOT等逻辑操作符
        processed_clause = self._preprocess_where_clause(where_clause, col_names)
        
        try:
            predicate = eval(f"lambda x: {processed_clause}")
            return predicate
        except Exception as e:
            raise Exception(f"WHERE条件解析错误: {str(e)}")

    def _preprocess_where_clause(self, where_clause: str, col_names: List[str]) -> str:
        """预处理WHERE子句，替换列名为索引引用"""
        processed_clause = where_clause
        
        # 替换列名为索引引用
        for col_idx, col_name in enumerate(col_names):
            # 处理各种列名出现的情况
            patterns = [
                f"\\b{col_name}\\b",  # 单词边界
                f"'{col_name}'",     # 单引号包围
                f'"{col_name}"',     # 双引号包围
            ]
            
            for pattern in patterns:
                processed_clause = re.sub(pattern, f"x[{col_idx}]", processed_clause)
        
        # 处理字符串比较（添加引号）
        processed_clause = self._process_string_comparisons(processed_clause)
        
        # 处理数值比较
        processed_clause = self._process_numeric_comparisons(processed_clause)
        
        return processed_clause

    def _process_string_comparisons(self, clause: str) -> str:
        """处理字符串比较，确保字符串值被正确引用"""
        # 匹配 = 'value' 或 = "value" 模式
        string_pattern = r"=\s*([^=<>!'\"]+)(?=\s*(?:AND|OR|$))"
        
        def replace_string(match):
            value = match.group(1).strip()
            # 如果值不是数字且没有被引号包围，则添加单引号
            if not (value.startswith("'") and value.endswith("'")) and \
               not (value.startswith('"') and value.endswith('"')) and \
               not value.replace('.', '').replace('-', '').isdigit():
                return f"= '{value}'"
            return match.group(0)
        
        return re.sub(string_pattern, replace_string, clause)

    def _process_numeric_comparisons(self, clause: str) -> str:
        """处理数值比较，确保数值被正确转换"""
        # 这里可以添加更复杂的数值处理逻辑
        # 目前保持简单实现
        return clause

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


class HybridDatabaseEngine:
    """混合架构数据库引擎：Python上层+C++核心"""

    def __init__(self):
        """初始化混合数据库引擎"""
        try:
            from db_core import StorageEngine, ExecutionEngine
            
            self.storage = StorageEngine()
            self.cpp_executor = ExecutionEngine(self.storage)
            self.parser = SimpleSQLParser()
            self.executor = HybridExecutionEngine(self.storage, self.cpp_executor)
            
        except ImportError as e:
            raise Exception(f"无法导入C++核心模块: {str(e)}。请先编译C++模块。")

    def execute(self, sql: str) -> Dict[str, Any]:
        """执行SQL语句"""
        try:
            ast = self.parser.parse(sql)
            if not ast:
                raise Exception("SQL解析失败")
            
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


class HybridCLI:
    """混合架构数据库命令行界面"""

    def __init__(self):
        """初始化CLI"""
        try:
            self.engine = HybridDatabaseEngine()
            print("=== 混合架构数据库系统 (Python-C++ Hybrid) ===")
            print("支持的命令: CREATE TABLE, INSERT, SELECT, DELETE")
            print("输入 'exit' 退出, 'help' 查看帮助\n")
        except Exception as e:
            print(f"数据库初始化失败: {str(e)}")
            sys.exit(1)

    def start(self):
        """启动命令行交互"""
        while True:
            try:
                sql = input("db> ").strip()
                
                if not sql:
                    continue
                    
                if sql.lower() == "exit":
                    self.engine.close()
                    print("再见!")
                    break
                    
                if sql.lower() == "help":
                    self._show_help()
                    continue
                    
                if sql.lower() == "tables":
                    self._show_tables()
                    continue
                    
                result = self.engine.execute(sql)
                self._display_result(result)
                
            except KeyboardInterrupt:
                print("\n\n再见!")
                self.engine.close()
                break
            except Exception as e:
                print(f"错误: {str(e)}")

    def _display_result(self, result: Dict[str, Any]):
        """显示查询结果"""
        if result["status"] != "success":
            print(f"执行失败: {result.get('error', '未知错误')}")
            return
            
        data = result.get("data", [])
        metadata = result.get("metadata", {})
        affected_rows = result.get("affected_rows", 0)
        execution_time = result.get("execution_time", 0)
        
        if isinstance(data, list) and data:
            columns = metadata.get("columns", [])
            if columns:
                self._print_table(columns, data)
                print(f"共 {len(data)} 行")
        else:
            message = metadata.get("message", f"影响 {affected_rows} 行")
            print(f"✓ {message}")
            
        if execution_time > 0:
            print(f"执行时间: {execution_time:.4f}秒")

    def _print_table(self, columns: List[str], data: List[List[str]]):
        """打印表格"""
        if not data:
            print("(无数据)")
            return
            
        col_widths = []
        for i, col in enumerate(columns):
            max_width = len(str(col))
            for row in data:
                if i < len(row):
                    max_width = max(max_width, len(str(row[i])))
            col_widths.append(max_width)
        
        total_width = sum(col_widths) + len(col_widths) * 3 + 1
        print("-" * total_width)
        
        header = " | ".join(f"{str(columns[i]).ljust(col_widths[i])}" for i in range(len(columns)))
        print(f"| {header} |")
        print("-" * total_width)
        
        for row in data:
            row_data = []
            for i in range(len(columns)):
                if i < len(row):
                    row_data.append(f"{str(row[i]).ljust(col_widths[i])}")
                else:
                    row_data.append("".ljust(col_widths[i]))
            
            data_str = " | ".join(row_data)
            print(f"| {data_str} |")
        
        print("-" * total_width)

    def _show_help(self):
        """显示帮助信息"""
        help_text = """
可用命令:
  CREATE TABLE table_name (col1 type1, col2 type2, ...)  - 创建表
  INSERT INTO table_name VALUES (val1, val2, ...)        - 插入数据
  SELECT col1, col2 FROM table_name [WHERE condition]    - 查询数据
  DELETE FROM table_name [WHERE condition]               - 删除数据
  tables                                                 - 显示所有表
  help                                                   - 显示此帮助
  exit                                                   - 退出程序

支持的数据类型:
  INT     - 整数
  STRING  - 字符串
  DOUBLE  - 浮点数

WHERE条件示例:
  age > 18
  name = 'Alice'
  score >= 90.0
        """
        print(help_text)

    def _show_tables(self):
        """显示所有表"""
        try:
            tables = self.engine.get_tables()
            if tables:
                print("数据库中的表:")
                for table in tables:
                    print(f"  - {table}")
            else:
                print("数据库中没有表")
        except Exception as e:
            print(f"获取表列表失败: {str(e)}")


def main():
    """主函数"""
    cli = HybridCLI()
    cli.start()


if __name__ == "__main__":
    main()
