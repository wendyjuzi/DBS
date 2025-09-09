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
        """解析INSERT语句"""
        pattern = r"INSERT\s+INTO\s+(\w+)\s+VALUES\s*\((.*)\)"
        match = re.match(pattern, sql, re.IGNORECASE | re.DOTALL)
        
        if not match:
            raise Exception("INSERT语句格式错误")
        
        table_name = match.group(1)
        values_str = match.group(2)
        values = self._parse_value_list(values_str)
        
        if not values:
            raise Exception("INSERT语句中缺少VALUES")

        return {
            "type": "INSERT",
            "table": table_name,
            "values": values
        }

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

    def _parse_select(self, sql: str) -> Dict[str, Any]:
        """解析SELECT语句"""
        pattern = r"SELECT\s+(.*?)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.*))?"
        match = re.match(pattern, sql, re.IGNORECASE | re.DOTALL)
        
        if not match:
            raise Exception("SELECT语句格式错误")
        
        columns_str = match.group(1).strip()
        table_name = match.group(2)
        where_clause = match.group(3)
        
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
        """执行INSERT计划"""
        table_name = plan["table"]
        values = plan["values"]
        
        if table_name not in self.table_columns:
            raise Exception(f"表 '{table_name}' 不存在")
        
        expected_cols = len(self.table_columns[table_name])
        if len(values) != expected_cols:
            raise Exception(f"列数不匹配，期望 {expected_cols} 列，实际 {len(values)} 列")
        
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
        
        if table_name not in self.table_columns:
            raise Exception(f"表 '{table_name}' 不存在")
        
        if target_columns == ["*"]:
            target_columns = self.table_columns[table_name]
        
        predicate = self._build_predicate(table_name, where_clause)
        
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
        
        if table_name not in self.table_columns:
            raise Exception(f"表 '{table_name}' 不存在")
        
        predicate = self._build_predicate(table_name, where_clause)
        deleted_count = self.executor.delete_rows(table_name, predicate)
        
        return {
            "affected_rows": deleted_count,
            "metadata": {"message": f"删除了 {deleted_count} 行"}
        }

    def _build_predicate(self, table_name: str, where_clause: Optional[str]) -> Callable[[List[str]], bool]:
        """将WHERE条件字符串转为C++可调用的过滤函数"""
        if not where_clause:
            return lambda x: True

        col_names = self.table_columns.get(table_name, [])
        processed_clause = where_clause
        
        for col_idx, col_name in enumerate(col_names):
            if col_name in processed_clause:
                processed_clause = processed_clause.replace(f"{col_name} ", f"x[{col_idx}] ")
                processed_clause = processed_clause.replace(f" {col_name}", f" x[{col_idx}]")
                processed_clause = processed_clause.replace(f"'{col_name}'", f"x[{col_idx}]")

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
