"""
简化SQL解析器 - 简化版本
"""

import re
from typing import Any, Dict, List, Optional, Tuple


class SimpleSQLParser:
    """简化SQL解析器，支持CREATE/INSERT/SELECT/DELETE语句"""

    def parse(self, sql: str) -> Dict[str, Any]:
        """
        解析SQL语句
        
        Args:
            sql: SQL语句字符串
            
        Returns:
            解析后的AST字典
        """
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
        # 使用正则表达式匹配CREATE TABLE语句
        pattern = r"CREATE\s+TABLE\s+(\w+)\s*\((.*)\)"
        match = re.match(pattern, sql, re.IGNORECASE | re.DOTALL)
        
        if not match:
            raise Exception("CREATE TABLE语句格式错误")
        
        table_name = match.group(1)
        columns_def = match.group(2)
        
        # 解析列定义
        columns = []
        # 简单的列解析：按逗号分割，但要注意括号内的逗号
        col_parts = self._split_columns(columns_def)
        
        for col_def in col_parts:
            col_def = col_def.strip()
            if not col_def:
                continue
                
            # 解析列定义：'id INT PRIMARY KEY' -> {name, type, is_primary_key}
            parts = col_def.split()
            if len(parts) < 2:
                raise Exception(f"列定义格式错误: {col_def}")
            
            col_name = parts[0]
            col_type = parts[1].upper()
            is_primary_key = "PRIMARY" in col_def.upper()
            
            # 验证类型
            if col_type not in ["INT", "STRING", "VARCHAR", "DOUBLE", "FLOAT"]:
                raise Exception(f"不支持的数据类型: {col_type}")
            
            # 标准化类型名
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
        """分割列定义字符串，处理括号内的逗号"""
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
        # 使用正则表达式匹配INSERT语句
        pattern = r"INSERT\s+INTO\s+(\w+)\s+VALUES\s*\((.*)\)"
        match = re.match(pattern, sql, re.IGNORECASE | re.DOTALL)
        
        if not match:
            raise Exception("INSERT语句格式错误")
        
        table_name = match.group(1)
        values_str = match.group(2)
        
        # 解析值列表
        values = self._parse_value_list(values_str)
        
        if not values:
            raise Exception("INSERT语句中缺少VALUES")

        return {
            "type": "INSERT",
            "table": table_name,
            "values": values
        }

    def _parse_value_list(self, value_str: str) -> List[str]:
        """解析值列表：'1, 'Alice', 20.5' -> ['1', 'Alice', '20.5']"""
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
        # 使用正则表达式匹配SELECT语句
        pattern = r"SELECT\s+(.*?)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.*))?"
        match = re.match(pattern, sql, re.IGNORECASE | re.DOTALL)
        
        if not match:
            raise Exception("SELECT语句格式错误")
        
        columns_str = match.group(1).strip()
        table_name = match.group(2)
        where_clause = match.group(3)
        
        # 解析目标列
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
        # 使用正则表达式匹配DELETE语句
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
