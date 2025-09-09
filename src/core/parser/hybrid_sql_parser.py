"""
混合架构SQL解析器 - 使用sqlparse库进行SQL解析
"""

import sqlparse
from sqlparse.tokens import Keyword, DML, DDL, Literal
from typing import Any, Dict, List, Optional, Tuple
from ...utils.exceptions import SQLSyntaxError


class HybridSQLParser:
    """混合架构SQL解析器，支持CREATE/INSERT/SELECT/DELETE语句"""

    def parse(self, sql: str) -> Dict[str, Any]:
        """
        解析SQL语句
        
        Args:
            sql: SQL语句字符串
            
        Returns:
            解析后的AST字典
            
        Raises:
            SQLSyntaxError: SQL语法错误
        """
        sql = sql.strip().rstrip(";")
        if not sql:
            raise SQLSyntaxError("空SQL语句")
            
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
                raise SQLSyntaxError(f"不支持的SQL语句类型: {sql.split()[0]}")
        except Exception as e:
            if isinstance(e, SQLSyntaxError):
                raise
            else:
                raise SQLSyntaxError(f"SQL解析错误: {str(e)}")

    def _parse_create_table(self, sql: str) -> Dict[str, Any]:
        """解析CREATE TABLE语句"""
        parsed = sqlparse.parse(sql)[0]
        table_name = None
        columns = []

        # 提取表名（第一个标识符）
        for token in parsed.tokens:
            if token.ttype is None and token.value.strip() and not token.value.strip().upper() in ['CREATE', 'TABLE', '(', ')']:
                table_name = token.value.strip()
                break

        if not table_name:
            raise SQLSyntaxError("CREATE TABLE语句中缺少表名")

        # 提取列定义（括号内的内容）
        in_columns = False
        current_column = ""
        
        for token in parsed.tokens:
            if token.value == '(':
                in_columns = True
                continue
            if token.value == ')':
                in_columns = False
                if current_column.strip():
                    columns.append(self._parse_column_definition(current_column.strip()))
                break
            if in_columns:
                current_column += token.value

        if not columns:
            raise SQLSyntaxError("CREATE TABLE语句中缺少列定义")

        return {
            "type": "CREATE_TABLE",
            "table": table_name,
            "columns": columns
        }

    def _parse_column_definition(self, col_def: str) -> Dict[str, Any]:
        """解析列定义：'id INT PRIMARY KEY' -> {name, type, is_primary_key}"""
        parts = col_def.strip().split()
        if len(parts) < 2:
            raise SQLSyntaxError(f"列定义格式错误: {col_def}")
        
        col_name = parts[0]
        col_type = parts[1].upper()
        is_primary_key = "PRIMARY" in col_def.upper()
        
        # 验证类型
        if col_type not in ["INT", "STRING", "VARCHAR", "DOUBLE", "FLOAT"]:
            raise SQLSyntaxError(f"不支持的数据类型: {col_type}")
        
        # 标准化类型名
        if col_type in ["VARCHAR", "TEXT"]:
            col_type = "STRING"
        elif col_type == "FLOAT":
            col_type = "DOUBLE"
        
        return {
            "name": col_name,
            "type": col_type,
            "is_primary_key": is_primary_key
        }

    def _parse_insert(self, sql: str) -> Dict[str, Any]:
        """解析INSERT语句"""
        parsed = sqlparse.parse(sql)[0]
        table_name = None
        values = []

        # 提取表名（INSERT INTO 后的Identifier）
        tokens = list(parsed.tokens)
        for i in range(len(tokens)):
            if tokens[i].value.upper() == "INTO" and i + 1 < len(tokens):
                if tokens[i+1].ttype == Identifier:
                    table_name = tokens[i+1].value.strip()
                    break

        if not table_name:
            raise SQLSyntaxError("INSERT语句中缺少表名")

        # 提取VALUES（括号内的字符串/数字）
        in_values = False
        current_value = ""
        paren_count = 0
        
        for token in parsed.tokens:
            if token.value.upper() == "VALUES":
                in_values = True
                continue
            if in_values:
                if token.value == '(':
                    paren_count += 1
                    if paren_count == 1:
                        current_value = ""
                        continue
                elif token.value == ')':
                    paren_count -= 1
                    if paren_count == 0:
                        # 解析当前值列表
                        if current_value.strip():
                            values.append(self._parse_value_list(current_value.strip()))
                        in_values = False
                        continue
                if paren_count > 0:
                    current_value += token.value

        if not values:
            raise SQLSyntaxError("INSERT语句中缺少VALUES")

        return {
            "type": "INSERT",
            "table": table_name,
            "values": values[0] if values else []  # 暂支持单条INSERT
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
        parsed = sqlparse.parse(sql)[0]
        table_name = None
        target_columns = []
        where_clause = None

        # 1. 提取目标列（SELECT后的内容）
        in_select = False
        for token in parsed.tokens:
            if token.ttype == DML and token.value.upper() == "SELECT":
                in_select = True
                continue
            if in_select:
                if token.value.upper() == "FROM":
                    in_select = False
                    break
                if token.ttype == Identifier or token.value == '*':
                    col = token.value.strip()
                    if col:
                        target_columns.append(col)

        # 2. 提取表名（FROM后的Identifier）
        in_from = False
        for token in parsed.tokens:
            if token.value.upper() == "FROM":
                in_from = True
                continue
            if in_from and token.ttype == Identifier:
                table_name = token.value.strip()
                in_from = False
                break

        if not table_name:
            raise SQLSyntaxError("SELECT语句中缺少FROM子句")

        # 3. 提取WHERE条件（WHERE后的内容）
        in_where = False
        where_parts = []
        for token in parsed.tokens:
            if token.value.upper() == "WHERE":
                in_where = True
                continue
            if in_where:
                if token.ttype in (Keyword, Identifier, Literal, sqlparse.tokens.Operator):
                    where_parts.append(token.value.strip())
        where_clause = " ".join(where_parts) if where_parts else None

        return {
            "type": "SELECT",
            "table": table_name,
            "columns": target_columns,
            "where": where_clause
        }

    def _parse_delete(self, sql: str) -> Dict[str, Any]:
        """解析DELETE语句"""
        parsed = sqlparse.parse(sql)[0]
        table_name = None
        where_clause = None

        # 1. 提取表名（DELETE FROM后的Identifier）
        in_from = False
        for token in parsed.tokens:
            if token.value.upper() == "FROM":
                in_from = True
                continue
            if in_from and token.ttype == Identifier:
                table_name = token.value.strip()
                in_from = False
                break

        if not table_name:
            raise SQLSyntaxError("DELETE语句中缺少FROM子句")

        # 2. 提取WHERE条件（同SELECT）
        in_where = False
        where_parts = []
        for token in parsed.tokens:
            if token.value.upper() == "WHERE":
                in_where = True
                continue
            if in_where:
                if token.ttype in (Keyword, Identifier, Literal, sqlparse.tokens.Operator):
                    where_parts.append(token.value.strip())
        where_clause = " ".join(where_parts) if where_parts else None

        return {
            "type": "DELETE",
            "table": table_name,
            "where": where_clause
        }
