"""
SQL解析器
"""

from __future__ import annotations

import re
from typing import Any, Dict, List

from ...utils.exceptions import SQLSyntaxError


class SQLParser:
    """极简解析器，支持 CREATE TABLE / INSERT / SELECT / DELETE。"""

    _re_space = re.compile(r"\s+")

    def parse(self, sql: str) -> Dict[str, Any]:
        s = sql.strip().rstrip(";")
        if not s:
            raise SQLSyntaxError("空SQL")
        head = s.split(None, 1)[0].upper()
        if head == "CREATE":
            return self._parse_create_table(s)
        if head == "INSERT":
            return self._parse_insert(s)
        if head == "SELECT":
            return self._parse_select(s)
        if head == "DELETE":
            return self._parse_delete(s)
        raise SQLSyntaxError(f"不支持的语句: {head}")

    def _parse_create_table(self, s: str) -> Dict[str, Any]:
        m = re.match(r"CREATE\s+TABLE\s+([a-zA-Z_][\w_]*)\s*\((.*)\)\s*\Z", s, re.IGNORECASE | re.DOTALL)
        if not m:
            raise SQLSyntaxError("CREATE TABLE 语法错误")
        table = m.group(1)
        cols = m.group(2)
        columns: List[Dict[str, str]] = []
        for part in self._split_commas(cols):
            part = part.strip()
            mm = re.match(r"([a-zA-Z_][\w_]*)\s+([a-zA-Z]+)", part)
            if not mm:
                raise SQLSyntaxError(f"列定义错误: {part}")
            columns.append({"name": mm.group(1), "type": mm.group(2).upper()})
        return {"type": "CREATE_TABLE", "table": table, "columns": columns}

    def _parse_insert(self, s: str) -> Dict[str, Any]:
        m = re.match(r"INSERT\s+INTO\s+([a-zA-Z_][\w_]*)\s*\((.*?)\)\s*VALUES\s*\((.*)\)\s*\Z", s, re.IGNORECASE | re.DOTALL)
        if not m:
            raise SQLSyntaxError("INSERT 语法错误")
        table = m.group(1)
        cols = [c.strip() for c in self._split_commas(m.group(2))]
        vals = [self._parse_value(v.strip()) for v in self._split_commas(m.group(3))]
        if len(cols) != len(vals):
            raise SQLSyntaxError("列和值数量不匹配")
        return {"type": "INSERT", "table": table, "columns": cols, "values": vals}

    def _parse_select(self, s: str) -> Dict[str, Any]:
        # SELECT col1,col2 FROM table [WHERE col op value]
        m = re.match(r"SELECT\s+(.*?)\s+FROM\s+([a-zA-Z_][\w_]*)(?:\s+WHERE\s+(.*))?\s*\Z", s, re.IGNORECASE | re.DOTALL)
        if not m:
            raise SQLSyntaxError("SELECT 语法错误")
        proj = [c.strip() for c in self._split_commas(m.group(1))]
        table = m.group(2)
        where = m.group(3)
        cond = None
        if where:
            cond = self._parse_simple_predicate(where.strip())
        return {"type": "SELECT", "table": table, "columns": proj, "where": cond}

    def _parse_delete(self, s: str) -> Dict[str, Any]:
        m = re.match(r"DELETE\s+FROM\s+([a-zA-Z_][\w_]*)(?:\s+WHERE\s+(.*))?\s*\Z", s, re.IGNORECASE | re.DOTALL)
        if not m:
            raise SQLSyntaxError("DELETE 语法错误")
        table = m.group(1)
        where = m.group(2)
        cond = None
        if where:
            cond = self._parse_simple_predicate(where.strip())
        return {"type": "DELETE", "table": table, "where": cond}

    # --- helpers ---
    def _split_commas(self, s: str) -> List[str]:
        parts: List[str] = []
        buf: List[str] = []
        in_str = False
        i = 0
        while i < len(s):
            ch = s[i]
            if ch == "'":
                in_str = not in_str
                buf.append(ch)
            elif ch == "," and not in_str:
                parts.append("".join(buf))
                buf = []
            else:
                buf.append(ch)
            i += 1
        if buf:
            parts.append("".join(buf))
        return parts

    def _parse_value(self, token: str):
        if token.startswith("'") and token.endswith("'"):
            return token[1:-1]
        if re.match(r"^-?\d+$", token):
            return int(token)
        raise SQLSyntaxError(f"不支持的值: {token}")

    def _parse_simple_predicate(self, s: str):
        # col = value | col > value | col < value
        m = re.match(r"([a-zA-Z_][\w_]*)\s*(=|>|<)\s*(.*)\Z", s)
        if not m:
            raise SQLSyntaxError("不支持的WHERE条件")
        col = m.group(1)
        op = m.group(2)
        val = self._parse_value(m.group(3).strip())
        return {"column": col, "op": op, "value": val}
