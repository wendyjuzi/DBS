"""
查询优化器 - 基于简单规则选择访问方法，并输出标准执行计划：
- SELECT：选择 access_method（index_scan / index_range_scan / seq_scan），输出 filter 条件列表
- INSERT/DELETE/CREATE：直接透传/附带必要元数据
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


class QueryOptimizer:
    """规则型优化器（简化版）。"""

    def __init__(self, storage_engine=None, catalog=None) -> None:
        self.storage = storage_engine
        self.catalog = catalog

    def optimize(self, ast: Dict[str, Any]) -> Dict[str, Any]:
        qtype = ast.get("type")
        if qtype == "SELECT":
            return self._optimize_select(ast)
        if qtype == "INSERT":
            return {"type": "INSERT", "table": ast["table"], "values": ast["values"]}
        if qtype == "DELETE":
            # 直接全表扫描删除（可扩展索引）
            return {"type": "DELETE", "table": ast["table"], "where": ast.get("where")}
        if qtype == "CREATE_TABLE":
            return ast
        return ast

    def _optimize_select(self, ast: Dict[str, Any]) -> Dict[str, Any]:
        table = ast["table"]
        columns = ast.get("columns", ["*"])
        where = ast.get("where")

        filter_list = self._parse_where(where) if where else []

        # 主键等值 → index_scan；主键范围 → index_range_scan；否则 seq_scan
        method = "seq_scan"
        params: Dict[str, Any] = {}

        if self._has_index(table):
            pk_cond = self._extract_pk_condition(filter_list)
            if pk_cond:
                if pk_cond["op"] == "=":
                    method = "index_scan"
                    params = {"pk_value": pk_cond["value"], "pk_column": pk_cond["column"]}
                elif pk_cond["op"] in (">", ">=", "<", "<="):
                    method = "index_range_scan"
                    # 简化：以字符串边界表达范围
                    if pk_cond["op"] in (">", ">="):
                        params = {"min_pk": pk_cond["value"], "max_pk": "\xFF\xFF\xFF\xFF"}
                    else:
                        params = {"min_pk": "", "max_pk": pk_cond["value"]}

        plan = {
            "type": "SELECT",
            "table": table,
            "columns": columns,
            "access_method": method,
            "access_params": params,
        }
        if filter_list:
            plan["filter"] = filter_list
        return plan

    def _has_index(self, table: str) -> bool:
        try:
            if hasattr(self.storage, "has_index"):
                return bool(self.storage.has_index(table))
        except Exception:
            pass
        return False

    def _parse_where(self, where_clause: str) -> List[Dict[str, Any]]:
        conds: List[Dict[str, Any]] = []
        parts = [p.strip() for p in where_clause.split(" AND ")]
        ops = ["!=", ">=", "<=", "=", ">", "<"]
        for part in parts:
            for op in ops:
                if op in part:
                    left, right = [x.strip() for x in part.split(op, 1)]
                    if (right.startswith("'") and right.endswith("'")) or (right.startswith('"') and right.endswith('"')):
                        right = right[1:-1]
                    conds.append({"column": left, "op": op, "value": right})
                    break
        return conds

    def _extract_pk_condition(self, conds: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        # 简化：优先匹配列名为 id/pk/primary_key
        for c in conds:
            if c.get("column") in ("id", "pk", "primary_key"):
                return c
        return None
