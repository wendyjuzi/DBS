"""
查询执行器
"""

from __future__ import annotations

import json
from typing import Any, Dict, Iterable, List, Tuple

from ...utils.exceptions import ExecutionError
from ...utils.constants import TYPE_INT, TYPE_TEXT


class QueryExecutor:
    def __init__(self, storage_engine, catalog):
        self.storage = storage_engine
        self.catalog = catalog

    # --- public ---
    def execute(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        typ = plan.get("type")
        if typ == "CREATE_TABLE":
            return self._exec_create_table(plan)
        if typ == "INSERT":
            return self._exec_insert(plan)
        if typ == "SELECT":
            return self._exec_select(plan)
        if typ == "DELETE":
            return self._exec_delete(plan)
        raise ExecutionError(f"未知计划类型: {typ}")

    # --- operators ---
    def _exec_create_table(self, p: Dict[str, Any]) -> Dict[str, Any]:
        self.catalog.create_table(p["table"], p["columns"])
        return {"affected_rows": 0, "metadata": {"message": "table created"}}

    def _exec_insert(self, p: Dict[str, Any]) -> Dict[str, Any]:
        table = p["table"]
        schema = self.catalog.get_table_schema(table)
        col_names = p["columns"]
        values = p["values"]
        # reorder to table schema order
        ordering = {name: idx for idx, name in enumerate(col_names)}
        row_obj = {}
        for col in schema["columns"]:
            name = col["name"]
            if name not in ordering:
                raise ExecutionError(f"缺少列: {name}")
            val = values[ordering[name]]
            t = col["type"].upper()
            if t == TYPE_INT and not isinstance(val, int):
                raise ExecutionError(f"列{ name }应为INT")
            if t == TYPE_TEXT and not isinstance(val, (str,)):
                raise ExecutionError(f"列{ name }应为TEXT")
            row_obj[name] = val
        row_bytes = json.dumps(row_obj, ensure_ascii=False).encode("utf-8")
        self.storage.append_row(table, row_bytes)
        return {"affected_rows": 1}

    def _seq_scan(self, table: str) -> Iterable[Dict[str, Any]]:
        for _, _, row_bytes in self.storage.scan_rows(table):
            yield json.loads(row_bytes.decode("utf-8"))

    def _apply_filter(self, rows: Iterable[Dict[str, Any]], cond: Dict[str, Any] | None):
        if not cond:
            for r in rows:
                yield r
            return
        col = cond["column"]
        op = cond["op"]
        val = cond["value"]
        for r in rows:
            if col not in r:
                continue
            lhs = r[col]
            ok = False
            if op == "=":
                ok = lhs == val
            elif op == ">":
                ok = lhs > val
            elif op == "<":
                ok = lhs < val
            if ok:
                yield r

    def _apply_project(self, rows: Iterable[Dict[str, Any]], cols: List[str]):
        if len(cols) == 1 and cols[0] == "*":
            for r in rows:
                yield r
            return
        for r in rows:
            yield {c: r.get(c) for c in cols}

    def _exec_select(self, p: Dict[str, Any]) -> Dict[str, Any]:
        table = p["table"]
        rows = self._seq_scan(table)
        rows = self._apply_filter(rows, p.get("where"))
        rows = self._apply_project(rows, p.get("columns", ["*"]))
        data = list(rows)
        return {"data": data, "affected_rows": len(data), "metadata": {"columns": list(data[0].keys()) if data else []}}

    def _exec_delete(self, p: Dict[str, Any]) -> Dict[str, Any]:
        table = p["table"]
        cond = p.get("where")
        affected = 0
        # naive: rescan and mark deleted where matches
        for page_id, slot_idx, row_bytes in list(self.storage.scan_rows(table)):
            r = json.loads(row_bytes.decode("utf-8"))
            keep = True
            if cond:
                col, op, val = cond["column"], cond["op"], cond["value"]
                lhs = r.get(col)
                if lhs is None:
                    keep = True
                elif op == "=":
                    keep = not (lhs == val)
                elif op == ">":
                    keep = not (lhs > val)
                elif op == "<":
                    keep = not (lhs < val)
            if not keep:
                self.storage.delete_in_page(table, page_id, slot_idx)
                affected += 1
        return {"affected_rows": affected}
