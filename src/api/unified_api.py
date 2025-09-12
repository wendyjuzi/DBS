"""
统一入口：将 系统目录 + 存储引擎 + 执行引擎 连接为一个易用接口。

- 目录(SystemCatalog)：由 C++ StorageEngine 内置并持久化到 sys_catalog_page_0.bin
- 存储(StorageEngine)：页式存储、页缓存、刷盘
- 执行(ExecutionEngine + HybridExecutionEngine)：Create/Insert/SeqScan/Filter/Project/IndexScan

用法：
    from src.api.unified_api import UnifiedDB
    db = UnifiedDB()
    db.create_table('t2', [('id','INT',True), ('name','STRING',False), ('age','INT',False), ('score','DOUBLE',False)])
    db.insert_many('t2', [["1","Alice","20","95.5"], ["2","Bob","19","88.0"]])
    print(db.select("SELECT id,name FROM t2 WHERE id = 1"))
"""

from __future__ import annotations

from typing import List, Tuple, Dict, Any

from .db_api import DatabaseAPI


class UnifiedDB:
    def __init__(self) -> None:
        self._db = DatabaseAPI()
        # 透出底层组件（可选高级用）：
        self.catalog = getattr(self._db, "_storage", None)
        self.storage = getattr(self._db, "_storage", None)
        self.executor = getattr(self._db, "_executor", None)
        self.runner = getattr(self._db, "_runner", None)

    def create_table(self, table: str, columns: List[Tuple[str, str, bool]]) -> Dict[str, Any]:
        cols = ", ".join([f"{name} {type}{' PRIMARY KEY' if is_pk else ''}" for name, type, is_pk in columns])
        return self._db.execute(f"CREATE TABLE {table}({cols})")

    def insert(self, table: str, row: List[str]) -> Dict[str, Any]:
        values = ",".join([f"'{v}'" if not str(v).isdigit() else str(v) for v in row])
        return self._db.execute(f"INSERT INTO {table} VALUES ({values})")

    def insert_many(self, table: str, rows: List[List[str]]) -> int:
        if not self.runner:
            # 回退逐行
            ok = 0
            for r in rows:
                if self.insert(table, r).get("status") == "success":
                    ok += 1
            return ok
        return int(self.runner.insert_many(table, rows))

    def select(self, sql: str) -> Dict[str, Any]:
        return self._db.execute(sql)

    def flush(self) -> None:
        self._db.flush()

    # --- 事务便捷方法 ---
    def begin(self) -> str:
        if hasattr(self._db, "begin"):
            return self._db.begin()
        return getattr(self.runner, "begin", lambda: "")()

    def commit(self) -> None:
        if hasattr(self._db, "commit"):
            return self._db.commit()
        return getattr(self.runner, "commit", lambda: None)()

    def rollback(self) -> None:
        if hasattr(self._db, "rollback"):
            return self._db.rollback()
        return getattr(self.runner, "rollback", lambda: None)()

    # --- 事务覆盖层观测 ---
    def show_tx_overlay(self):
        if hasattr(self.runner, "get_tx_overlay_snapshot"):
            return self.runner.get_tx_overlay_snapshot()
        return {"in_tx": False, "tables": {}}

    # --- 索引便捷方法（内存二级索引） ---
    def create_index(self, table: str, column: str, pk_column: str) -> bool:
        mgr = getattr(self.runner, "index_manager", None)
        return bool(mgr and mgr.create_index(table, column, pk_column))

    def drop_index(self, table: str, column: str) -> bool:
        mgr = getattr(self.runner, "index_manager", None)
        return bool(mgr and mgr.drop_index(table, column))


