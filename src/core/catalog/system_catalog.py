"""
系统目录管理
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from ...utils.constants import CATALOG_DB_DIR, CATALOG_META_FILE
from ...utils.helpers import ensure_dir, read_json, write_json
from ...utils.exceptions import CatalogError


class SystemCatalog:
    """极简系统目录，持久化表元数据。"""

    def __init__(self, base_dir: str = CATALOG_DB_DIR):
        self.base_dir = ensure_dir(base_dir)
        self.meta_path = self.base_dir / CATALOG_META_FILE
        self._tables: Dict[str, Dict[str, Any]] = {}
        self._load()

    def _load(self) -> None:
        data = read_json(self.meta_path, default={"tables": {}})
        self._tables = data.get("tables", {})

    def _flush(self) -> None:
        write_json(self.meta_path, {"tables": self._tables})

    # --- API ---
    def create_table(self, name: str, columns: List[Dict[str, str]]) -> None:
        if name in self._tables:
            raise CatalogError(f"表已存在: {name}")
        # normalize
        norm_cols = []
        for col in columns:
            norm_cols.append({"name": col["name"], "type": col["type"].upper()})
        self._tables[name] = {"columns": norm_cols}
        self._flush()

    def get_tables(self) -> List[str]:
        return sorted(self._tables.keys())

    def get_table_schema(self, name: str) -> Dict[str, Any]:
        if name not in self._tables:
            raise CatalogError(f"表不存在: {name}")
        return self._tables[name]

    def has_table(self, name: str) -> bool:
        return name in self._tables
