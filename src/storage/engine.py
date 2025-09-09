"""
存储引擎抽象基类
"""

from __future__ import annotations

from typing import Dict, Iterable, List, Optional, Tuple

from .file_storage import FileStorage
from .page import Page
from ..utils.constants import CATALOG_DB_DIR
from ..utils.exceptions import StorageError


class StorageEngine:
    """极简存储引擎，支持按页读写与行追加/扫描。"""

    def __init__(self, base_dir: str = CATALOG_DB_DIR):
        self.fs = FileStorage(base_dir)

    # --- Page I/O ---
    def get_page(self, table: str, page_id: int) -> Optional[Page]:
        data = self.fs.read_page(table, page_id)
        if data is None:
            return None
        return Page(page_id, data)

    def write_page(self, table: str, page: Page) -> None:
        self.fs.write_page(table, page.page_id, page.to_bytes())

    def _ensure_page(self, table: str, page_id: int) -> Page:
        page = self.get_page(table, page_id)
        if page is None:
            page = Page(page_id)
        return page

    # --- Row ops ---
    def append_row(self, table: str, row_bytes: bytes) -> Tuple[int, int, int]:
        """Append row, return (page_id, slot_index, offset)."""
        page_count = self.fs.page_count(table)
        page_id = max(0, page_count - 1)
        page = self.get_page(table, page_id)
        if page is None:
            page = Page(0)
            page_id = 0
        try:
            slot_idx, offset = page.insert_row(row_bytes)
        except Exception:
            # create new page
            page_id += 1
            page = Page(page_id)
            slot_idx, offset = page.insert_row(row_bytes)
        self.write_page(table, page)
        return page_id, slot_idx, offset

    def scan_rows(self, table: str) -> Iterable[Tuple[int, int, bytes]]:
        page_count = self.fs.page_count(table)
        for pid in range(page_count):
            page = self.get_page(table, pid)
            if not page:
                continue
            for slot_idx, row in page.iterate_rows():
                yield pid, slot_idx, row

    def delete_in_page(self, table: str, page_id: int, slot_index: int) -> None:
        page = self.get_page(table, page_id)
        if page is None:
            raise StorageError("Page not found")
        page.mark_deleted(slot_index)
        self.write_page(table, page)
