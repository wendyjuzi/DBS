"""
基于文件的存储引擎实现
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from ..utils.constants import PAGE_SIZE
from ..utils.helpers import ensure_dir


class FileStorage:
    """最简单的基于单文件的分页存取。每个表一个数据文件。"""

    def __init__(self, base_dir: str | Path):
        self.base_dir = ensure_dir(base_dir)

    def _table_path(self, table_name: str) -> Path:
        return self.base_dir / f"{table_name}.tbl"

    def read_page(self, table_name: str, page_id: int) -> Optional[bytes]:
        path = self._table_path(table_name)
        if not path.exists():
            return None
        with path.open("rb") as f:
            f.seek(page_id * PAGE_SIZE)
            data = f.read(PAGE_SIZE)
            if not data or len(data) < PAGE_SIZE:
                return None
            return data

    def write_page(self, table_name: str, page_id: int, page_bytes: bytes) -> None:
        if len(page_bytes) != PAGE_SIZE:
            raise ValueError("Page size mismatch")
        path = self._table_path(table_name)
        ensure_dir(path.parent)
        flags = os.O_RDWR | os.O_CREAT
        # use binary mode default permissions
        fd = os.open(path, flags)
        try:
            os.lseek(fd, page_id * PAGE_SIZE, os.SEEK_SET)
            os.write(fd, page_bytes)
            os.fsync(fd)
        finally:
            os.close(fd)

    def page_count(self, table_name: str) -> int:
        path = self._table_path(table_name)
        if not path.exists():
            return 0
        size = path.stat().st_size
        return size // PAGE_SIZE
