"""
存储引擎抽象基类 - 添加缓存支持
"""

from __future__ import annotations
from typing import Dict, Iterable, List, Optional, Tuple, Any
import logging

from .file_storage import FileStorage
from .buffer_pool import BufferPool
from .page import Page
from .constants import USER_DB_DIR, DEFAULT_CACHE_CAPACITY, DEFAULT_CACHE_STRATEGY

logger = logging.getLogger(__name__)

class StorageEngine:
    """极简存储引擎，支持缓存管理和页式存储"""

    def __init__(self, base_dir: str = USER_DB_DIR, cache_capacity: int = DEFAULT_CACHE_CAPACITY, cache_strategy: str = DEFAULT_CACHE_STRATEGY):
        self.fs = FileStorage(base_dir)
        self.buffer_pool = BufferPool(cache_capacity, cache_strategy, self.fs)

        # 页分配管理
        self.page_allocations: Dict[str, List[int]] = {}
        self.free_pages: Dict[str, List[int]] = {}

    # engine.py 修改 allocate_page 方法
    def allocate_page(self, table: str) -> int:
        """分配一个新页"""
        if table not in self.page_allocations:
            self.page_allocations[table] = []
            self.free_pages[table] = []

        # 首先尝试重用空闲页
        if self.free_pages[table]:
            page_id = self.free_pages[table].pop()
            logger.info(f"重用空闲页: 表={table}, 页={page_id}")
            return page_id

        # 分配新页 - 使用文件中的页数
        page_count = self.fs.page_count(table)
        page_id = page_count
        self.page_allocations[table].append(page_id)

        # 初始化空页并写入磁盘
        page = Page(page_id)
        self.fs.write_page(table, page_id, page.to_bytes())

        logger.info(f"分配新页: 表={table}, 页={page_id}")
        return page_id

    def free_page(self, table: str, page_id: int):
        """释放一页"""
        if table in self.free_pages and page_id in self.page_allocations.get(table, []):
            self.free_pages[table].append(page_id)
            logger.info(f"释放页: 表={table}, 页={page_id}")

    # --- 缓存接口 ---
    def get_page(self, table: str, page_id: int) -> Optional[Page]:
        """通过缓存获取页"""
        return self.buffer_pool.get_page(table, page_id)

    # engine.py 修改 write_page 方法
    def write_page(self, table: str, page: Page) -> None:
        """写页，标记为脏页并立即写入磁盘"""
        self.buffer_pool.mark_dirty(table, page.page_id)
        # 立即写入磁盘以确保持久化
        self.buffer_pool.flush_page(table, page.page_id)

    def flush_page(self, table: str, page_id: int):
        """强制将页写回磁盘"""
        self.buffer_pool.flush_page(table, page_id)

    def flush_all(self):
        """将所有脏页写回磁盘"""
        self.buffer_pool.flush_all()

    def get_cache_stats(self) -> Dict[str, Any]:
        """获取缓存统计信息"""
        return self.buffer_pool.get_stats()

    # --- 行操作 ---
    def append_row(self, table: str, row_bytes: bytes) -> Tuple[int, int, int]:
        """追加行，支持自动页分配"""
        page_count = self.fs.page_count(table)
        page_id = max(0, page_count - 1) if page_count > 0 else self.allocate_page(table)

        # 尝试插入到现有页
        page = self.get_page(table, page_id)
        if page is None:
            page = Page(page_id)

        try:
            slot_idx, offset = page.insert_row(row_bytes)
            self.write_page(table, page)
            return page_id, slot_idx, offset
        except ValueError:
            # 当前页已满，分配新页
            page_id = self.allocate_page(table)
            page = Page(page_id)
            slot_idx, offset = page.insert_row(row_bytes)
            self.write_page(table, page)
            return page_id, slot_idx, offset

    def scan_rows(self, table: str) -> Iterable[Tuple[int, int, bytes]]:
        """扫描所有页中的行"""
        page_count = self.fs.page_count(table)
        for page_id in range(page_count):
            page = self.get_page(table, page_id)
            if not page:
                continue
            for slot_idx, row in page.iterate_rows():
                yield page_id, slot_idx, row

    def delete_row(self, table: str, page_id: int, slot_index: int) -> None:
        """删除行"""
        page = self.get_page(table, page_id)
        if page is None:
            raise ValueError("Page not found")
        page.mark_deleted(slot_index)
        self.write_page(table, page)
        logger.debug(f"删除行: 表={table}, 页={page_id}, 槽={slot_index}")