"""
缓存管理实现
"""

from __future__ import annotations
from collections import OrderedDict
from typing import Dict, Optional, Tuple
import logging

from .page import Page
from .file_storage import FileStorage
from .constants import PAGE_SIZE, LOG_LEVEL, ENABLE_CACHE_LOG

# 配置日志
logger = logging.getLogger(__name__)
if ENABLE_CACHE_LOG:
    logging.basicConfig(level=getattr(logging, LOG_LEVEL))

class BufferPool:
    """页缓存池，支持LRU和FIFO替换策略"""

    def __init__(self, capacity: int = 100, strategy: str = "LRU", fs: FileStorage = None):
        self.capacity = capacity
        self.strategy = strategy.upper()
        self.fs = fs

        # 缓存数据结构
        # 使用 OrderedDict 实现 LRU
        self.cache: OrderedDict[Tuple[str, int], Page] = OrderedDict()
        self.dirty_pages: Dict[Tuple[str, int], bool] = {}

        # 统计信息
        self.hits = 0
        self.misses = 0
        self.evictions = 0

        if self.strategy not in ["LRU", "FIFO"]:
            raise ValueError("替换策略必须是 'LRU' 或 'FIFO'")

    # buffer_pool.py 修改 get_page 方法
    def get_page(self, table: str, page_id: int) -> Optional[Page]:
        """获取页，如果不在缓存中则从磁盘加载"""
        key = (table, page_id)

        # 检查缓存命中
        if key in self.cache:
            self.hits += 1
            page = self.cache[key]
            # LRU策略：将访问的页移到最新位置
            if self.strategy == "LRU":
                self.cache.move_to_end(key)
            logger.debug(f"缓存命中: 表={table}, 页={page_id}")
            return page

        # 缓存未命中
        self.misses += 1
        logger.debug(f"缓存未命中: 表={table}, 页={page_id}")

        # 从磁盘加载页 - 需要先检查文件是否存在
        if not self.fs._table_path(table).exists():
            # 文件不存在，创建空页
            page = Page(page_id)
        else:
            page_data = self.fs.read_page(table, page_id)
            if page_data is None:
                # 页不存在，创建空页
                page = Page(page_id)
            else:
                page = Page(page_id, page_data)

        # 如果缓存已满，执行替换策略
        if len(self.cache) >= self.capacity:
            self._evict_page()

        # 将新页加入缓存
        self.cache[key] = page
        self.dirty_pages[key] = False

        # FIFO策略：新页放在最后
        if self.strategy == "FIFO":
            self.cache.move_to_end(key)

        return page

    # buffer_pool.py 修改 _evict_page 方法
    def _evict_page(self):
        """根据替换策略淘汰一页"""
        if not self.cache:
            return

        if self.strategy == "LRU":
            # LRU淘汰最久未使用的（第一个）
            key, page = self.cache.popitem(last=False)
        else:  # FIFO
            # FIFO淘汰最先进入的（第一个）
            key, page = self.cache.popitem(last=False)

        # 如果页是脏页，写回磁盘
        if self.dirty_pages.get(key, False):
            self.fs.write_page(key[0], key[1], page.to_bytes())

        self.evictions += 1
        logger.info(f"页淘汰: 表={key[0]}, 页={key[1]}, 策略={self.strategy}")

        # 清理脏页标记
        if key in self.dirty_pages:
            del self.dirty_pages[key]

    def mark_dirty(self, table: str, page_id: int):
        """标记页为脏页"""
        key = (table, page_id)
        if key in self.cache:
            self.dirty_pages[key] = True

    def flush_page(self, table: str, page_id: int):
        """将页写回磁盘"""
        key = (table, page_id)
        if key in self.cache:
            page = self.cache[key]
            self.fs.write_page(table, page_id, page.to_bytes())
            self.dirty_pages[key] = False
            logger.debug(f"页写回磁盘: 表={table}, 页={page_id}")

    def flush_all(self):
        """将所有脏页写回磁盘"""
        for key in list(self.dirty_pages.keys()):
            if self.dirty_pages[key]:
                self.flush_page(key[0], key[1])

    def get_stats(self) -> Dict:
        """获取缓存统计信息"""
        return {
            "cache_size": len(self.cache),
            "capacity": self.capacity,
            "hits": self.hits,
            "misses": self.misses,
            "hit_rate": self.hits / (self.hits + self.misses) if (self.hits + self.misses) > 0 else 0,
            "evictions": self.evictions,
            "dirty_pages": sum(1 for dirty in self.dirty_pages.values() if dirty),
            "strategy": self.strategy
        }

    def clear(self):
        """清空缓存"""
        self.flush_all()
        self.cache.clear()
        self.dirty_pages.clear()
        self.hits = 0
        self.misses = 0
        self.evictions = 0