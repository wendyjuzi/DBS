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

    def __init__(self, capacity: int = 100, strategy: str = "LRU", fs: FileStorage = None, enable_readahead: bool = True, readahead_window: int = 3):
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

        self.enable_readahead = enable_readahead
        self.readahead_window = readahead_window  # 预读页面数量
        self.last_access = None  # 记录最后一次访问的 (table, page_id)

        if self.strategy not in ["LRU", "FIFO"]:
            raise ValueError("替换策略必须是 'LRU' 或 'FIFO'")

    # buffer_pool.py 修改 get_page 方法
    def get_page(self, table: str, page_id: int) -> Optional[Page]:
        """获取页，如果不在缓存中则从磁盘加载"""
        # 参数验证
        if not table or not isinstance(table, str):
            raise ValueError("表名不能为空且必须是字符串")
        if page_id < 0:
            raise ValueError("页号不能为负数")

        key = (table, page_id)

        # 检查缓存命中
        if key in self.cache:
            self.hits += 1
            page = self.cache[key]
            # LRU策略：将访问的页移到最新位置
            if self.strategy == "LRU":
                self.cache.move_to_end(key)
            logger.debug(f"缓存命中: 表={table}, 页={page_id}")

            # 触发预读：如果启用且是顺序访问
            if self.enable_readahead and self._is_sequential_access(table, page_id):
                self._readahead(table, page_id)

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

        # 如果缓存容量为0，直接返回页但不加入缓存
        if self.capacity == 0:
            return page

        # 如果缓存已满，执行替换策略
        if len(self.cache) >= self.capacity:
            self._evict_page()

        # 将新页加入缓存
        self.cache[key] = page
        self.dirty_pages[key] = False

        # FIFO策略：新页放在最后
        if self.strategy == "FIFO":
            self.cache.move_to_end(key)

        # 将新页加入缓存后也触发预读
        if self.enable_readahead and self._is_sequential_access(table, page_id):
            self._readahead(table, page_id)

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

    def _is_sequential_access(self, table: str, current_page_id: int) -> bool:
        """判断是否为顺序访问（用于触发预读）"""
        if self.last_access is None:
            self.last_access = (table, current_page_id)
            return False

        last_table, last_page_id = self.last_access
        self.last_access = (table, current_page_id)

        # 只有相同表且页面ID连续才认为是顺序访问
        if table != last_table:
            return False

        # 页面ID连续（包括正序和倒序）
        return abs(current_page_id - last_page_id) == 1

    def _readahead(self, table: str, current_page_id: int):
        """执行预读：提前加载后续页面到缓存"""
        for offset in range(1, self.readahead_window + 1):
            next_page_id = current_page_id + offset

            # 检查页面是否已经在缓存中
            if (table, next_page_id) in self.cache:
                continue

            # 检查页面是否存在（避免预读不存在的页面）
            if not self.fs._table_path(table).exists():
                continue

            # 从磁盘加载页面（但不标记为脏）
            try:
                page_data = self.fs.read_page(table, next_page_id)
                if page_data is None:
                    continue  # 页面不存在

                page = Page(next_page_id, page_data)

                # 如果缓存已满，执行替换策略
                if len(self.cache) >= self.capacity:
                    self._evict_page()

                # 将预读页面加入缓存（但不标记为脏）
                key = (table, next_page_id)
                self.cache[key] = page
                self.dirty_pages[key] = False

                if self.strategy == "FIFO":
                    self.cache.move_to_end(key)

                logger.debug(f"预读页面: 表={table}, 页={next_page_id}")

            except Exception as e:
                logger.warning(f"预读页面失败: 表={table}, 页={next_page_id}, 错误: {e}")