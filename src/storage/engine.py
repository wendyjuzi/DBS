"""
存储引擎抽象基类 - 添加缓存支持
"""

from __future__ import annotations
from typing import Dict, Iterable, List, Optional, Tuple, Any
import logging
import os
import time

from .file_storage import FileStorage
from .buffer_pool import BufferPool
from .page import Page
from .constants import USER_DB_DIR, DEFAULT_CACHE_CAPACITY, DEFAULT_CACHE_STRATEGY
from .wal import WALManager, LogRecord, Recovery
from .transaction import TransactionManager, Transaction

logger = logging.getLogger(__name__)

class StorageEngine:
    """极简存储引擎，支持缓存管理和页式存储"""

    def __init__(self, base_dir: str = USER_DB_DIR,
                 cache_capacity: int = DEFAULT_CACHE_CAPACITY,
                 cache_strategy: str = DEFAULT_CACHE_STRATEGY,
                 enable_wal: bool = True):
        """
        初始化存储引擎

        Args:
            base_dir: 数据文件存储目录
            cache_capacity: 缓存容量（页数）
            cache_strategy: 缓存替换策略（LRU/FIFO）
            enable_wal: 是否启用WAL日志
        """
        self.fs = FileStorage(base_dir)
        self.buffer_pool = BufferPool(cache_capacity, cache_strategy, self.fs)
        self.enable_wal = enable_wal

        # 页分配管理
        self.page_allocations: Dict[str, List[int]] = {}
        self.free_pages: Dict[str, List[int]] = {}

        # WAL和事务管理
        if enable_wal:
            # 初始化WAL管理器，日志文件放在数据目录的wal子目录
            wal_dir = os.path.join(base_dir, "wal")
            self.wal = WALManager(wal_dir)
            self.txm = TransactionManager(self.wal)
            self._current_txid: Optional[str] = None

            # 系统启动时执行WAL恢复
            self._perform_wal_recovery()
        else:
            self.wal = None
            self.txm = None
            self._current_txid = None

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

            # 记录WAL日志（如果在事务中）
            self._log_page_operation("PAGE_REUSE", table, page_id)

            return page_id

        # 分配新页 - 使用文件中的页数
        page_count = self.fs.page_count(table)
        page_id = page_count
        self.page_allocations[table].append(page_id)

        # 初始化空页并写入磁盘
        page = Page(page_id)
        self.fs.write_page(table, page_id, page.to_bytes())

        # 记录WAL日志（如果在事务中）
        self._log_page_operation("PAGE_ALLOCATE", table, page_id)

        logger.info(f"分配新页: 表={table}, 页={page_id}")
        return page_id

    def free_page(self, table: str, page_id: int):
        """释放一页"""
        if table in self.free_pages and page_id in self.page_allocations.get(table, []):
            self.free_pages[table].append(page_id)

            # 记录WAL日志（如果在事务中）
            self._log_page_operation("PAGE_FREE", table, page_id)

            logger.info(f"释放页: 表={table}, 页={page_id}")

    # --- 缓存接口 ---
    def get_page(self, table: str, page_id: int) -> Optional[Page]:
        """通过缓存获取页"""
        return self.buffer_pool.get_page(table, page_id)

    def write_page(self, table: str, page: Page) -> None:
        """
        写页，支持WAL日志记录

        注意：这里采用no-steal策略，即事务提交前不真正写盘
        采用force策略，即事务提交时强制刷盘
        """
        # 标记为脏页（缓存中）
        self.buffer_pool.mark_dirty(table, page.page_id)

        # 记录WAL日志（如果在事务中）
        if self._current_txid and self.wal:
            log_record = LogRecord(
                txid=self._current_txid,
                op="PAGE_WRITE",
                table=table,
                payload={
                    "page_id": page.page_id,
                    "data": page.to_bytes().hex(),  # 十六进制编码便于JSON序列化
                    "timestamp": time.time()
                }
            )
            self.wal.append(log_record, sync=False)  # 异步刷盘，提高性能

        # 如果不是在事务中，或者采用steal策略，立即写盘
        if self._current_txid is None:
            self.buffer_pool.flush_page(table, page.page_id)

    def flush_page(self, table: str, page_id: int):
        """强制将页写回磁盘"""
        self.buffer_pool.flush_page(table, page_id)

    def flush_all(self):
        """将所有脏页写回磁盘"""
        # 刷盘数据页
        self.buffer_pool.flush_all()

        # 刷盘WAL日志
        if self.wal:
            self.wal.flush()

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

    def _perform_wal_recovery(self) -> None:
        """
        系统启动时执行WAL恢复流程

        恢复步骤：
        1. 分析WAL日志，识别已提交和未提交的事务
        2. 重做所有已提交事务的操作
        3. 撤销未提交事务的操作（可选，需要undo日志）
        4. 清理恢复后的状态
        """
        if not self.enable_wal or self.wal is None:
            return

        logger.info("开始WAL恢复流程...")
        start_time = time.time()

        try:
            # 创建恢复分析器
            recovery = Recovery(self.wal)

            # 分析日志，获取已提交的事务ID
            committed_txids = recovery.analyze_committed()
            logger.info(f"分析完成，已提交事务: {committed_txids}")

            # 重做阶段：对已提交事务的所有操作进行重做
            redo_count = 0
            for record in self.wal.iterate():
                if record.txid not in committed_txids:
                    continue  # 跳过未提交事务

                try:
                    if record.op == "PAGE_WRITE":
                        self._redo_page_write(record)
                        redo_count += 1
                    elif record.op == "PAGE_ALLOCATE":
                        self._redo_page_allocate(record)
                        redo_count += 1
                    elif record.op == "PAGE_FREE":
                        self._redo_page_free(record)
                        redo_count += 1
                except Exception as e:
                    logger.error(f"重做操作失败: {record.op}, 错误: {e}")

            logger.info(f"WAL恢复完成，重做了 {redo_count} 个操作，耗时 {time.time() - start_time:.3f}s")

        except Exception as e:
            logger.error(f"WAL恢复过程中发生错误: {e}")
            # 恢复失败不应该影响系统启动，可以继续运行但记录错误

    def _redo_page_write(self, record: LogRecord) -> None:
        """
        重做页面写操作

        Args:
            record: 日志记录，包含页面数据和元信息
        """
        try:
            table = record.table
            page_id = record.payload["page_id"]
            page_data = bytes.fromhex(record.payload["data"])

            # 直接写入文件系统，绕过缓存（确保数据持久化）
            self.fs.write_page(table, page_id, page_data)

            # 如果页面在缓存中，需要更新缓存以避免脏数据
            if self.buffer_pool:
                # 从缓存中移除该页面，强制下次从磁盘读取最新数据
                key = (table, page_id)
                if key in self.buffer_pool.cache:
                    del self.buffer_pool.cache[key]
                if key in self.buffer_pool.dirty_pages:
                    del self.buffer_pool.dirty_pages[key]

            logger.debug(f"重做页面写: 表={table}, 页={page_id}")

        except Exception as e:
            logger.error(f"重做页面写操作失败: {e}")
            raise

    def _redo_page_allocate(self, record: LogRecord) -> None:
        """
        重做页面分配操作

        Args:
            record: 日志记录，包含页面分配信息
        """
        try:
            table = record.table
            page_id = record.payload["page_id"]

            # 确保页面分配表中有记录
            if table not in self.page_allocations:
                self.page_allocations[table] = []
            if page_id not in self.page_allocations[table]:
                self.page_allocations[table].append(page_id)

            # 从空闲页列表中移除（如果存在）
            if table in self.free_pages and page_id in self.free_pages[table]:
                self.free_pages[table].remove(page_id)

            logger.debug(f"重做页面分配: 表={table}, 页={page_id}")

        except Exception as e:
            logger.error(f"重做页面分配操作失败: {e}")

    def _redo_page_free(self, record: LogRecord) -> None:
        """
        重做页面释放操作

        Args:
            record: 日志记录，包含页面释放信息
        """
        try:
            table = record.table
            page_id = record.payload["page_id"]

            # 添加到空闲页列表
            if table not in self.free_pages:
                self.free_pages[table] = []
            if page_id not in self.free_pages[table]:
                self.free_pages[table].append(page_id)

            # 从分配列表中移除
            if table in self.page_allocations and page_id in self.page_allocations[table]:
                self.page_allocations[table].remove(page_id)

            logger.debug(f"重做页面释放: 表={table}, 页={page_id}")

        except Exception as e:
            logger.error(f"重做页面释放操作失败: {e}")

    # ==================== 事务管理接口 ====================

    def begin_transaction(self) -> Optional[str]:
        """
        开始一个新事务

        Returns:
            str: 事务ID，如果WAL未启用则返回None
        """
        if not self.enable_wal or self.txm is None:
            return None

        try:
            txn = self.txm.begin()
            self._current_txid = txn.txid
            logger.info(f"事务开始: {txn.txid}")
            return txn.txid
        except Exception as e:
            logger.error(f"开始事务失败: {e}")
            return None

    def commit_transaction(self) -> bool:
        """
        提交当前事务

        Returns:
            bool: 提交是否成功
        """
        if not self.enable_wal or self.txm is None or self._current_txid is None:
            return False

        try:
            # 1. 刷盘所有脏页，确保数据持久化
            self.flush_all()

            # 2. 提交事务（写入COMMIT日志记录）
            self.txm.commit(self._current_txid)

            # 3. 刷盘WAL日志，确保日志持久化
            if self.wal:
                self.wal.flush()

            logger.info(f"事务提交: {self._current_txid}")
            self._current_txid = None
            return True

        except Exception as e:
            logger.error(f"提交事务失败: {e}")
            return False

    def rollback_transaction(self) -> bool:
        """
        回滚当前事务

        Returns:
            bool: 回滚是否成功
        """
        if not self.enable_wal or self.txm is None or self._current_txid is None:
            return False

        try:
            # 写入ABORT日志记录
            self.txm.rollback(self._current_txid)

            # 注意：这里只是记录回滚，实际的数据回滚需要undo日志
            # 在当前实现中，回滚主要依靠WAL恢复时不处理未提交事务

            logger.info(f"事务回滚: {self._current_txid}")
            self._current_txid = None
            return True

        except Exception as e:
            logger.error(f"回滚事务失败: {e}")
            return False

    def _log_page_operation(self, operation: str, table: str, page_id: int) -> None:
        """
        记录页面操作日志

        Args:
            operation: 操作类型（PAGE_ALLOCATE, PAGE_FREE, PAGE_REUSE）
            table: 表名
            page_id: 页面ID
        """
        if not self.enable_wal or self.wal is None or self._current_txid is None:
            return

        try:
            log_record = LogRecord(
                txid=self._current_txid,
                op=operation,
                table=table,
                payload={
                    "page_id": page_id,
                    "timestamp": time.time()
                }
            )
            self.wal.append(log_record, sync=False)
        except Exception as e:
            logger.error(f"记录页面操作日志失败: {e}")

    def get_wal_status(self) -> Dict[str, Any]:
        """
        获取WAL状态信息

        Returns:
            Dict: 包含WAL状态的信息字典
        """
        if not self.enable_wal or self.wal is None:
            return {"enabled": False}

        try:
            wal_size = 0
            if os.path.exists(self.wal.path):
                wal_size = os.path.getsize(self.wal.path)

            return {
                "enabled": True,
                "wal_path": str(self.wal.path),
                "wal_size": wal_size,
                "current_txid": self._current_txid,
                "active_transactions": len(self.txm.get_active_txids()) if self.txm else 0
            }
        except Exception as e:
            logger.error(f"获取WAL状态失败: {e}")
            return {"enabled": False, "error": str(e)}

    def checkpoint(self) -> bool:
        """
        创建检查点：刷盘所有数据并截断WAL日志

        Returns:
            bool: 检查点是否创建成功
        """
        if not self.enable_wal or self.wal is None:
            return False

        try:
            # 1. 刷盘所有数据
            self.flush_all()

            # 2. 创建检查点记录
            if self._current_txid:
                log_record = LogRecord(
                    txid="CHECKPOINT",
                    op="CHECKPOINT",
                    payload={
                        "timestamp": time.time(),
                        "active_txids": list(self.txm.get_active_txids()) if self.txm else []
                    }
                )
                self.wal.append(log_record, sync=True)

            # 3. 这里可以添加W日志截断逻辑（实际生产环境需要）
            logger.info("检查点创建完成")
            return True

        except Exception as e:
            logger.error(f"创建检查点失败: {e}")
            return False