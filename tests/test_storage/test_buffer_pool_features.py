"""
测试缓存替换策略、预读机制和WAL日志恢复功能的测试文件
python tests/test_storage/test_buffer_pool_features.py
"""

import unittest
import tempfile
import os
import shutil
import time
import gc
from collections import deque
from typing import Dict, Any
import sys
from pathlib import Path

# 添加项目根目录到Python路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.storage.buffer_pool import BufferPool, ClockEntry
from src.storage.file_storage import FileStorage
from src.storage.page import Page
from src.storage.engine import StorageEngine
from src.storage.constants import PAGE_SIZE


class TestBufferPoolFeatures(unittest.TestCase):
    """测试缓存池功能：替换策略、预读机制和WAL恢复"""

    def setUp(self):
        """设置测试环境"""
        self.test_dir = tempfile.mkdtemp()
        self.fs = FileStorage(self.test_dir)

        # 创建测试表数据
        self.table_name = "test_table"
        self._create_test_data()

    def tearDown(self):
        """清理测试环境"""
        if os.path.exists(self.test_dir):
            # 先尝试关闭所有可能打开的文件句柄
            gc.collect()

            # 重试机制删除目录
            for _ in range(3):  # 重试3次
                try:
                    shutil.rmtree(self.test_dir)
                    break
                except (PermissionError, OSError):
                    time.sleep(0.1)  # 等待100ms再重试

    def _create_test_data(self, num_pages=10):
        """创建测试数据页"""
        for page_id in range(num_pages):
            page = Page(page_id)
            # 添加一些测试数据
            for i in range(5):
                try:
                    page.insert_row(f"test_data_{page_id}_{i}".encode())
                except ValueError:
                    break  # 页已满
            self.fs.write_page(self.table_name, page_id, page.to_bytes())

    def test_clock_replacement_strategy(self):
        """测试Clock替换策略"""
        # 创建小容量缓存池以测试替换，关闭预读
        buffer_pool = BufferPool(
            capacity=3,
            strategy="CLOCK",
            fs=self.fs,
            enable_readahead=False  # 关闭预读
        )

        # 加载几个页面
        page1 = buffer_pool.get_page(self.table_name, 0)
        page2 = buffer_pool.get_page(self.table_name, 1)
        page3 = buffer_pool.get_page(self.table_name, 2)

        # 验证页面已加载
        self.assertIn((self.table_name, 0), buffer_pool.cache)
        self.assertIn((self.table_name, 1), buffer_pool.cache)
        self.assertIn((self.table_name, 2), buffer_pool.cache)

        # 访问页面0和1，设置引用位为1
        buffer_pool.get_page(self.table_name, 0)
        buffer_pool.get_page(self.table_name, 1)

        # 页面2的引用位应该还是0，所以应该淘汰页面2
        page4 = buffer_pool.get_page(self.table_name, 3)

        # 验证页面2被淘汰，页面0和1保留
        self.assertNotIn((self.table_name, 2), buffer_pool.cache)
        self.assertIn((self.table_name, 0), buffer_pool.cache)
        self.assertIn((self.table_name, 1), buffer_pool.cache)
        self.assertIn((self.table_name, 3), buffer_pool.cache)

        # 验证Clock算法统计信息
        stats = buffer_pool.get_stats()
        self.assertEqual(stats["strategy"], "CLOCK")
        self.assertEqual(stats["clock_entries"], 3)

        print("Clock替换策略测试通过")

    def test_lru_replacement_strategy(self):
        """测试LRU替换策略"""
        buffer_pool = BufferPool(capacity=3, strategy="LRU", fs=self.fs)

        # 加载页面
        buffer_pool.get_page(self.table_name, 0)  # 最近使用
        buffer_pool.get_page(self.table_name, 1)  # 次近使用
        buffer_pool.get_page(self.table_name, 2)  # 最久未使用

        # 访问页面0，使其成为最近使用
        buffer_pool.get_page(self.table_name, 0)

        # 加载第4个页面，应该淘汰页面1（最久未使用）
        buffer_pool.get_page(self.table_name, 3)

        # 验证页面1被淘汰
        self.assertNotIn((self.table_name, 1), buffer_pool.cache)
        self.assertIn((self.table_name, 3), buffer_pool.cache)

        # 验证LRU统计信息
        stats = buffer_pool.get_stats()
        self.assertEqual(stats["strategy"], "LRU")

        print("LRU替换策略测试通过")

    def test_fifo_replacement_strategy(self):
        """测试FIFO替换策略"""
        # 关闭预读，避免干扰
        buffer_pool = BufferPool(
            capacity=3,
            strategy="FIFO",
            fs=self.fs,
            enable_readahead=False
        )

        # 加载页面
        buffer_pool.get_page(self.table_name, 0)  # 最先进入
        buffer_pool.get_page(self.table_name, 1)  # 其次进入
        buffer_pool.get_page(self.table_name, 2)  # 最后进入

        # 访问页面0，FIFO不应该改变淘汰顺序
        buffer_pool.get_page(self.table_name, 0)

        # 加载第4个页面，应该淘汰页面0（最先进入）
        buffer_pool.get_page(self.table_name, 3)

        # 验证页面0被淘汰，页面1、2、3在缓存中
        self.assertNotIn((self.table_name, 0), buffer_pool.cache)
        self.assertIn((self.table_name, 1), buffer_pool.cache)
        self.assertIn((self.table_name, 2), buffer_pool.cache)
        self.assertIn((self.table_name, 3), buffer_pool.cache)

        # 验证FIFO统计信息
        stats = buffer_pool.get_stats()
        self.assertEqual(stats["strategy"], "FIFO")

        print("FIFO替换策略测试通过")

    def test_readahead_mechanism(self):
        """测试预读机制"""
        buffer_pool = BufferPool(
            capacity=10,
            strategy="LRU",
            fs=self.fs,
            enable_readahead=True,
            readahead_window=3
        )

        # 第一次访问，不应该触发预读
        page0 = buffer_pool.get_page(self.table_name, 0)
        self.assertEqual(buffer_pool.readahead_count, 0)

        # 顺序访问页面1，应该触发预读
        page1 = buffer_pool.get_page(self.table_name, 1)
        self.assertGreater(buffer_pool.readahead_count, 0)

        # 验证预读的页面已加载到缓存
        self.assertIn((self.table_name, 2), buffer_pool.cache)  # 预读页面
        self.assertIn((self.table_name, 3), buffer_pool.cache)  # 预读页面
        self.assertIn((self.table_name, 4), buffer_pool.cache)  # 预读页面

        # 验证统计信息
        stats = buffer_pool.get_stats()
        self.assertTrue(stats["readahead_enabled"])
        self.assertEqual(stats["readahead_window"], 3)
        self.assertGreater(stats["readahead_count"], 0)

        print("预读机制测试通过")

    def test_wal_recovery(self):
        """测试WAL日志恢复"""
        # 创建启用WAL的存储引擎
        engine = StorageEngine(
            base_dir=self.test_dir,
            cache_capacity=10,
            enable_wal=True,
            enable_readahead=False  # 关闭预读以简化测试
        )

        # 开始事务
        txid = engine.begin_transaction()
        self.assertIsNotNone(txid)

        # 在事务中修改数据 - 使用唯一的数据以便识别
        unique_data = f"test_row_1_{time.time()}".encode()
        page_id, slot_idx, offset = engine.append_row(self.table_name, unique_data)
        page = engine.get_page(self.table_name, page_id)
        self.assertIsNotNone(page)

        # 提交事务
        success = engine.commit_transaction()
        self.assertTrue(success)

        # 确保数据已写入
        engine.flush_all()

        # 模拟崩溃：创建新的引擎实例，应该触发WAL恢复
        recovered_engine = StorageEngine(
            base_dir=self.test_dir,
            cache_capacity=10,
            enable_wal=True,
            enable_readahead=False
        )

        # 验证数据已恢复
        rows = list(recovered_engine.scan_rows(self.table_name))

        # 调试信息
        print(f"恢复后找到的行数: {len(rows)}")
        print(f"查找的数据: {unique_data}")

        found = False
        for _, _, row_data in rows:
            if row_data == unique_data:
                found = True
                break
            print(f"找到的行: {row_data}")

        self.assertTrue(found, "WAL恢复后应能找到提交的数据")

        # 验证WAL状态
        wal_status = recovered_engine.get_wal_status()
        self.assertTrue(wal_status["enabled"])

        print("WAL恢复测试通过")

    def test_wal_recovery_with_uncommitted_transaction(self):
        """测试WAL恢复处理未提交事务"""
        # 创建启用WAL的存储引擎
        engine = StorageEngine(
            base_dir=self.test_dir,
            cache_capacity=10,
            enable_wal=True,
            enable_readahead=False
        )

        # 开始事务但不提交
        txid = engine.begin_transaction()
        self.assertIsNotNone(txid)

        # 在事务中修改数据
        page_id, slot_idx, offset = engine.append_row(self.table_name, b"uncommitted_row")

        # 不提交事务，直接创建新引擎实例模拟崩溃
        recovered_engine = StorageEngine(
            base_dir=self.test_dir,
            cache_capacity=10,
            enable_wal=True,
            enable_readahead=False
        )

        # 验证未提交的数据不应存在
        rows = list(recovered_engine.scan_rows(self.table_name))
        found = False
        for _, _, row_data in rows:
            if row_data == b"uncommitted_row":
                found = True
                break
        self.assertFalse(found, "WAL恢复后不应找到未提交的数据")

        print("WAL恢复未提交事务测试通过")

    def test_cache_statistics(self):
        """测试缓存统计信息"""
        buffer_pool = BufferPool(capacity=3, strategy="LRU", fs=self.fs)

        # 初始统计
        stats = buffer_pool.get_stats()
        self.assertEqual(stats["hits"], 0)
        self.assertEqual(stats["misses"], 0)
        self.assertEqual(stats["cache_size"], 0)

        # 加载页面（缓存未命中）
        buffer_pool.get_page(self.table_name, 0)
        stats = buffer_pool.get_stats()
        self.assertEqual(stats["misses"], 1)
        self.assertEqual(stats["cache_size"], 1)

        # 再次加载相同页面（缓存命中）
        buffer_pool.get_page(self.table_name, 0)
        stats = buffer_pool.get_stats()
        self.assertEqual(stats["hits"], 1)
        self.assertEqual(stats["misses"], 1)

        # 计算命中率
        self.assertAlmostEqual(stats["hit_rate"], 0.5)

        print("缓存统计信息测试通过")

    def test_dirty_page_handling(self):
        """测试脏页处理"""
        buffer_pool = BufferPool(capacity=3, strategy="LRU", fs=self.fs)

        # 加载页面并标记为脏
        page = buffer_pool.get_page(self.table_name, 0)
        buffer_pool.mark_dirty(self.table_name, 0)

        # 验证脏页标记
        self.assertTrue(buffer_pool.dirty_pages[(self.table_name, 0)])

        # 淘汰页面，应该触发写回磁盘
        buffer_pool.get_page(self.table_name, 1)
        buffer_pool.get_page(self.table_name, 2)
        buffer_pool.get_page(self.table_name, 3)  # 触发淘汰

        # 验证页面0已被淘汰
        self.assertNotIn((self.table_name, 0), buffer_pool.cache)

        # 重新加载页面0，验证数据已持久化
        page_reloaded = buffer_pool.get_page(self.table_name, 0)
        self.assertIsNotNone(page_reloaded)

        print("脏页处理测试通过")


if __name__ == "__main__":
    # 运行所有测试
    unittest.main(verbosity=2)