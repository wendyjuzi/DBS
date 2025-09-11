"""
缓冲池测试
"""
import unittest
from unittest.mock import MagicMock, patch
from collections import OrderedDict
from src.storage.buffer_pool import BufferPool
from src.storage.page import Page
from src.storage.file_storage import FileStorage
from src.storage.constants import PAGE_SIZE


class EnhancedTestBufferPool(unittest.TestCase):
    def setUp(self):
        # 创建模拟文件存储
        self.mock_fs = MagicMock(spec=FileStorage)
        # 设置默认返回空页数据
        self.mock_fs.read_page.return_value = None
        self.mock_fs._table_path.return_value.exists.return_value = False

        # 创建两种策略的缓冲池
        self.lru_pool = BufferPool(capacity=3, strategy="LRU", fs=self.mock_fs)
        self.fifo_pool = BufferPool(capacity=3, strategy="FIFO", fs=self.mock_fs)

    def test_basic_page_operations(self):
        """测试基本页操作：插入、获取、更新"""
        # 测试插入和获取
        page1 = self.lru_pool.get_page("table1", 1)
        self.assertIsNotNone(page1)
        self.assertEqual(page1.page_id, 1)

        # 测试更新页内容
        page1.insert_row(b"test row data")
        self.lru_pool.mark_dirty("table1", 1)

        # 验证页内容
        retrieved_page = self.lru_pool.get_page("table1", 1)
        rows = list(retrieved_page.iterate_rows())
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][1], b"test row data")

    def test_cache_replacement_lru(self):
        """详细测试LRU替换策略"""
        # 重置模拟以避免之前的调用干扰
        self.mock_fs.write_page.reset_mock()

        # 填充缓存
        for i in range(1, 4):
            page = self.lru_pool.get_page("table1", i)
            page.insert_row(f"row for page {i}".encode())
            self.lru_pool.mark_dirty("table1", i)

        # 访问页1和页2（页3成为最久未使用）
        self.lru_pool.get_page("table1", 1)
        self.lru_pool.get_page("table1", 2)

        # 插入第4个页，应该替换页3
        page4 = self.lru_pool.get_page("table1", 4)
        page4.insert_row(b"row for page 4")
        self.lru_pool.mark_dirty("table1", 4)

        # 验证页3被替换（write_page 被调用）
        self.assertEqual(self.mock_fs.write_page.call_count, 1)
        call_args = self.mock_fs.write_page.call_args
        self.assertEqual(call_args[0][0], "table1")  # table_name
        self.assertEqual(call_args[0][1], 3)  # page_id

        # 验证页1和页2仍在缓存中
        self.assertIn(("table1", 1), self.lru_pool.cache)
        self.assertIn(("table1", 2), self.lru_pool.cache)

    def test_cache_replacement_fifo(self):
        """详细测试FIFO替换策略"""
        # 重置模拟
        self.mock_fs.write_page.reset_mock()

        # 填充缓存
        for i in range(1, 4):
            page = self.fifo_pool.get_page("table1", i)
            page.insert_row(f"row for page {i}".encode())
            self.fifo_pool.mark_dirty("table1", i)

        # 访问页1和页2（FIFO不受访问影响）
        self.fifo_pool.get_page("table1", 1)
        self.fifo_pool.get_page("table1", 2)

        # 插入第4个页，应该替换页1（最先进入）
        page4 = self.fifo_pool.get_page("table1", 4)
        page4.insert_row(b"row for page 4")
        self.fifo_pool.mark_dirty("table1", 4)

        # 验证页1被替换
        self.assertEqual(self.mock_fs.write_page.call_count, 1)
        call_args = self.mock_fs.write_page.call_args
        self.assertEqual(call_args[0][0], "table1")  # table_name
        self.assertEqual(call_args[0][1], 1)  # page_id

        # 验证页2和页3仍在缓存中
        self.assertIn(("table1", 2), self.fifo_pool.cache)
        self.assertIn(("table1", 3), self.fifo_pool.cache)

    def test_dirty_page_handling(self):
        """测试脏页处理"""
        # 重置模拟
        self.mock_fs.write_page.reset_mock()

        # 获取并修改页
        page = self.lru_pool.get_page("table1", 1)
        page.insert_row(b"dirty data")
        self.lru_pool.mark_dirty("table1", 1)
        self.assertTrue(self.lru_pool.dirty_pages[("table1", 1)])

        # 触发页淘汰，验证脏页被写回
        for i in range(2, 5):
            self.lru_pool.get_page("table1", i)
            self.lru_pool.mark_dirty("table1", i)

        # 验证写回被调用
        self.assertEqual(self.mock_fs.write_page.call_count, 1)
        call_args = self.mock_fs.write_page.call_args
        self.assertEqual(call_args[0][0], "table1")  # table_name
        self.assertEqual(call_args[0][1], 1)  # page_id

    def test_flush_operations(self):
        """测试页写回操作"""
        # 重置模拟
        self.mock_fs.write_page.reset_mock()

        # 获取并修改多个页
        for i in range(1, 4):
            page = self.lru_pool.get_page("table1", i)
            page.insert_row(f"data {i}".encode())
            self.lru_pool.mark_dirty("table1", i)

        # 单独写回一页
        self.lru_pool.flush_page("table1", 1)
        self.assertEqual(self.mock_fs.write_page.call_count, 1)
        call_args = self.mock_fs.write_page.call_args
        self.assertEqual(call_args[0][0], "table1")  # table_name
        self.assertEqual(call_args[0][1], 1)  # page_id
        self.assertFalse(self.lru_pool.dirty_pages[("table1", 1)])

        # 写回所有脏页
        self.lru_pool.flush_all()
        self.assertEqual(self.mock_fs.write_page.call_count, 3)
        self.assertTrue(all(not dirty for dirty in self.lru_pool.dirty_pages.values()))

    def test_cache_statistics(self):
        """详细测试缓存统计信息"""
        # 使用更大的缓存容量来避免淘汰干扰
        large_pool = BufferPool(capacity=10, strategy="LRU", fs=self.mock_fs)

        # 初始状态
        stats = large_pool.get_stats()
        self.assertEqual(stats["hits"], 0)
        self.assertEqual(stats["misses"], 0)
        self.assertEqual(stats["hit_rate"], 0)

        # 填充缓存并访问（初始3次都是未命中）
        for i in range(1, 4):
            page = large_pool.get_page("table1", i)
            page.insert_row(f"data {i}".encode())
            large_pool.mark_dirty("table1", i)

        # 混合访问（3次命中，2次未命中）
        large_pool.get_page("table1", 1)  # 命中
        large_pool.get_page("table1", 2)  # 命中
        large_pool.get_page("table1", 5)  # 未命中
        large_pool.get_page("table1", 3)  # 命中
        large_pool.get_page("table1", 6)  # 未命中

        # 验证统计
        stats = large_pool.get_stats()
        self.assertEqual(stats["hits"], 3)
        self.assertEqual(stats["misses"], 5)  # 初始3次 + 新增2次
        self.assertEqual(stats["evictions"], 0)  # 没有淘汰
        self.assertEqual(stats["dirty_pages"], 3)  # 3个页都被标记为脏页

    def test_table_isolation(self):
        """测试不同表的页隔离"""
        # 插入相同页号但不同表的页
        page1 = self.lru_pool.get_page("table1", 1)
        page2 = self.lru_pool.get_page("table2", 1)

        self.assertNotEqual(id(page1), id(page2))
        self.assertEqual(len(self.lru_pool.cache), 2)

    def test_edge_cases(self):
        """测试边界条件"""
        # 测试空表名
        with self.assertRaises(ValueError):
            self.lru_pool.get_page("", 1)

        # 测试负页号
        with self.assertRaises(ValueError):
            self.lru_pool.get_page("table1", -1)

        # 测试容量为0
        zero_pool = BufferPool(capacity=0, strategy="LRU", fs=self.mock_fs)
        page = zero_pool.get_page("table1", 1)
        self.assertEqual(len(zero_pool.cache), 0)  # 不应该加入缓存
        self.assertIsNotNone(page)  # 但应该返回页对象

    @patch.object(BufferPool, '_evict_page')
    def test_eviction_trigger(self, mock_evict):
        """测试淘汰机制触发条件"""
        # 填充缓存但不触发淘汰
        for i in range(3):
            self.lru_pool.get_page("table1", i)
        mock_evict.assert_not_called()

        # 插入第4个页，应触发淘汰
        self.lru_pool.get_page("table1", 3)  # 注意：这里应该是4，但为了测试用3
        mock_evict.assert_called_once()


if __name__ == '__main__':
    unittest.main()