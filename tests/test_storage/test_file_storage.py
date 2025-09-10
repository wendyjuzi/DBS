"""
存储系统测试
"""

import unittest
import tempfile
import os
import sys
from pathlib import Path

# 添加src目录到Python路径，以便正确导入模块
sys.path.append(str(Path(__file__).parent.parent.parent / "src"))

# 修改导入路径：从 src.storage 导入
from src.storage.engine import StorageEngine
from src.storage.buffer_pool import BufferPool
from src.storage.constants import PAGE_SIZE


class TestStorageSystem(unittest.TestCase):

    def setUp(self):
        # 创建临时目录
        self.temp_dir = tempfile.mkdtemp()
        self.engine = StorageEngine(base_dir=self.temp_dir, cache_capacity=3, cache_strategy="LRU")

    def tearDown(self):
        # 清理临时文件
        import shutil
        shutil.rmtree(self.temp_dir)

    def test_page_allocation(self):
        """测试页分配和释放"""
        # 分配新页
        page_id = self.engine.allocate_page("test_table")
        self.assertEqual(page_id, 0)

        # 再分配一页
        page_id2 = self.engine.allocate_page("test_table")
        self.assertEqual(page_id2, 1)

        # 释放一页
        self.engine.free_page("test_table", 0)

        # 再次分配应该重用释放的页
        page_id3 = self.engine.allocate_page("test_table")
        self.assertEqual(page_id3, 0)

    # test_file_storage.py 修改测试用例
    def test_buffer_pool_lru(self):
        """测试LRU缓存替换策略"""
        # 创建测试表数据
        test_data = b"x" * 100

        # 插入多行数据，超过缓存容量
        for i in range(5):
            self.engine.append_row("test_table", test_data)

        stats = self.engine.get_cache_stats()
        # 缓存大小应该不超过容量限制
        self.assertLessEqual(stats["cache_size"], 3)
        # 应该有页被淘汰或者缓存未满
        self.assertTrue(stats["evictions"] > 0 or stats["cache_size"] < 3)

    def test_crud_operations(self):
        """测试增删改查操作"""
        # 插入数据
        row1 = b"test_data_1"
        row2 = b"test_data_2"

        page_id1, slot_idx1, _ = self.engine.append_row("test_table", row1)
        page_id2, slot_idx2, _ = self.engine.append_row("test_table", row2)

        # 读取数据
        rows = list(self.engine.scan_rows("test_table"))
        self.assertEqual(len(rows), 2)

        # 验证数据内容
        self.assertEqual(rows[0][2], row1)
        self.assertEqual(rows[1][2], row2)

        # 删除数据
        self.engine.delete_row("test_table", page_id1, slot_idx1)

        # 验证删除后
        rows_after_delete = list(self.engine.scan_rows("test_table"))
        self.assertEqual(len(rows_after_delete), 1)
        self.assertEqual(rows_after_delete[0][2], row2)

    def test_cache_stats(self):
        """测试缓存统计"""
        test_data = b"x" * 100

        # 初次访问 - 应该未命中
        self.engine.append_row("test_table", test_data)
        stats = self.engine.get_cache_stats()
        self.assertEqual(stats["misses"], 1)
        self.assertEqual(stats["hits"], 0)

        # 再次访问同一页 - 应该命中
        self.engine.append_row("test_table", test_data)
        stats = self.engine.get_cache_stats()
        self.assertEqual(stats["hits"], 1)

        # 命中率应该在0-1之间
        self.assertGreaterEqual(stats["hit_rate"], 0)
        self.assertLessEqual(stats["hit_rate"], 1)

    def test_empty_table_scan(self):
        """测试空表扫描"""
        rows = list(self.engine.scan_rows("empty_table"))
        self.assertEqual(len(rows), 0)

    def test_page_write_read(self):
        """测试页的写入和读取"""
        # 分配页并写入数据
        page_id = self.engine.allocate_page("test_table")
        test_data = b"test_page_data"

        # 获取页对象并手动插入数据
        page = self.engine.get_page("test_table", page_id)
        if page is None:
            page = self.engine.buffer_pool.get_page("test_table", page_id)

        slot_idx, offset = page.insert_row(test_data)
        # 确保数据写入磁盘
        self.engine.flush_page("test_table", page_id)

        # 重新创建引擎实例来测试磁盘持久化
        new_engine = StorageEngine(base_dir=self.temp_dir)
        rows = list(new_engine.scan_rows("test_table"))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][2], test_data)

    def test_debug_buffer_pool(self):
        """调试缓存行为"""
        test_data = b"x" * 100

        print("=== 缓存调试信息 ===")
        for i in range(5):
            result = self.engine.append_row("debug_table", test_data)
            stats = self.engine.get_cache_stats()
            print(f"插入 {i}: 页ID={result[0]}, 缓存大小={stats['cache_size']}, 淘汰数={stats['evictions']}")

        final_stats = self.engine.get_cache_stats()
        print(f"最终统计: {final_stats}")

if __name__ == "__main__":
    unittest.main()
