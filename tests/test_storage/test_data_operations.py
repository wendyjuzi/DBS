"""
测试数据操作、页管理、缓存效果和统计信息的测试文件
python tests/test_storage/test_data_operations.py
"""

import unittest
import tempfile
import os
import shutil
import time
import gc
from pathlib import Path
import sys

# 添加项目根目录到Python路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.storage.buffer_pool import BufferPool
from src.storage.file_storage import FileStorage
from src.storage.engine import StorageEngine
from src.storage.constants import PAGE_SIZE


class TestDataOperations(unittest.TestCase):
    """测试数据操作、页管理、缓存效果和统计信息"""

    def setUp(self):
        """设置测试环境"""
        self.test_dir = tempfile.mkdtemp()
        self.fs = FileStorage(self.test_dir)

        # 创建测试表
        self.table_name = "user_data"
        self.engine = StorageEngine(
            base_dir=self.test_dir,
            cache_capacity=10,  # 小容量便于测试替换策略
            cache_strategy="LRU",
            enable_wal=True,
            enable_readahead=False  # 关闭预读以简化测试
        )

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

    def test_data_operations(self):
        """测试基本数据操作：插入、查询、删除"""
        # 1. 插入数据
        test_data = [
            b"user_1_data",
            b"user_2_data",
            b"user_3_data",
            b"user_4_data"
        ]

        # 记录插入结果用于验证
        inserted_records = []

        # 开始事务
        txid = self.engine.begin_transaction()
        self.assertIsNotNone(txid)

        # 插入数据
        for data in test_data:
            page_id, slot_idx, offset = self.engine.append_row(self.table_name, data)
            inserted_records.append((page_id, slot_idx, data))
            print(f"插入数据: 页={page_id}, 槽={slot_idx}, 数据={data}")

        # 提交事务
        self.assertTrue(self.engine.commit_transaction())

        # 2. 验证数据查询
        # 扫描所有行并验证
        all_rows = list(self.engine.scan_rows(self.table_name))
        self.assertGreaterEqual(len(all_rows), len(test_data))

        # 验证插入的数据都存在
        for _, _, data in inserted_records:
            found = False
            for _, _, row_data in all_rows:
                if row_data == data:
                    found = True
                    break
            self.assertTrue(found, f"数据 {data} 未找到")

        # 3. 测试删除操作
        # 删除第二条记录
        target_record = inserted_records[1]
        self.engine.delete_row(self.table_name, target_record[0], target_record[1])

        # 验证删除后的数据
        all_rows_after_delete = list(self.engine.scan_rows(self.table_name))
        found = False
        for _, _, row_data in all_rows_after_delete:
            if row_data == target_record[2]:
                found = True
                break
        self.assertFalse(found, "删除的数据仍然存在")

        print("基本数据操作测试通过")

    def test_page_allocation(self):
        """测试页分配与释放"""
        # 初始页数
        initial_pages = self.fs.page_count(self.table_name)

        # 分配新页
        new_page_id = self.engine.allocate_page(self.table_name)
        self.assertEqual(new_page_id, initial_pages)  # 第一个分配的页应该是0

        # 验证页数增加
        self.assertEqual(self.fs.page_count(self.table_name), initial_pages + 1)

        # 释放页
        self.engine.free_page(self.table_name, new_page_id)

        # 再次分配应该重用释放的页
        reused_page_id = self.engine.allocate_page(self.table_name)
        self.assertEqual(reused_page_id, new_page_id)

        # 验证统计信息
        stats = self.engine.get_cache_stats()
        self.assertGreaterEqual(stats["evictions"], 0)

        print("页分配与释放测试通过")

    def test_cache_efficiency(self):
        """测试缓存命中率与替换策略效果"""
        # 重置缓存统计
        self.engine.buffer_pool.clear()

        # 创建足够大的测试数据（每条1KB）
        data_size = 1024  # 1KB
        test_data = [b'x' * data_size for _ in range(50)]  # 50条1KB数据

        # 插入数据并记录分配的页
        allocated_pages = set()
        for data in test_data:
            page_id, _, _ = self.engine.append_row(self.table_name, data)
            allocated_pages.add(page_id)
            print(f"数据插入到页 {page_id}")

        # 验证分配的页数
        print(f"分配的页数: {len(allocated_pages)}")
        self.assertGreater(len(allocated_pages), 5, "应分配超过5页")

        # 第一次访问 - 应该都是缓存未命中
        for page_id in sorted(allocated_pages)[:10]:  # 访问前10页
            self.engine.get_page(self.table_name, page_id)

        # 第二次访问相同页 - 应该都是缓存命中
        for page_id in sorted(allocated_pages)[:10]:
            self.engine.get_page(self.table_name, page_id)

        # 访问另外5个不同的页 - 应该触发替换
        if len(allocated_pages) > 10:
            for page_id in sorted(allocated_pages)[10:15]:
                self.engine.get_page(self.table_name, page_id)

        # 获取缓存统计
        stats = self.engine.get_cache_stats()
        print(f"\n缓存统计: {stats}")

        # 验证命中率
        self.assertGreater(stats["hits"], 0)
        self.assertGreater(stats["misses"], 0)
        self.assertGreater(stats["hit_rate"], 0)

        # 验证替换策略效果
        if self.engine.buffer_pool.strategy == "LRU":
            self.assertGreater(stats["evictions"], 0, "应发生页面淘汰")

        print("缓存命中率与替换策略效果测试通过")

    def test_logging_and_stats(self):
        """测试日志与统计信息输出"""
        # 启用详细日志
        import logging
        logging.basicConfig(level=logging.INFO)

        # 确保WAL目录存在
        wal_dir = os.path.join(self.test_dir, "wal")
        os.makedirs(wal_dir, exist_ok=True)

        # 在事务中执行操作
        txid = self.engine.begin_transaction()
        self.assertIsNotNone(txid)

        # 执行多个操作以确保生成WAL日志
        for i in range(3):
            self.engine.append_row(self.table_name, f"test_data_{i}".encode())

        # 提交事务以确保WAL日志被记录
        self.assertTrue(self.engine.commit_transaction())

        # 强制刷盘WAL日志
        if self.engine.wal:
            self.engine.wal.flush()

        # 获取页面操作也会产生缓存统计
        self.engine.get_page(self.table_name, 0)

        # 获取统计信息
        cache_stats = self.engine.get_cache_stats()
        wal_stats = self.engine.get_wal_status()

        # 验证WAL日志大小应该大于0
        self.assertGreater(wal_stats.get("wal_size", 0), 0, "WAL日志大小应该大于0")

        # 验证统计信息完整性
        self.assertIn("hits", cache_stats)
        self.assertIn("misses", cache_stats)
        self.assertIn("hit_rate", cache_stats)
        self.assertIn("enabled", wal_stats)

        # 打印统计信息
        print("\n缓存统计信息:")
        for k, v in cache_stats.items():
            print(f"{k}: {v}")

        print("\nWAL统计信息:")
        for k, v in wal_stats.items():
            print(f"{k}: {v}")

        print("日志与统计信息测试通过")


if __name__ == "__main__":
    # 运行所有测试
    unittest.main(verbosity=2)