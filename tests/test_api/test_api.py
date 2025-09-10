"""
API接口测试
"""

import unittest
import tempfile
import os
import sys
from pathlib import Path

# 添加src目录到Python路径
sys.path.append(str(Path(__file__).parent.parent / "src"))

from src.api.db_api import DatabaseAPI

class TestDatabaseAPI(unittest.TestCase):

    def setUp(self):
        # 创建临时目录
        self.temp_dir = tempfile.mkdtemp()
        self.db = DatabaseAPI(data_dir=self.temp_dir, buffer_capacity=10)

    def tearDown(self):
        # 清理临时文件
        import shutil
        self.db.shutdown()
        shutil.rmtree(self.temp_dir)

    def test_create_table(self):
        """测试创建表"""
        success = self.db.create_table('users', [
            {'name': 'id', 'type': 'INT'},
            {'name': 'name', 'type': 'VARCHAR', 'length': 50},
            {'name': 'age', 'type': 'INT'}
        ])
        self.assertTrue(success)

        # 验证表信息
        table_info = self.db.get_table_info('users')
        self.assertIsNotNone(table_info)
        self.assertEqual(table_info['table_name'], 'users')
        self.assertEqual(len(table_info['columns']), 3)

    def test_insert_and_read_data(self):
        """测试插入和读取数据"""
        # 先创建表
        self.db.create_table('users', [
            {'name': 'id', 'type': 'INT'},
            {'name': 'name', 'type': 'VARCHAR'},
            {'name': 'age', 'type': 'INT'}
        ])

        # 插入数据
        success = self.db.insert_row('users', {'id': 1, 'name': 'Alice', 'age': 25})
        self.assertTrue(success)

        success = self.db.insert_row('users', {'id': 2, 'name': 'Bob', 'age': 30})
        self.assertTrue(success)

        # 读取数据
        rows = self.db.read_table_data('users')
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]['id'], 1)
        self.assertEqual(rows[1]['name'], 'Bob')

    def test_scan_rows(self):
        """测试扫描行"""
        self.db.create_table('test_table', [
            {'name': 'id', 'type': 'INT'},
            {'name': 'value', 'type': 'VARCHAR'}
        ])

        # 插入多条数据
        for i in range(5):
            self.db.insert_row('test_table', {'id': i, 'value': f'test_{i}'})

        # 扫描数据
        rows = self.db.scan_rows('test_table', limit=3)
        self.assertEqual(len(rows), 3)
        self.assertIn('page_id', rows[0])
        self.assertIn('slot_idx', rows[0])
        self.assertIn('data', rows[0])

    def test_list_tables(self):
        """测试列出表"""
        self.db.create_table('table1', [{'name': 'id', 'type': 'INT'}])
        self.db.create_table('table2', [{'name': 'id', 'type': 'INT'}])

        tables = self.db.list_tables()
        self.assertEqual(len(tables), 2)
        self.assertIn('table1', tables)
        self.assertIn('table2', tables)

    def test_delete_row(self):
        """测试删除行"""
        self.db.create_table('test_table', [
            {'name': 'id', 'type': 'INT'},
            {'name': 'name', 'type': 'VARCHAR'}
        ])

        # 插入数据
        self.db.insert_row('test_table', {'id': 1, 'name': 'Alice'})

        # 扫描获取行位置信息
        rows = self.db.scan_rows('test_table')
        self.assertEqual(len(rows), 1)

        # 删除行
        success = self.db.delete_row('test_table', rows[0]['page_id'], rows[0]['slot_idx'])
        self.assertTrue(success)

        # 验证删除后
        rows_after = self.db.scan_rows('test_table')
        self.assertEqual(len(rows_after), 0)

    def test_get_stats(self):
        """测试获取统计信息"""
        self.db.create_table('stats_test', [{'name': 'id', 'type': 'INT'}])
        self.db.insert_row('stats_test', {'id': 1})

        stats = self.db.get_storage_stats()
        self.assertEqual(stats['table_count'], 1)
        self.assertEqual(stats['total_rows'], 1)
        self.assertIn('cache_stats', stats)

if __name__ == '__main__':
    unittest.main()