"""
文件系统测试
"""
import unittest
import tempfile
import shutil
import os
from src.storage.file_storage import FileStorage
from src.storage.page import Page
from src.storage.constants import PAGE_SIZE


class TestFileSystem(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.file_storage = FileStorage(base_dir=self.test_dir)
        self.table_name = "test_table"

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_file_creation(self):
        """测试文件创建"""
        page_id = 0
        # 创建一个空页
        page = Page(page_id)
        # 插入一行数据
        test_data = b'test file content'
        page.insert_row(test_data)
        page_bytes = page.to_bytes()

        # 写入文件
        self.file_storage.write_page(self.table_name, page_id, page_bytes)

        # 验证文件存在
        file_path = self.file_storage._table_path(self.table_name)
        self.assertTrue(file_path.exists())

        # 读取并验证内容
        read_data = self.file_storage.read_page(self.table_name, page_id)
        self.assertIsNotNone(read_data)
        # 可以用Page类反序列化验证
        read_page = Page(page_id, read_data)
        rows = list(read_page.iterate_rows())
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][1], test_data)

    def test_directory_creation(self):
        """测试目录自动创建"""
        # 使用不存在的目录
        new_dir = os.path.join(self.test_dir, 'new_subdir')
        file_storage = FileStorage(base_dir=new_dir)

        # 应该自动创建目录
        self.assertTrue(os.path.exists(new_dir))

    def test_large_data(self):
        """测试大数据量"""
        page_id = 1
        # 创建接近页大小的数据
        large_data = b'x' * 4000
        page = Page(page_id)
        page.insert_row(large_data)
        page_bytes = page.to_bytes()

        self.file_storage.write_page(self.table_name, page_id, page_bytes)

        # 读取验证
        read_data = self.file_storage.read_page(self.table_name, page_id)
        self.assertIsNotNone(read_data)
        read_page = Page(page_id, read_data)
        rows = list(read_page.iterate_rows())
        self.assertEqual(rows[0][1], large_data)

    def test_multiple_pages(self):
        """测试多页写入和读取"""
        # 创建多个页
        for i in range(10):
            page = Page(i)
            page.insert_row(f"content {i}".encode())
            page_bytes = page.to_bytes()
            self.file_storage.write_page(self.table_name, i, page_bytes)

        # 读取多个页
        for i in range(10):
            read_data = self.file_storage.read_page(self.table_name, i)
            self.assertIsNotNone(read_data)
            read_page = Page(i, read_data)
            rows = list(read_page.iterate_rows())
            self.assertEqual(rows[0][1], f"content {i}".encode())

    def test_file_overwrite(self):
        """测试文件覆盖写入"""
        page_id = 0
        # 首次写入
        page1 = Page(page_id)
        initial_data = b'initial content'
        page1.insert_row(initial_data)
        self.file_storage.write_page(self.table_name, page_id, page1.to_bytes())

        # 覆盖写入
        page2 = Page(page_id)
        updated_data = b'updated content'
        page2.insert_row(updated_data)
        self.file_storage.write_page(self.table_name, page_id, page2.to_bytes())

        # 验证内容已更新
        read_data = self.file_storage.read_page(self.table_name, page_id)
        read_page = Page(page_id, read_data)
        rows = list(read_page.iterate_rows())
        self.assertEqual(rows[0][1], updated_data)

    def test_read_nonexistent_page(self):
        """测试读取不存在的页"""
        non_existent_page_id = 999
        data = self.file_storage.read_page(self.table_name, non_existent_page_id)
        self.assertIsNone(data)

    def test_page_size_boundary(self):
        """测试页大小边界条件"""
        page_id = 2
        # 创建一个空页
        page = Page(page_id)
        # 计算最大可插入数据大小
        max_length = page._free_space() - Page.SLOT_SIZE
        # 测试刚好等于最大可插入大小的数据
        exact_size_data = b'x' * max_length
        page.insert_row(exact_size_data)
        page_bytes = page.to_bytes()
        self.assertEqual(len(page_bytes), PAGE_SIZE)

        # 写入文件
        self.file_storage.write_page(self.table_name, page_id, page_bytes)

        # 读取验证
        read_data = self.file_storage.read_page(self.table_name, page_id)
        self.assertIsNotNone(read_data)
        self.assertEqual(len(read_data), PAGE_SIZE)

    def test_page_count(self):
        """测试页计数"""
        # 初始应为0
        self.assertEqual(self.file_storage.page_count(self.table_name), 0)

        # 写入一页
        page = Page(0)
        page.insert_row(b'test')
        self.file_storage.write_page(self.table_name, 0, page.to_bytes())
        self.assertEqual(self.file_storage.page_count(self.table_name), 1)

        # 写入第二页
        page = Page(1)
        page.insert_row(b'test2')
        self.file_storage.write_page(self.table_name, 1, page.to_bytes())
        self.assertEqual(self.file_storage.page_count(self.table_name), 2)


if __name__ == '__main__':
    unittest.main()