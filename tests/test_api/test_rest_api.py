"""
REST API接口测试
覆盖了以下功能：

创建表（test_create_table）

获取统计信息（test_get_stats）

健康检查（test_health_check）

数据插入和查询（test_insert_and_query_data）

简化版数据插入和查询（test_insert_and_query_simple_data）

空表列表查询（test_list_tables_empty）
"""

import unittest
import tempfile
import json
import shutil
from pathlib import Path
import sys

# 添加src目录到Python路径
sys.path.append(str(Path(__file__).parent.parent / "src"))

from src.api.rest_api import create_rest_app
from src.api.db_api import DatabaseAPI, clear_database_instances


class TestRestAPI(unittest.TestCase):

    def setUp(self):
        # 创建临时目录
        self.temp_dir = tempfile.mkdtemp()

        # 清空所有数据库实例
        clear_database_instances()

        # 创建测试应用，传入临时目录
        self.app = create_rest_app(self.temp_dir)
        self.app.config['TESTING'] = True
        self.client = self.app.test_client()

    def tearDown(self):
        # 清空所有数据库实例
        clear_database_instances()
        shutil.rmtree(self.temp_dir)

    def test_list_tables_empty(self):
        """测试空表列表"""
        response = self.client.get('/api/tables')
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertEqual(data['tables'], [])

    def test_create_table(self):
        """测试创建表API"""
        table_data = {
            'table_name': 'users',
            'columns': [
                {'name': 'id', 'type': 'INT'},
                {'name': 'name', 'type': 'VARCHAR', 'length': 50}
            ]
        }

        response = self.client.post('/api/tables',
                                    data=json.dumps(table_data),
                                    content_type='application/json')

        self.assertEqual(response.status_code, 201)
        data = json.loads(response.data)
        self.assertEqual(data['message'], '表创建成功')

        # 验证表确实创建了
        response = self.client.get('/api/tables')
        data = json.loads(response.data)
        self.assertIn('users', data['tables'])

    def test_insert_and_query_data(self):
        """测试插入和查询数据API"""
        # 先创建表
        table_data = {
            'table_name': 'test_table',
            'columns': [{'name': 'id', 'type': 'INT'}, {'name': 'name', 'type': 'VARCHAR'}]
        }
        response = self.client.post('/api/tables',
                                    data=json.dumps(table_data),
                                    content_type='application/json')
        self.assertEqual(response.status_code, 201)

        # 插入数据
        row_data = {'id': 1, 'name': 'Test User'}
        response = self.client.post('/api/tables/test_table/data',
                                    data=json.dumps(row_data),
                                    content_type='application/json')
        self.assertEqual(response.status_code, 201)

        # 查询数据
        response = self.client.get('/api/tables/test_table/data')
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)

        # 检查返回的数据
        self.assertEqual(len(data['data']), 1)
        row = data['data'][0]
        self.assertIn('data', row)
        self.assertEqual(row['data']['id'], 1)
        self.assertEqual(row['data']['name'], 'Test User')

    def test_insert_and_query_simple_data(self):
        """测试插入和查询数据（简化版本）"""
        # 先创建表
        table_data = {
            'table_name': 'simple_table',
            'columns': [{'name': 'id', 'type': 'INT'}, {'name': 'name', 'type': 'VARCHAR'}]
        }
        self.client.post('/api/tables',
                         data=json.dumps(table_data),
                         content_type='application/json')

        # 插入数据
        row_data = {'id': 1, 'name': 'Simple User'}
        response = self.client.post('/api/tables/simple_table/data',
                                    data=json.dumps(row_data),
                                    content_type='application/json')
        self.assertEqual(response.status_code, 201)

        # 使用read_table_data接口（返回纯数据）
        from src.api.db_api import get_database
        db = get_database(self.temp_dir)
        simple_data = db.read_table_data('simple_table')
        self.assertEqual(len(simple_data), 1)
        self.assertEqual(simple_data[0]['id'], 1)
        self.assertEqual(simple_data[0]['name'], 'Simple User')

    def test_health_check(self):
        """测试健康检查"""
        response = self.client.get('/api/health')
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertEqual(data['status'], 'healthy')

    def test_get_stats(self):
        """测试统计信息API"""
        response = self.client.get('/api/stats')
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertIn('table_count', data)
        self.assertIn('cache_stats', data)


if __name__ == '__main__':
    unittest.main()