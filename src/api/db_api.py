"""
数据库API接口层
基于页式存储引擎的统一访问接口
"""

import json
import logging
import pickle
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path
from dataclasses import dataclass

from src.storage.engine import StorageEngine
from src.storage.constants import PAGE_SIZE

# 定义列类型
COLUMN_TYPES = {
    'INT': int,
    'VARCHAR': str,
    'TEXT': str,
    'BOOLEAN': bool,
    'FLOAT': float
}


@dataclass
class ColumnDefinition:
    """列定义"""
    name: str
    type: str
    length: int = 0  # 对于VARCHAR类型有用


class DatabaseAPI:
    """基于页式存储引擎的数据库统一访问接口"""

    def __init__(self, data_dir: str = 'data', buffer_capacity: int = 100):
        # 确保数据目录存在
        Path(data_dir).mkdir(parents=True, exist_ok=True)

        self.storage_engine = StorageEngine(base_dir=data_dir, cache_capacity=buffer_capacity)
        self.catalog_file = Path(data_dir) / 'catalog.pkl'
        self.tables = self._load_catalog()
        self.logger = logging.getLogger(__name__)

    def _load_catalog(self) -> Dict:
        """加载系统目录"""
        if self.catalog_file.exists():
            try:
                with open(self.catalog_file, 'rb') as f:
                    return pickle.load(f)
            except (pickle.PickleError, IOError) as e:
                self.logger.error(f"加载目录失败: {e}")
                return {}
        return {}

    def _save_catalog(self):
        """保存系统目录"""
        try:
            with open(self.catalog_file, 'wb') as f:
                pickle.dump(self.tables, f)
        except IOError as e:
            self.logger.error(f"保存目录失败: {e}")

    def create_table(self, table_name: str, columns: List[Dict]) -> bool:
        """
        创建表
        :param table_name: 表名
        :param columns: 列定义列表 [{'name': 'id', 'type': 'INT'}, ...]
        :return: 是否成功
        """
        if table_name in self.tables:
            self.logger.warning(f"表 {table_name} 已存在")
            return False

        # 验证列定义
        validated_columns = []
        for col in columns:
            if col['type'] not in COLUMN_TYPES:
                self.logger.error(f"不支持的列类型: {col['type']}")
                return False
            validated_columns.append(ColumnDefinition(
                name=col['name'],
                type=col['type'],
                length=col.get('length', 0)
            ))

        # 创建表信息
        table_info = {
            'table_name': table_name,
            'columns': [col.__dict__ for col in validated_columns],
            'page_count': 0,
            'row_count': 0,
            'pages': []  # 页ID列表
        }

        self.tables[table_name] = table_info
        self._save_catalog()

        self.logger.info(f"创建表 {table_name} 成功")
        return True

    def _serialize_row(self, table_name: str, row_data: Dict) -> bytes:
        """序列化行数据为字节"""
        if table_name not in self.tables:
            raise ValueError(f"表 {table_name} 不存在")

        table_info = self.tables[table_name]
        serialized_data = {}

        for col_def in table_info['columns']:
            col_name = col_def['name']
            col_type = col_def['type']

            if col_name not in row_data:
                raise ValueError(f"缺少列 {col_name}")

            # 类型验证和转换
            value = row_data[col_name]
            expected_type = COLUMN_TYPES[col_type]

            if not isinstance(value, expected_type):
                try:
                    value = expected_type(value)
                except (ValueError, TypeError):
                    raise ValueError(f"列 {col_name} 类型不匹配，期望 {col_type}")

            serialized_data[col_name] = value

        return pickle.dumps(serialized_data)

    def _deserialize_row(self, table_name: str, row_bytes: bytes) -> Dict:
        """从字节反序列化行数据"""
        try:
            return pickle.loads(row_bytes)
        except pickle.PickleError as e:
            self.logger.error(f"反序列化行数据失败: {e}")
            return {}

    def insert_row(self, table_name: str, row_data: Dict) -> bool:
        """
        插入行数据 - 使用存储引擎的行级接口
        :param table_name: 表名
        :param row_data: 行数据 {'column1': value1, 'column2': value2}
        :return: 是否成功
        """
        if table_name not in self.tables:
            self.logger.error(f"表 {table_name} 不存在")
            return False

        try:
            # 序列化行数据
            row_bytes = self._serialize_row(table_name, row_data)

            # 使用存储引擎的append_row接口
            page_id, slot_idx, offset = self.storage_engine.append_row(table_name, row_bytes)

            # 更新目录信息
            table_info = self.tables[table_name]
            if page_id not in table_info['pages']:
                table_info['pages'].append(page_id)
                table_info['page_count'] += 1

            table_info['row_count'] += 1
            self._save_catalog()

            self.logger.info(f"向表 {table_name} 插入数据成功，页: {page_id}, 槽: {slot_idx}")
            return True

        except Exception as e:
            self.logger.error(f"插入数据失败: {e}")
            return False

    def scan_rows(self, table_name: str, limit: int = 100) -> List[Dict]:
        """
        扫描表数据 - 使用存储引擎的scan接口
        :param table_name: 表名
        :param limit: 限制返回行数
        :return: 行数据列表
        """
        if table_name not in self.tables:
            return []

        results = []
        count = 0

        # 使用存储引擎的scan_rows接口
        for page_id, slot_idx, row_bytes in self.storage_engine.scan_rows(table_name):
            if count >= limit:
                break

            row_data = self._deserialize_row(table_name, row_bytes)
            if row_data:
                results.append({
                    'page_id': page_id,
                    'slot_idx': slot_idx,
                    'data': row_data
                })
                count += 1

        return results

    def delete_row(self, table_name: str, page_id: int, slot_index: int) -> bool:
        """
        删除行数据
        :param table_name: 表名
        :param page_id: 页ID
        :param slot_index: 槽索引
        :return: 是否成功
        """
        if table_name not in self.tables:
            return False

        try:
            self.storage_engine.delete_row(table_name, page_id, slot_index)

            # 更新统计信息
            table_info = self.tables[table_name]
            table_info['row_count'] = max(0, table_info['row_count'] - 1)
            self._save_catalog()

            return True
        except Exception as e:
            self.logger.error(f"删除数据失败: {e}")
            return False

    def get_table_info(self, table_name: str) -> Optional[Dict]:
        """获取表信息"""
        return self.tables.get(table_name)

    def list_tables(self) -> List[str]:
        """列出所有表"""
        return list(self.tables.keys())

    def read_table_data(self, table_name: str, limit: int = 100) -> List[Dict]:
        """
        读取表数据（兼容旧接口）
        :param table_name: 表名
        :param limit: 限制返回行数
        :return: 行数据列表
        """
        rows = self.scan_rows(table_name, limit)
        return [row['data'] for row in rows]

    def get_storage_stats(self) -> Dict[str, Any]:
        """获取存储统计信息"""
        cache_stats = self.storage_engine.get_cache_stats()

        total_rows = sum(table['row_count'] for table in self.tables.values())
        total_pages = sum(table['page_count'] for table in self.tables.values())

        return {
            'table_count': len(self.tables),
            'total_rows': total_rows,
            'total_pages': total_pages,
            'cache_stats': cache_stats
        }

    def flush_all(self):
        """刷新所有数据到磁盘"""
        self.storage_engine.flush_all()
        self.logger.info("所有数据已刷新到磁盘")

    def shutdown(self):
        """关闭数据库"""
        self.flush_all()
        self.logger.info("数据库已关闭")


# 在文件末尾修改单例模式
_db_instances = {}  # 改为使用字典存储多个实例

def get_database(data_dir: str = 'data') -> DatabaseAPI:
    """获取数据库实例（按数据目录区分）"""
    global _db_instances
    if data_dir not in _db_instances:
        _db_instances[data_dir] = DatabaseAPI(data_dir)
    return _db_instances[data_dir]

def clear_database_instances():
    """清空所有数据库实例（用于测试）"""
    global _db_instances
    for instance in _db_instances.values():
        instance.shutdown()
    _db_instances = {}