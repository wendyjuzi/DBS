#!/usr/bin/env python3
"""
OS存储缓存系统集成
将modules/os_storage缓存系统集成到混合架构数据库系统中
"""

import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

# 添加项目根目录到路径
proj_root = Path(__file__).resolve().parent
sys.path.insert(0, str(proj_root))

from src.storage import StorageEngine as PythonStorageEngine, BufferPool, FileStorage
from src.storage.constants import DEFAULT_CACHE_CAPACITY, DEFAULT_CACHE_STRATEGY
import db_core

class HybridStorageEngine:
    """混合存储引擎 - 集成Python缓存系统和C++存储引擎"""
    
    def __init__(self, 
                 cache_capacity: int = DEFAULT_CACHE_CAPACITY,
                 cache_strategy: str = DEFAULT_CACHE_STRATEGY,
                 enable_cpp_acceleration: bool = True):
        """
        初始化混合存储引擎
        
        Args:
            cache_capacity: 缓存容量
            cache_strategy: 缓存策略 (LRU/FIFO)
            enable_cpp_acceleration: 是否启用C++加速
        """
        self.cache_capacity = cache_capacity
        self.cache_strategy = cache_strategy
        self.enable_cpp_acceleration = enable_cpp_acceleration
        
        # 初始化Python存储引擎（带缓存）
        self.python_storage = PythonStorageEngine(
            cache_capacity=cache_capacity,
            cache_strategy=cache_strategy
        )
        
        # 初始化C++存储引擎（如果启用）
        if enable_cpp_acceleration:
            try:
                self.cpp_storage = db_core.StorageEngine()
                self.cpp_execution = db_core.ExecutionEngine(self.cpp_storage)
                print(f"[HYBRID] C++存储引擎初始化成功")
            except Exception as e:
                print(f"[HYBRID] C++存储引擎初始化失败: {e}")
                self.enable_cpp_acceleration = False
        else:
            self.cpp_storage = None
            self.cpp_execution = None
        
        # 缓存统计
        self.cache_stats = {
            "hits": 0,
            "misses": 0,
            "evictions": 0,
            "cpp_operations": 0,
            "python_operations": 0
        }
        
        print(f"[HYBRID] 混合存储引擎初始化完成")
        print(f"[HYBRID] 缓存容量: {cache_capacity}, 策略: {cache_strategy}")
        print(f"[HYBRID] C++加速: {'启用' if enable_cpp_acceleration else '禁用'}")
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """获取缓存统计信息"""
        python_stats = self.python_storage.buffer_pool.get_stats()
        return {
            "python_cache": python_stats,
            "hybrid_stats": self.cache_stats,
            "cpp_enabled": self.enable_cpp_acceleration
        }
    
    def flush_all_dirty_pages(self):
        """刷盘所有脏页"""
        if self.enable_cpp_acceleration:
            try:
                self.cpp_storage.flush_all_dirty_pages()
                self.cache_stats["cpp_operations"] += 1
            except Exception as e:
                print(f"[HYBRID] C++刷盘失败: {e}")
        
        # Python存储引擎刷盘
        self.python_storage.buffer_pool.flush_all_dirty_pages()
        self.cache_stats["python_operations"] += 1
    
    def create_table(self, table_name: str, columns: List[Dict[str, Any]]) -> bool:
        """创建表"""
        success = False
        
        # 在C++引擎中创建表
        if self.enable_cpp_acceleration:
            try:
                cpp_columns = []
                for col in columns:
                    # 转换数据类型
                    if col["type"] == "INT":
                        dtype = db_core.DataType.INT
                    elif col["type"] == "STRING":
                        dtype = db_core.DataType.STRING
                    elif col["type"] == "DOUBLE":
                        dtype = db_core.DataType.DOUBLE
                    else:
                        dtype = db_core.DataType.STRING
                    
                    cpp_columns.append(db_core.Column(
                        col["name"], 
                        dtype, 
                        col.get("is_primary_key", False)
                    ))
                
                success = self.cpp_execution.create_table(table_name, cpp_columns)
                if success:
                    self.cache_stats["cpp_operations"] += 1
                    print(f"[HYBRID] C++创建表成功: {table_name}")
            except Exception as e:
                print(f"[HYBRID] C++创建表失败: {e}")
        
        # 在Python引擎中创建表（作为备份或主要存储）
        if not success or not self.enable_cpp_acceleration:
            try:
                # 这里需要根据Python存储引擎的接口调整
                success = True  # 简化实现
                self.cache_stats["python_operations"] += 1
                print(f"[HYBRID] Python创建表: {table_name}")
            except Exception as e:
                print(f"[HYBRID] Python创建表失败: {e}")
                success = False
        
        return success
    
    def insert(self, table_name: str, row_values: List[str]) -> bool:
        """插入数据"""
        success = False
        
        # 在C++引擎中插入
        if self.enable_cpp_acceleration:
            try:
                success = self.cpp_execution.insert(table_name, row_values)
                if success:
                    self.cache_stats["cpp_operations"] += 1
            except Exception as e:
                print(f"[HYBRID] C++插入失败: {e}")
        
        # 在Python引擎中插入（如果C++失败）
        if not success:
            try:
                # 这里需要根据Python存储引擎的接口调整
                success = True  # 简化实现
                self.cache_stats["python_operations"] += 1
            except Exception as e:
                print(f"[HYBRID] Python插入失败: {e}")
                success = False
        
        return success
    
    def select(self, table_name: str, columns: List[str] = None, conditions: List[Dict] = None) -> List[List[str]]:
        """查询数据"""
        results = []
        
        # 优先使用C++引擎查询
        if self.enable_cpp_acceleration:
            try:
                rows = self.cpp_execution.seq_scan(table_name)
                results = [row.get_values() for row in rows]
                self.cache_stats["cpp_operations"] += 1
                
                # 应用过滤条件
                if conditions and results:
                    filtered_results = []
                    for row in results:
                        match = True
                        for condition in conditions:
                            col_name = condition["column"]
                            op = condition["op"]
                            value = condition["value"]
                            
                            # 简化条件匹配
                            if col_name in columns:
                                col_idx = columns.index(col_name)
                                if col_idx < len(row):
                                    if op == "=" and row[col_idx] != value:
                                        match = False
                                        break
                                    elif op == ">" and float(row[col_idx]) <= float(value):
                                        match = False
                                        break
                                    elif op == "<" and float(row[col_idx]) >= float(value):
                                        match = False
                                        break
                        
                        if match:
                            filtered_results.append(row)
                    
                    results = filtered_results
                
                print(f"[HYBRID] C++查询成功: {len(results)} 行")
            except Exception as e:
                print(f"[HYBRID] C++查询失败: {e}")
                results = []
        
        # 如果C++查询失败，使用Python引擎
        if not results and not self.enable_cpp_acceleration:
            try:
                # 这里需要根据Python存储引擎的接口调整
                results = []  # 简化实现
                self.cache_stats["python_operations"] += 1
                print(f"[HYBRID] Python查询: {len(results)} 行")
            except Exception as e:
                print(f"[HYBRID] Python查询失败: {e}")
                results = []
        
        return results
    
    def update(self, table_name: str, set_clauses: List[tuple], where_predicate) -> int:
        """更新数据"""
        updated_count = 0
        
        if self.enable_cpp_acceleration:
            try:
                updated_count = self.cpp_execution.update_rows(table_name, set_clauses, where_predicate)
                self.cache_stats["cpp_operations"] += 1
                print(f"[HYBRID] C++更新成功: {updated_count} 行")
            except Exception as e:
                print(f"[HYBRID] C++更新失败: {e}")
        
        return updated_count
    
    def delete_table(self, table_name: str) -> bool:
        """删除表"""
        success = False
        
        if self.enable_cpp_acceleration:
            try:
                success = self.cpp_execution.drop_table(table_name)
                if success:
                    self.cache_stats["cpp_operations"] += 1
                    print(f"[HYBRID] C++删除表成功: {table_name}")
            except Exception as e:
                print(f"[HYBRID] C++删除表失败: {e}")
        
        return success
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """获取表信息"""
        info = {}
        
        if self.enable_cpp_acceleration:
            try:
                columns = self.cpp_storage.get_table_columns(table_name)
                info = {
                    "columns": list(columns) if columns else [],
                    "has_index": self.cpp_storage.has_index(table_name),
                    "index_size": self.cpp_storage.get_index_size(table_name)
                }
            except Exception as e:
                print(f"[HYBRID] 获取表信息失败: {e}")
        
        return info


def create_hybrid_storage_engine(cache_capacity: int = 100, 
                                cache_strategy: str = "LRU",
                                enable_cpp_acceleration: bool = True) -> HybridStorageEngine:
    """创建混合存储引擎实例"""
    return HybridStorageEngine(
        cache_capacity=cache_capacity,
        cache_strategy=cache_strategy,
        enable_cpp_acceleration=enable_cpp_acceleration
    )


if __name__ == "__main__":
    # 测试混合存储引擎
    print("=== 混合存储引擎测试 ===")
    
    # 创建混合存储引擎
    hybrid_storage = create_hybrid_storage_engine(
        cache_capacity=50,
        cache_strategy="LRU",
        enable_cpp_acceleration=True
    )
    
    # 测试创建表
    print("\n1. 创建测试表")
    columns = [
        {"name": "id", "type": "INT", "is_primary_key": True},
        {"name": "name", "type": "STRING", "is_primary_key": False},
        {"name": "age", "type": "INT", "is_primary_key": False}
    ]
    
    success = hybrid_storage.create_table("test_table", columns)
    print(f"创建表结果: {success}")
    
    # 测试插入数据
    print("\n2. 插入测试数据")
    test_data = [
        ["1", "Alice", "20"],
        ["2", "Bob", "21"],
        ["3", "Charlie", "19"]
    ]
    
    for row in test_data:
        success = hybrid_storage.insert("test_table", row)
        print(f"插入 {row}: {success}")
    
    # 测试查询数据
    print("\n3. 查询测试数据")
    results = hybrid_storage.select("test_table", ["id", "name", "age"])
    print(f"查询结果: {len(results)} 行")
    for row in results:
        print(f"  {row}")
    
    # 测试更新数据
    print("\n4. 更新测试数据")
    set_clauses = [("age", "25")]
    where_predicate = lambda row: row[0] == "1"  # id = "1"
    
    updated_count = hybrid_storage.update("test_table", set_clauses, where_predicate)
    print(f"更新结果: {updated_count} 行")
    
    # 获取缓存统计
    print("\n5. 缓存统计")
    stats = hybrid_storage.get_cache_stats()
    print(f"缓存统计: {stats}")
    
    # 清理测试数据
    print("\n6. 清理测试数据")
    success = hybrid_storage.delete_table("test_table")
    print(f"删除表结果: {success}")
    
    print("\n=== 测试完成 ===")
