"""
数据导出器 - 支持将表数据导出为CSV/JSON格式
"""

import csv
import json
import os
from typing import List, Dict, Any
from pathlib import Path

class DataExporter:
    """数据导出器，支持CSV和JSON格式导出"""

    def __init__(self, execution_engine):
        """
        初始化数据导出器

        Args:
            execution_engine: C++执行引擎实例
        """
        self.executor = execution_engine

    def export_table_to_csv(self, table_name: str, output_path: str) -> bool:
        """
        将表数据导出为CSV文件

        Args:
            table_name: 表名
            output_path: 输出文件路径

        Returns:
            bool: 导出是否成功
        """
        try:
            # 获取表数据
            table_data = self.executor.export_table_data(table_name)

            if not table_data or len(table_data) <= 1:  # 只有表头
                print(f"表 '{table_name}' 没有数据可导出")
                return False

            # 确保输出目录存在
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            # 写入CSV文件
            with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)

                # 写入所有数据（包括表头）
                for row in table_data:
                    writer.writerow(row)

            print(f"✓ 表 '{table_name}' 已导出到: {output_path}")
            print(f"  导出记录: {len(table_data) - 1} 行")  # 减去表头
            return True

        except Exception as e:
            print(f"❌ 导出失败: {str(e)}")
            return False

    def export_table_to_json(self, table_name: str, output_path: str) -> bool:
        """
        将表数据导出为JSON文件

        Args:
            table_name: 表名
            output_path: 输出文件路径

        Returns:
            bool: 导出是否成功
        """
        try:
            # 获取表数据
            table_data = self.executor.export_table_data(table_name)

            if not table_data or len(table_data) <= 1:  # 只有表头
                print(f"表 '{table_name}' 没有数据可导出")
                return False

            # 获取列名（表头）
            columns = table_data[0]
            data_rows = table_data[1:]

            # 构建导出数据结构
            export_data = {
                "table_name": table_name,
                "columns": columns,
                "data": [
                    dict(zip(columns, row)) for row in data_rows
                ]
            }

            # 确保输出目录存在
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            # 写入JSON文件
            with open(output_path, 'w', encoding='utf-8') as jsonfile:
                json.dump(export_data, jsonfile, ensure_ascii=False, indent=2)

            print(f"✓ 表 '{table_name}' 已导出到: {output_path}")
            print(f"  导出记录: {len(data_rows)} 行")
            return True

        except Exception as e:
            print(f"❌ 导出失败: {str(e)}")
            return False

    def get_export_formats(self) -> Dict[str, str]:
        """获取支持的导出格式"""
        return {
            "csv": "逗号分隔值文件",
            "json": "JSON数据文件"
        }