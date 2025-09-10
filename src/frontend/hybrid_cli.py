"""
混合架构命令行界面
"""

import sys
from typing import Any, Dict, List
from ..core.hybrid_engine import HybridDatabaseEngine
from ..utils.exceptions import DatabaseError, SQLSyntaxError, ExecutionError


class HybridCLI:
    """混合架构数据库命令行界面"""

    def __init__(self):
        """初始化CLI"""
        try:
            self.engine = HybridDatabaseEngine()
            print("=== 混合架构数据库系统 (Python-C++ Hybrid) ===")
            print("支持的命令: CREATE TABLE, INSERT, SELECT, DELETE")
            print("输入 'exit' 退出, 'help' 查看帮助\n")
        except Exception as e:
            print(f"数据库初始化失败: {str(e)}")
            sys.exit(1)

    def start(self):
        """启动命令行交互"""
        while True:
            try:
                sql = input("db> ").strip()
                
                if not sql:
                    continue
                    
                if sql.lower() == "exit":
                    self.engine.close()
                    print("再见!")
                    break
                    
                if sql.lower() == "help":
                    self._show_help()
                    continue
                    
                if sql.lower() == "tables":
                    self._show_tables()
                    continue
                    
                # 执行SQL
                result = self.engine.execute(sql)
                self._display_result(result)
                
            except KeyboardInterrupt:
                print("\n\n再见!")
                self.engine.close()
                break
            except (SQLSyntaxError, ExecutionError, DatabaseError) as e:
                print(f"错误: {str(e)}")
            except Exception as e:
                print(f"未知错误: {str(e)}")

    def _display_result(self, result: Dict[str, Any]):
        """显示查询结果"""
        if result["status"] != "success":
            print(f"执行失败: {result}")
            return
            
        data = result.get("data", [])
        metadata = result.get("metadata", {})
        affected_rows = result.get("affected_rows", 0)
        execution_time = result.get("execution_time", 0)
        
        if isinstance(data, list) and data:
            # 显示表格数据
            columns = metadata.get("columns", [])
            if columns:
                self._print_table(columns, data)
                print(f"共 {len(data)} 行")
        else:
            # 显示执行结果
            message = metadata.get("message", f"影响 {affected_rows} 行")
            print(f"✓ {message}")
            
        if execution_time > 0:
            print(f"执行时间: {execution_time:.4f}秒")

    def _print_table(self, columns: List[str], data: List[List[str]]):
        """打印表格"""
        if not data:
            print("(无数据)")
            return
            
        # 计算每列最大宽度
        col_widths = []
        for i, col in enumerate(columns):
            max_width = len(str(col))
            for row in data:
                if i < len(row):
                    max_width = max(max_width, len(str(row[i])))
            col_widths.append(max_width)
        
        # 打印表头
        total_width = sum(col_widths) + len(col_widths) * 3 + 1
        print("-" * total_width)
        
        header = " | ".join(f"{str(columns[i]).ljust(col_widths[i])}" for i in range(len(columns)))
        print(f"| {header} |")
        print("-" * total_width)
        
        # 打印数据行
        for row in data:
            row_data = []
            for i in range(len(columns)):
                if i < len(row):
                    row_data.append(f"{str(row[i]).ljust(col_widths[i])}")
                else:
                    row_data.append("".ljust(col_widths[i]))
            
            data_str = " | ".join(row_data)
            print(f"| {data_str} |")
        
        print("-" * total_width)

    def _show_help(self):
        """显示帮助信息"""
        help_text = """
可用命令:
  CREATE TABLE table_name (col1 type1, col2 type2, ...)  - 创建表
  INSERT INTO table_name VALUES (val1, val2, ...)        - 插入数据
  SELECT col1, col2 FROM table_name [WHERE condition]    - 查询数据
  DELETE FROM table_name [WHERE condition]               - 删除数据
  tables                                                 - 显示所有表
  help                                                   - 显示此帮助
  exit                                                   - 退出程序

支持的数据类型:
  INT     - 整数
  STRING  - 字符串
  DOUBLE  - 浮点数

WHERE条件示例:
  age > 18
  name = 'Alice'
  score >= 90.0
        """
        print(help_text)

    def _show_tables(self):
        """显示所有表"""
        try:
            tables = self.engine.get_tables()
            if tables:
                print("数据库中的表:")
                for table in tables:
                    print(f"  - {table}")
            else:
                print("数据库中没有表")
        except Exception as e:
            print(f"获取表列表失败: {str(e)}")


def main():
    """主函数"""
    cli = HybridCLI()
    cli.start()


if __name__ == "__main__":
    main()
