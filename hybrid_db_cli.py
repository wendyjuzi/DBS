#!/usr/bin/env python3
"""
混合架构数据库系统 - 命令行界面
适配现有的SQL编译器适配器和执行引擎
"""

import sys
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

# 添加项目根目录到Python路径
proj_root = Path(__file__).resolve().parent
sys.path.insert(0, str(proj_root))

# 导入现有组件
from src.api.sql_compiler_adapter import SQLCompilerAdapter
from src.utils.exceptions import ExecutionError, SQLSyntaxError


class HybridDatabaseCLI:
    """混合架构数据库命令行界面 - 使用SQL编译器适配器"""

    def __init__(self):
        """初始化CLI"""
        try:
            print("正在初始化混合架构数据库系统...")
            self.adapter = SQLCompilerAdapter()
            print("=== 混合架构数据库系统 (SQL编译器 + C++执行引擎) ===")
            print("支持的命令: CREATE TABLE, INSERT, SELECT, DELETE, UPDATE")
            print("输入 'exit' 退出, 'help' 查看帮助, 'tables' 显示所有表")
            print("输入 'cache' 查看缓存统计, 'flushcache' 刷新缓存到磁盘")
            print("注意: 适配 modules/sql_compiler 的语法限制\n")
        except Exception as e:
            print(f"数据库初始化失败: {str(e)}")
            print("请确保:")
            print("1. C++模块已编译 (运行 scripts/run_final_demo.ps1)")
            print("2. 所有依赖文件存在")
            sys.exit(1)

    def start(self):
        """启动命令行交互"""
        print("数据库系统已就绪，可以开始输入SQL语句...\n")
        
        while True:
            try:
                # 支持多行SQL输入
                sql_lines = []
                while True:
                    line = input("db> " if not sql_lines else "  > ").strip()
                    
                    if not line and not sql_lines:
                        continue
                    
                    if line.lower() == "exit":
                        self._cleanup()
                        print("再见!")
                        return
                    
                    if line.lower() == "help":
                        self._show_help()
                        break
                    
                    if line.lower() == "tables":
                        self._show_tables()
                        break
                    
                    if line.lower() == "clear":
                        os.system('cls' if os.name == 'nt' else 'clear')
                        break
                    
                    if line.lower() == "flush":
                        self._flush_database()
                        break
                    
                    if line.lower() == "cache":
                        self._show_cache()
                        break

                    if line.lower() == "flushcache":
                        self._flush_cache()
                        break
                    
                    # 收集SQL行
                    sql_lines.append(line)
                    
                    # 检查是否以分号结尾（SQL语句结束）
                    if line.endswith(';'):
                        break
                
                # 如果有SQL语句，执行它
                if sql_lines:
                    sql = ' '.join(sql_lines)
                    self._execute_sql(sql)
                
            except KeyboardInterrupt:
                print("\n\n正在退出...")
                self._cleanup()
                break
            except Exception as e:
                print(f"系统错误: {str(e)}")

    def _execute_sql(self, sql: str):
        """执行SQL语句"""
        if not sql.strip():
            return
        
        print(f"执行: {sql}")
        print("-" * 60)
        
        try:
            start_time = time.time()
            result = self.adapter.execute(sql)
            execution_time = time.time() - start_time
            
            self._display_result(result, execution_time)
            
        except SQLSyntaxError as e:
            print(f" SQL语法错误: {str(e)}")
            self._show_syntax_help()
        except ExecutionError as e:
            print(f" 执行错误: {str(e)}")
        except Exception as e:
            print(f" 未知错误: {str(e)}")
        
        print("-" * 60)

    def _display_result(self, result: Dict[str, Any], execution_time: float):
        """显示查询结果"""
        if result.get("status") == "error":
            print(f" 执行失败: {result.get('error', '未知错误')}")
            return
        
        data = result.get("data", [])
        metadata = result.get("metadata", {})
        affected_rows = result.get("affected_rows", 0)
        
        # 显示数据
        if isinstance(data, list) and data:
            columns = metadata.get("columns", [])
            if columns:
                self._print_table(columns, data)
                print(f"✓ 查询完成，返回 {len(data)} 行")
            else:
                print("✓ 查询完成，但无列信息")
        else:
            # 显示操作结果
            message = metadata.get("message", f"操作完成，影响 {affected_rows} 行")
            print(f"✓ {message}")
        
        # 显示执行时间
        if execution_time > 0:
            print(f"⏱  执行时间: {execution_time:.4f}秒")

    def _print_table(self, columns: List[str], data: List[List[str]]):
        """打印表格"""
        if not data:
            print("(无数据)")
            return
        
        # 计算列宽
        col_widths = []
        for i, col in enumerate(columns):
            max_width = len(str(col))
            for row in data:
                if i < len(row):
                    max_width = max(max_width, len(str(row[i])))
            col_widths.append(max_width)
        
        # 打印表头
        total_width = sum(col_widths) + len(col_widths) * 3 + 1
        print("┌" + "─" * (total_width - 2) + "┐")
        
        header = " │ ".join(f"{str(columns[i]).ljust(col_widths[i])}" for i in range(len(columns)))
        print(f"│ {header} │")
        print("├" + "─" * (total_width - 2) + "┤")
        
        # 打印数据行
        for row in data:
            row_data = []
            for i in range(len(columns)):
                if i < len(row):
                    row_data.append(f"{str(row[i]).ljust(col_widths[i])}")
                else:
                    row_data.append("".ljust(col_widths[i]))
            
            data_str = " │ ".join(row_data)
            print(f"│ {data_str} │")
        
        print("└" + "─" * (total_width - 2) + "┘")

    def _show_help(self):
        """显示帮助信息"""
        help_text = """
📖 混合架构数据库系统帮助

🔧 系统命令:
  help       - 显示此帮助信息
  tables     - 显示所有表
  clear      - 清屏
  flush      - 刷盘数据到磁盘
  cache      - 显示缓存统计
  flushcache - 刷新缓存并刷盘
  exit       - 退出程序

📝 SQL语句支持:
  CREATE TABLE table_name (col1 type1, col2 type2, ...)  - 创建表
  INSERT INTO table_name VALUES (val1, val2, ...)        - 插入数据
  SELECT col1, col2 FROM table_name [WHERE condition]    - 查询数据
  DELETE FROM table_name [WHERE condition]               - 删除数据
  UPDATE table_name SET col1=val1 [WHERE condition]      - 更新数据
  DROP TABLE table_name                                  - 删除表
  SELECT ... FROM table1 JOIN table2 ON col1=col2        - 表连接
  SELECT ... FROM table ORDER BY col [ASC/DESC]          - 排序查询
  SELECT ... FROM table GROUP BY col                     - 分组查询

📊 支持的数据类型:
  INT     - 整数
  STRING  - 字符串
  DOUBLE  - 浮点数

⚠️  语法限制 (适配 modules/sql_compiler):
  - 不支持 PRIMARY KEY 语法
  - 不支持 * 通配符，必须指定具体列名
  - INSERT 必须指定列名: INSERT INTO table(col1, col2) VALUES (...)
  - WHERE 只支持简单条件: WHERE col = value
  - 不支持复杂条件如 AND, OR, NOT

💡 示例:
  CREATE TABLE students (id INT, name STRING, age INT, score DOUBLE);
  INSERT INTO students (id, name, age, score) VALUES (1, 'Alice', 20, 85.5);
  SELECT name, score FROM students WHERE age > 18;
  UPDATE students SET score = 90.0 WHERE id = 1;
  DELETE FROM students WHERE id = 1;
  DROP TABLE students;
  
  -- 高级查询示例:
  SELECT s.name, c.course FROM students s JOIN courses c ON s.id = c.student_id;
  SELECT name, score FROM students ORDER BY score DESC;
  SELECT age, COUNT(*) FROM students GROUP BY age;
        """
        print(help_text)

    def _show_syntax_help(self):
        """显示语法帮助"""
        syntax_help = """
🔧 SQL语法帮助

由于适配 modules/sql_compiler 的限制，请注意以下语法要求:

1. CREATE TABLE:
   ✅ CREATE TABLE table_name (col1 INT, col2 STRING);
   ❌ CREATE TABLE table_name (id INT PRIMARY KEY, name STRING);

2. INSERT:
   ✅ INSERT INTO table_name (col1, col2) VALUES (val1, val2);
   ❌ INSERT INTO table_name VALUES (val1, val2);

3. SELECT:
   ✅ SELECT col1, col2 FROM table_name WHERE col1 = value;
   ❌ SELECT * FROM table_name WHERE col1 >= 5 AND col2 = 'test';

4. WHERE条件:
   ✅ WHERE id = 5
   ✅ WHERE name = 'Alice'
   ✅ WHERE score > 80
   ❌ WHERE id >= 3 AND id <= 7
   ❌ WHERE name = 'Alice' OR age > 20

5. DROP TABLE:
   ✅ DROP TABLE table_name;
   ❌ DROP TABLE IF EXISTS table_name;

6. JOIN查询:
   ✅ SELECT t1.col1, t2.col2 FROM table1 t1 JOIN table2 t2 ON t1.id = t2.id;
   ❌ SELECT * FROM table1 JOIN table2 ON condition;

7. ORDER BY:
   ✅ SELECT col1, col2 FROM table ORDER BY col1 ASC;
   ✅ SELECT col1, col2 FROM table ORDER BY col1 DESC;
   ❌ SELECT * FROM table ORDER BY col1, col2;

8. GROUP BY:
   ✅ SELECT col1, COUNT(*) FROM table GROUP BY col1;
   ✅ SELECT col1, SUM(col2) FROM table GROUP BY col1;
   ❌ SELECT * FROM table GROUP BY col1;
        """
        print(syntax_help)

    def _show_tables(self):
        """显示所有表"""
        try:
            catalog_info = self.adapter.get_catalog_info()
            tables = catalog_info.get("tables", [])
            
            if tables:
                print(" 数据库中的表:")
                for table in tables:
                    print(f"  • {table}")
            else:
                print(" 数据库中没有表")
        except Exception as e:
            print(f" 获取表列表失败: {str(e)}")

    def _flush_database(self):
        """刷盘数据"""
        try:
            self.adapter.flush()
            print("✓ 数据已刷盘到磁盘")
        except Exception as e:
            print(f" 刷盘失败: {str(e)}")

    def _cleanup(self):
        """清理资源"""
        try:
            self.adapter.flush()
            print("✓ 数据已保存")
        except Exception as e:
            print(f"⚠  保存数据时出错: {str(e)}")

    def _show_cache(self):
        """显示缓存统计信息"""
        try:
            stats = self.adapter.get_cache_stats()
            print("缓存统计:")
            print(f"  Python缓存: {stats.get('python_cache', {})}")
            print(f"  混合统计: {stats.get('hybrid_stats', {})}")
            print(f"  C++加速: {stats.get('cpp_enabled', False)}")
        except Exception as e:
            print(f" 获取缓存统计失败: {str(e)}")

    def _flush_cache(self):
        """刷新缓存到磁盘"""
        try:
            self.adapter.flush_cache()
            print("✓ 缓存已刷新并刷盘")
        except Exception as e:
            print(f" 刷新缓存失败: {str(e)}")


def main():
    """主函数"""
    try:
        cli = HybridDatabaseCLI()
        cli.start()
    except Exception as e:
        print(f"启动失败: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
