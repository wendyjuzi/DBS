"""
混合架构命令行界面
"""

import sys
from typing import Any, Dict, List
from ..core.hybrid_engine import HybridDatabaseEngine
from ..utils.exceptions import DatabaseError, SQLSyntaxError, ExecutionError

# 新增：prompt_toolkit 支持的历史与补全
try:
	from prompt_toolkit import PromptSession
	from prompt_toolkit.history import FileHistory
	from prompt_toolkit.completion import Completer, Completion
	from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
	from prompt_toolkit.shortcuts import prompt
	PROMPT_TOOLKIT_AVAILABLE = True
except Exception:
	PROMPT_TOOLKIT_AVAILABLE = False

SQL_KEYWORDS = [
	"SELECT", "FROM", "WHERE", "INSERT", "INTO", "VALUES", "CREATE", "TABLE",
	"DELETE", "UPDATE", "SET", "BEGIN", "COMMIT", "ROLLBACK", "SHOW", "INDEX",
	"CREATE INDEX", "DROP INDEX", "DROP TABLE", "CREATE VIEW", "DROP VIEW",
	"CREATE PROCEDURE", "DROP PROCEDURE", "CALL", "GROUP BY", "ORDER BY",
	"COUNT", "SUM", "AVG", "MIN", "MAX"
]

class _SQLCompleter(Completer):
	def __init__(self, get_catalog_names):
		self._get_catalog_names = get_catalog_names

	def get_completions(self, document, complete_event):
		text = document.text_before_cursor.upper()
		last = text.split()[-1] if text.split() else ""
		# 补全关键字
		for kw in SQL_KEYWORDS:
			if kw.startswith(last):
				yield Completion(kw, start_position=-len(last))
		# 动态补全表与列
		names = self._get_catalog_names()
		for n in names:
			if n.upper().startswith(last):
				yield Completion(n, start_position=-len(last))

class HybridCLI:
	"""混合架构数据库命令行界面"""

	def __init__(self):
		"""初始化CLI"""
		try:
			self.engine = HybridDatabaseEngine()
			print("=== 混合架构数据库系统 (Python-C++ Hybrid) ===")
			print("支持的命令: CREATE TABLE, INSERT, SELECT, DELETE, UPDATE, BEGIN/COMMIT/ROLLBACK, SHOW")
			print("输入 'exit' 退出, 'help' 查看帮助\n")
			# 初始化历史与补全
			self._session = None
			if PROMPT_TOOLKIT_AVAILABLE:
				def _catalog_names():
					try:
						# 表名 + 常见列名收集（简化：仅表名）
						return self.engine.get_tables()
					except Exception:
						return []
				self._session = PromptSession(
					message="db> ",
					history=FileHistory(".db_cli_history"),
					auto_suggest=AutoSuggestFromHistory(),
					completer=_SQLCompleter(_catalog_names),
				)
		except Exception as e:
			print(f"数据库初始化失败: {str(e)}")
			sys.exit(1)

	def start(self):
		"""启动命令行交互"""
		while True:
			try:
				if self._session:
					sql = self._session.prompt().strip()
				else:
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
			columns = metadata.get("columns", [])
			if columns:
				self._print_table(columns, data)
				print(f"共 {len(data)} 行")
		else:
			message = metadata.get("message", f"影响 {affected_rows} 行")
			print(f"✓ {message}")
		if execution_time > 0:
			print(f"执行时间: {execution_time:.4f}秒")

	def _print_table(self, columns: List[str], data: List[List[str]]):
		"""打印表格"""
		if not data:
			print("(无数据)")
			return
		col_widths = []
		for i, col in enumerate(columns):
			max_width = len(str(col))
			for row in data:
				if i < len(row):
					max_width = max(max_width, len(str(row[i])))
			col_widths.append(max_width)
		total_width = sum(col_widths) + len(col_widths) * 3 + 1
		print("-" * total_width)
		header = " | ".join(f"{str(columns[i]).ljust(col_widths[i])}" for i in range(len(columns)))
		print(f"| {header} |")
		print("-" * total_width)
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
  SELECT COUNT(*) FROM table_name                        - 聚合
  DELETE FROM table_name [WHERE condition]               - 删除数据
  tables                                                 - 显示所有表
  help                                                   - 显示此帮助
  exit                                                   - 退出程序

快捷键:
  上/下  历史命令（若安装 prompt_toolkit）
  Tab    自动补全（若安装 prompt_toolkit）
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
