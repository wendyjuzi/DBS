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

# 导入prompt_toolkit用于历史记录和自动补全
try:
    from prompt_toolkit import PromptSession
    from prompt_toolkit.history import FileHistory
    from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
    from prompt_toolkit.completion import Completer, Completion
    from prompt_toolkit.completion.word_completer import WordCompleter
    from prompt_toolkit.lexers import PygmentsLexer
    from prompt_toolkit.styles import Style
    PROMPT_TOOLKIT_AVAILABLE = True
except ImportError:
    PROMPT_TOOLKIT_AVAILABLE = False
    # 创建占位符类，避免NameError
    class Completer:
        def get_completions(self, document, complete_event):
            return []
    class Completion:
        def __init__(self, text, start_position=0, display_meta=""):
            self.text = text
            self.start_position = start_position
            self.display_meta = display_meta

# 导入现有组件
from src.api.sql_compiler_adapter import SQLCompilerAdapter
from src.core.hybrid_engine import HybridDatabaseEngine
from src.utils.exceptions import ExecutionError, SQLSyntaxError


class SQLCompleter(Completer):
    """SQL自动补全器"""
    
    def __init__(self, cli_instance):
        self.cli = cli_instance
        
        # SQL关键字
        self.sql_keywords = [
            'SELECT', 'FROM', 'WHERE', 'INSERT', 'INTO', 'VALUES', 'UPDATE', 'SET',
            'DELETE', 'CREATE', 'TABLE', 'DROP', 'ALTER', 'INDEX', 'PRIMARY', 'KEY',
            'FOREIGN', 'REFERENCES', 'UNIQUE', 'NOT', 'NULL', 'DEFAULT', 'AUTO_INCREMENT',
            'INT', 'STRING', 'DOUBLE', 'VARCHAR', 'TEXT', 'DATETIME', 'DATE', 'TIME',
            'AND', 'OR', 'IN', 'LIKE', 'BETWEEN', 'IS', 'ORDER', 'BY', 'GROUP', 'HAVING',
            'LIMIT', 'OFFSET', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 'OUTER', 'ON', 'AS',
            'DISTINCT', 'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'ASC', 'DESC',
            'BEGIN', 'COMMIT', 'ROLLBACK', 'TRANSACTION', 'AUTOCOMMIT',
            'EXPLAIN', 'SHOW', 'EXPORT', 'TO', 'PATH', 'CSV', 'JSON', 'IMPORT'
        ]
        
        # 系统命令
        self.system_commands = [
            'help', 'tables', 'clear', 'flush', 'cache', 'flushcache', 
            'exit', 'MODE', 'ADAPTER', 'CORE', 'SHOW', 'TRANSACTION', 'OVERLAY',
            'SET', 'ON', 'OFF', 'BEGIN', 'COMMIT', 'ROLLBACK'
        ]
        
        # 操作符和符号
        self.operators = ['=', '!=', '<', '>', '<=', '>=', '(', ')', ',', ';']
    
    def get_completions(self, document, complete_event):
        """获取补全建议"""
        word = document.get_word_before_cursor(WORD=True)
        text_before_cursor = document.text_before_cursor
        
        # 获取当前上下文
        context = self._get_context(text_before_cursor)
        
        # 根据上下文提供不同的补全
        if context == 'keyword':
            completions = self._get_keyword_completions(word)
        elif context == 'table':
            completions = self._get_table_completions(word)
        elif context == 'column':
            completions = self._get_column_completions(word, text_before_cursor)
        elif context == 'system':
            completions = self._get_system_completions(word)
        else:
            # 默认提供所有类型的补全
            completions = (self._get_keyword_completions(word) + 
                          self._get_table_completions(word) + 
                          self._get_system_completions(word))
        
        # 去重并排序
        seen = set()
        unique_completions = []
        for completion in sorted(completions, key=lambda x: x.text):
            if completion.text not in seen:
                seen.add(completion.text)
                unique_completions.append(completion)
        
        return unique_completions
    
    def _get_context(self, text_before_cursor):
        """分析当前上下文"""
        text = text_before_cursor.upper().strip()
        
        # 系统命令上下文
        if (text.startswith('HELP') or text.startswith('TABLES') or 
            text.startswith('CLEAR') or text.startswith('FLUSH') or
            text.startswith('CACHE') or text.startswith('MODE') or
            text.startswith('SHOW') or text.startswith('SET')):
            return 'system'

        # 导入命令上下文
        if text.startswith('IMPORT TABLE'):
            return 'import'

        # 表名上下文 (FROM, JOIN, INTO等后面)
        if any(keyword in text for keyword in ['FROM ', 'JOIN ', 'INTO ', 'UPDATE ', 'TABLE ']):
            return 'table'
        
        # 列名上下文 (SELECT, WHERE, SET等后面)
        if any(keyword in text for keyword in ['SELECT ', 'WHERE ', 'SET ', 'ORDER BY ', 'GROUP BY ']):
            return 'column'
        
        # 默认关键字上下文
        return 'keyword'
    
    def _get_keyword_completions(self, word):
        """获取SQL关键字补全"""
        word_upper = word.upper() if word else ''
        completions = []
        
        for keyword in self.sql_keywords:
            if keyword.startswith(word_upper):
                completions.append(Completion(
                    keyword, 
                    start_position=-len(word) if word else 0,
                    display_meta=f"SQL关键字"
                ))
        
        return completions
    
    def _get_table_completions(self, word):
        """获取表名补全"""
        word_lower = word.lower() if word else ''
        completions = []
        
        try:
            # 获取当前数据库中的表
            catalog_info = self.cli.adapter.get_catalog_info()
            tables = catalog_info.get("tables", [])
            
            for table in tables:
                if table.lower().startswith(word_lower):
                    completions.append(Completion(
                        table,
                        start_position=-len(word) if word else 0,
                        display_meta=f"表名"
                    ))
        except Exception:
            pass  # 如果获取表信息失败，忽略错误
        
        return completions
    
    def _get_column_completions(self, word, text_before_cursor):
        """获取列名补全"""
        word_lower = word.lower() if word else ''
        completions = []
        
        try:
            # 尝试从当前SQL中提取表名
            table_name = self._extract_table_from_sql(text_before_cursor)
            if table_name:
                # 获取表的列信息
                columns = self._get_table_columns(table_name)
                for column in columns:
                    if column.lower().startswith(word_lower):
                        completions.append(Completion(
                            column,
                            start_position=-len(word) if word else 0,
                            display_meta=f"列名 ({table_name})"
                        ))
        except Exception:
            pass  # 如果获取列信息失败，忽略错误
        
        return completions
    
    def _get_system_completions(self, word):
        """获取系统命令补全"""
        word_lower = word.lower() if word else ''
        completions = []
        
        for command in self.system_commands:
            if command.lower().startswith(word_lower):
                completions.append(Completion(
                    command,
                    start_position=-len(word) if word else 0,
                    display_meta=f"系统命令"
                ))
        
        return completions
    
    def _extract_table_from_sql(self, sql_text):
        """从SQL文本中提取表名"""
        sql_upper = sql_text.upper()
        
        # 简单的表名提取逻辑
        patterns = [
            ('FROM ', ' FROM '),
            ('JOIN ', ' JOIN '),
            ('INTO ', ' INTO '),
            ('UPDATE ', 'UPDATE ')
        ]
        
        for keyword, search_pattern in patterns:
            if search_pattern in sql_upper:
                parts = sql_upper.split(search_pattern, 1)
                if len(parts) > 1:
                    # 提取表名（到空格或逗号为止）
                    table_part = parts[1].split()[0].split(',')[0]
                    return table_part.lower()
        
        return None
    
    def _get_table_columns(self, table_name):
        """获取指定表的列名"""
        try:
            # 通过执行一个简单的查询来获取列信息
            result = self.cli.adapter.execute(f"SELECT * FROM {table_name} LIMIT 0")
            metadata = result.get("metadata", {})
            return metadata.get("columns", [])
        except Exception:
            return []


class SimpleHistory:
    """简单的历史记录管理器（当prompt_toolkit不可用时使用）"""
    
    def __init__(self, history_file=None):
        self.history_file = history_file or os.path.join(os.path.expanduser("~"), ".hybrid_db_history")
        self.history = []
        self.current_index = 0
        self._load_history()
    
    def _load_history(self):
        """加载历史记录"""
        try:
            if os.path.exists(self.history_file):
                with open(self.history_file, 'r', encoding='utf-8') as f:
                    self.history = [line.strip() for line in f.readlines() if line.strip()]
                self.current_index = len(self.history)
        except Exception:
            pass
    
    def append(self, text):
        """添加命令到历史记录"""
        if text.strip() and (not self.history or self.history[-1] != text.strip()):
            self.history.append(text.strip())
            self.current_index = len(self.history)
            self._save_history()
    
    def _save_history(self):
        """保存历史记录"""
        try:
            with open(self.history_file, 'w', encoding='utf-8') as f:
                for item in self.history[-1000:]:  # 只保存最近1000条
                    f.write(item + '\n')
        except Exception:
            pass
    
    def get_previous(self):
        """获取上一条历史记录"""
        if self.history and self.current_index > 0:
            self.current_index -= 1
            return self.history[self.current_index]
        return ""
    
    def get_next(self):
        """获取下一条历史记录"""
        if self.history and self.current_index < len(self.history) - 1:
            self.current_index += 1
            return self.history[self.current_index]
        elif self.history and self.current_index == len(self.history) - 1:
            self.current_index += 1
            return ""
        return ""


class HybridDatabaseCLI:
    """混合架构数据库命令行界面 - 使用SQL编译器适配器"""

    def __init__(self):
        """初始化CLI"""
        try:
            print("正在初始化混合架构数据库系统...")
            # 两套后端：编译器适配器路径、核心混合引擎路径
            self.adapter = SQLCompilerAdapter()
            self.core_engine = HybridDatabaseEngine()
            self.mode = "adapter"  # adapter | core
            
            # 初始化历史记录和自动补全
            self._init_history_and_completion()
            
            print("=== 混合架构数据库系统 (SQL编译器 + C++执行引擎) ===")
            print("支持的命令: CREATE TABLE, INSERT, SELECT, DELETE, UPDATE, DROP TABLE, EXPORT TABLE")
            print("输入 'exit' 退出, 'help' 查看帮助, 'tables' 显示所有表")
            print("输入 'cache' 查看缓存统计, 'flushcache' 刷新缓存到磁盘")
            print("输入 'BEGIN' 开启事务, 'COMMIT' 提交, 'ROLLBACK' 回滚")
            print("输入 'SHOW TRANSACTION' 查看事务状态, 'SET AUTOCOMMIT = ON|OFF' 设置自动提交")
            print("输入 'MODE ADAPTER|CORE' 切换执行后端 (当前: adapter)")
            print("注意: 适配 modules/sql_compiler 的语法限制")
            
            # 显示增强功能提示
            if PROMPT_TOOLKIT_AVAILABLE:
                print("\n✨ 增强功能已启用:")
                print("  - 使用 ↑↓ 箭头键浏览历史命令")
                print("  - 按 Tab 键自动补全 (表名、列名、SQL关键字)")
                print("  - 历史命令自动建议")
                print("  - 历史记录保存在 ~/.hybrid_db_history")
            else:
                print("\n💡 提示: 安装 prompt_toolkit 获得更好的交互体验")
                print("   pip install prompt_toolkit")
            print()
            
        except Exception as e:
            print(f"数据库初始化失败: {str(e)}")
            print("请确保:")
            print("1. C++模块已编译 (运行 scripts/run_final_demo.ps1)")
            print("2. 所有依赖文件存在")
            sys.exit(1)
    
    def _init_history_and_completion(self):
        """初始化历史记录和自动补全"""
        self.session = None
        self.simple_history = None
        self.prompt_toolkit_available = PROMPT_TOOLKIT_AVAILABLE
        
        if PROMPT_TOOLKIT_AVAILABLE:
            try:
                # 创建历史记录文件路径
                history_file = os.path.join(os.path.expanduser("~"), ".hybrid_db_history")
                
                # 创建自动补全器
                completer = SQLCompleter(self)
                
                # 创建提示会话
                self.session = PromptSession(
                    history=FileHistory(history_file),
                    completer=completer,
                    auto_suggest=AutoSuggestFromHistory(),
                    complete_style='column',  # 补全样式
                    reserve_space_for_menu=8,  # 为补全菜单保留空间
                )
                print("✓ 历史记录和自动补全已启用")
            except Exception as e:
                print(f"⚠ 启用增强功能失败: {e}")
                print("将使用基础模式")
                self.prompt_toolkit_available = False
        
        if not self.prompt_toolkit_available:
            # 使用简单的历史记录管理器
            self.simple_history = SimpleHistory()
            print("✓ 基础历史记录已启用")
    
    def _get_input(self, prompt):
        """获取用户输入，支持历史记录和自动补全"""
        if self.prompt_toolkit_available and self.session:
            try:
                return self.session.prompt(prompt)
            except KeyboardInterrupt:
                raise
            except Exception as e:
                print(f"⚠ 输入错误: {e}")
                return input(prompt)
        else:
            # 使用标准input，但支持简单历史记录
            return self._simple_input(prompt)
    
    def _simple_input(self, prompt):
        """简单的输入方法，支持基础历史记录"""
        try:
            import sys
            
            # 显示提示符
            sys.stdout.write(prompt)
            sys.stdout.flush()
            
            # 读取输入
            line = ""
            while True:
                char = sys.stdin.read(1)
                if char == '\n':
                    break
                elif char == '\r':
                    continue
                elif ord(char) == 3:  # Ctrl+C
                    raise KeyboardInterrupt()
                elif ord(char) == 4:  # Ctrl+D
                    raise EOFError()
                elif ord(char) == 127:  # Backspace
                    if line:
                        line = line[:-1]
                        sys.stdout.write('\b \b')
                        sys.stdout.flush()
                elif ord(char) == 27:  # Escape sequence (arrow keys)
                    next_char = sys.stdin.read(1)
                    if next_char == '[':
                        arrow = sys.stdin.read(1)
                        if arrow == 'A':  # Up arrow
                            history_line = self.simple_history.get_previous()
                            if history_line:
                                # 清除当前行
                                sys.stdout.write('\r' + ' ' * (len(prompt) + len(line)) + '\r')
                                sys.stdout.write(prompt)
                                line = history_line
                                sys.stdout.write(line)
                                sys.stdout.flush()
                        elif arrow == 'B':  # Down arrow
                            history_line = self.simple_history.get_next()
                            # 清除当前行
                            sys.stdout.write('\r' + ' ' * (len(prompt) + len(line)) + '\r')
                            sys.stdout.write(prompt)
                            line = history_line
                            sys.stdout.write(line)
                            sys.stdout.flush()
                else:
                    line += char
                    sys.stdout.write(char)
                    sys.stdout.flush()
            
            sys.stdout.write('\n')
            sys.stdout.flush()
            
            # 添加到历史记录
            if line.strip():
                self.simple_history.append(line)
            
            return line
            
        except (KeyboardInterrupt, EOFError):
            raise
        except Exception:
            # 如果出错，回退到标准input
            return input(prompt)

    def start(self):
        """启动命令行交互"""
        print("数据库系统已就绪，可以开始输入SQL语句...\n")
        
        while True:
            try:
                # 支持多行SQL输入
                sql_lines = []
                while True:
                    # 使用增强的输入方法
                    line = self._get_input("db> " if not sql_lines else "  > ").strip()

                    # 检查是否是导出命令
                    if line.upper().startswith("EXPORT TABLE"):
                        self._handle_export_command(line)
                        continue

                    # 检查是否是导入命令
                    if line.upper().startswith("IMPORT TABLE"):
                        self._handle_import_command(line)
                        continue

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

                    if line.lower() == "show overlay":
                        self._show_tx_overlay()
                        break

                    # 切换后端模式
                    if line.upper().startswith("MODE "):
                        self._switch_mode(line)
                        break

                    # 事务控制命令（大小写不敏感，直接交给适配器处理）
                    if line.upper() in ("BEGIN", "COMMIT", "ROLLBACK", "SHOW TRANSACTION") or line.upper().startswith("SET AUTOCOMMIT"):
                        self._execute_sql(line.upper() + ";")
                        break
                    
                    # EXPLAIN 支持：前缀匹配直接执行
                    if line.lower().startswith("explain "):
                        sql_lines = [line[len("explain "):]]
                        self._execute_sql("EXPLAIN " + ' '.join(sql_lines))
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
        
        # 将SQL添加到历史记录（如果不是系统命令）
        if not sql.strip().lower() in ['help', 'tables', 'clear', 'flush', 'cache', 'flushcache', 'exit']:
            if self.prompt_toolkit_available and self.session:
                try:
                    # prompt_toolkit会自动处理历史记录
                    pass
                except Exception:
                    pass
            elif self.simple_history:
                self.simple_history.append(sql)
        
        print(f"执行: {sql}")
        print("-" * 60)
        
        try:
            start_time = time.time()
            # 根据模式路由
            if self.mode == "adapter":
                result = self.adapter.execute(sql)
            else:
                # CORE 模式下：直接走核心混合引擎（简单SQL，支持 * 与更宽松 WHERE）
                # 仅传递纯 SQL；事务/索引/EXPLAIN 等命令在 CORE 模式下不支持
                up = sql.strip().upper().rstrip(';')
                unsupported = (
                    up in ("BEGIN", "COMMIT", "ROLLBACK", "SHOW TRANSACTION") or
                    up.startswith("SET AUTOCOMMIT") or
                    up.startswith("CREATE INDEX") or up.startswith("DROP INDEX") or
                    up.startswith("CREATE COMPOSITE INDEX") or up.startswith("DROP COMPOSITE INDEX") or
                    up == "SHOW INDEXES" or up == "SHOW COMPOSITE INDEXES" or
                    up.startswith("EXPLAIN ")
                )
                if unsupported:
                    result = {"status": "error", "error": "该命令在 CORE 模式暂不支持，请切换 MODE ADAPTER", "affected_rows": 0, "data": []}
                else:
                    result = self.core_engine.execute(sql)
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

    def _switch_mode(self, line: str):
        """切换执行后端模式"""
        try:
            parts = line.strip().split()
            if len(parts) != 2:
                print(" 用法: MODE ADAPTER|CORE")
                return
            target = parts[1].lower()
            if target not in ("adapter", "core"):
                print(" 模式必须是 ADAPTER 或 CORE")
                return
            self.mode = target
            print(f"✓ 已切换到模式: {self.mode}")
            if self.mode == "adapter":
                print("  - 使用 SQL 编译器 + C++ 执行引擎，支持事务/索引/EXPLAIN 等增强命令")
            else:
                print("  - 使用核心混合引擎，支持更宽松 SQL 解析（含 *、更复杂 WHERE）")
        except Exception as e:
            print(f" 切换模式失败: {str(e)}")

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

✨ 增强功能:
  ↑↓ 箭头键  - 浏览历史命令
  Tab 键     - 自动补全 (表名、列名、SQL关键字)
  历史记录   - 自动保存到 ~/.hybrid_db_history

🔧 系统命令:
  help       - 显示此帮助信息
  tables     - 显示所有表
  clear      - 清屏
  flush      - 刷盘数据到磁盘
  cache      - 显示缓存统计
  flushcache - 刷新缓存并刷盘
  SHOW TRANSACTION         - 查看事务状态与缓冲
  SHOW OVERLAY             - 查看事务覆盖层 (MVCC/UNDO)
  SET AUTOCOMMIT = ON|OFF  - 开关自动提交
  BEGIN      - 开启事务
  COMMIT     - 提交事务
  ROLLBACK   - 回滚事务
  MODE ADAPTER|CORE        - 切换执行后端 (ADAPTER 支持事务/索引/EXPLAIN 与版本链; CORE 为核心引擎快速路径)
  CREATE INDEX idx ON table(col) PK pkcol;  - 创建二级索引
  DROP INDEX table(col);                     - 删除索引
  SHOW INDEXES                               - 查看所有索引
  SHOW TRIGGERS                              - 查看所有触发器
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
  EXPORT TABLE table_name TO format PATH 'file_path'     - 导出数据
  IMPORT TABLE table_name FROM format PATH 'file_path'   - 从文件导入数据

📚 视图/物化视图/触发器/过程:
  -- 视图（非物化）：
  CREATE VIEW v AS SELECT col1, col2 FROM t;
  SELECT * FROM v;               -- 支持 * 与指定列
  DROP VIEW v;

  -- 物化视图（以物理表存储，需刷新）：
  CREATE MATERIALIZED VIEW mv AS SELECT col1 FROM t;
  REFRESH MATERIALIZED VIEW mv;  -- 全量刷新
  DROP MATERIALIZED VIEW mv;

  -- 触发器（行级 BEFORE/AFTER INSERT/UPDATE/DELETE）：
  CREATE TRIGGER trg BEFORE INSERT ON t AS BEGIN
    -- 这里写多条语句，每条以分号结束
    INSERT INTO audit(id) VALUES (1);
  END;
  SHOW TRIGGERS;
  DROP TRIGGER trg ON t;

  -- 存储过程（适配器内解释执行）：
  CREATE PROCEDURE p AS BEGIN
    INSERT INTO t(id,name) VALUES (10,'x');
  END;
  CALL p;
  DROP PROCEDURE p;

🔎 索引命令:
  CREATE INDEX idx ON table(col) PK pkcol;               - 创建单列二级索引
  CREATE INDEX idx ON table(col) USING HASH PK pkcol;    - 指定索引策略(HASH/BTREE)
  CREATE COMPOSITE INDEX idx ON table(col1,col2);        - 创建复合索引（内存雏形）
  DROP INDEX table(col);                                 - 删除索引
  DROP COMPOSITE INDEX ON table;                         - 删除复合索引
  SHOW INDEXES;                                          - 查看所有索引
  SHOW COMPOSITE INDEXES;                                - 查看复合索引
  EXPLAIN <SQL>;                                         - 显示执行路径与估计

🧵 版本链/MVCC 提示（仅 ADAPTER 模式生效）:
  - 使用 BEGIN 开启事务后，INSERT/DELETE/UPDATE 会写入版本链；当前事务可见，其他事务不可见。
  - COMMIT 后版本对其他会话可见；ROLLBACK 将丢弃未提交版本。
  - SHOW OVERLAY 可查看当前事务覆盖层（新增/删除计数）。

🔀 模式说明:
  - ADAPTER: 走 SQL 编译器 + 执行器，支持事务/索引/EXPLAIN，启用版本链可见性控制。
  - CORE:    走核心混合引擎，SQL 更宽松（允许 * 与更复杂 WHERE），但不支持事务/索引/EXPLAIN/版本链。

💡 快速示例（MVCC）:
  MODE ADAPTER
  BEGIN;
  INSERT INTO t (id, name) VALUES (5, 'E');
  SELECT id, name FROM t WHERE id = 5;  -- 本事务可见
  ROLLBACK;  -- 其它会话始终不可见

📊 支持的数据类型:
  INT     - 整数
  STRING  - 字符串
  DOUBLE  - 浮点数
  
📥 支持的导入格式:
  csv     - 逗号分隔值文件
  json    - JSON数据文件
  
📤 支持的导出格式:
  csv     - 逗号分隔值文件
  json    - JSON数据文件
  
⚠️  语法限制 (适配 modules/sql_compiler):
  - 不支持 PRIMARY KEY 语法
  - 不支持 * 通配符，必须指定具体列名
  - INSERT 必须指定列名: INSERT INTO table(col1, col2) VALUES (...)
  - WHERE 只支持简单条件: WHERE col = value
  - 不支持复杂条件如 AND, OR, NOT

💡 示例:
  CREATE TABLE students (id INT, name STRING, age INT, score DOUBLE);
  INSERT INTO students (id, name, age, score) VALUES (1, 'Alice', 20, 85.5);
  SELECT name, score FROM students WHERE age > 21;
  UPDATE students SET score = 90.0 WHERE id = 1;
  DELETE FROM students WHERE id = 1;
  DROP TABLE students;
  EXPORT TABLE students TO csv PATH 'students.csv';
  EXPORT TABLE students TO json PATH 'students.json';
  IMPORT TABLE students FROM csv PATH 'studentstest1.csv';
  IMPORT TABLE students FROM json PATH 'studentstest2.json';
  
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
   
9. EXPORT TABLE:
   ✅ EXPORT TABLE table_name TO csv PATH 'file.csv';
   ✅ EXPORT TABLE table_name TO json PATH 'file.json';
   ❌ EXPORT TABLE table_name TO xml PATH 'file.xml';
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

    def _show_tx_overlay(self):
        """显示事务覆盖层信息"""
        try:
            # 通过 UnifiedDB 访问 runner 暴露的 overlay 快照
            from src.api.unified_api import UnifiedDB
            if not hasattr(self, "_unified"):
                self._unified = UnifiedDB()
            snap = self._unified.show_tx_overlay()
            print("事务覆盖层:")
            print(f"  in_tx: {snap.get('in_tx')}")
            tables = snap.get("tables", {}) or {}
            if not tables:
                print("  (empty)")
            else:
                for t, s in tables.items():
                    print(f"  - {t}: inserts={s.get('inserts',0)}, deletes={s.get('deletes',0)}")
        except Exception as e:
            print(f" 显示覆盖层失败: {str(e)}")

    def _handle_export_command(self, command: str):
        """处理导出命令: EXPORT TABLE table_name TO format PATH 'path'"""
        try:
            # 移除末尾的分号（如果存在）
            if command.endswith(';'):
                command = command[:-1].strip()

            parts = command.split()
            if len(parts) >= 7 and parts[0].upper() == "EXPORT" and parts[1].upper() == "TABLE":
                table_name = parts[2]
                if parts[3].upper() != "TO":
                    raise ValueError("缺少 TO 关键字")

                format_type = parts[4].lower()
                if parts[5].upper() != "PATH":
                    raise ValueError("缺少 PATH 关键字")

                # 处理路径（可能包含空格，需要合并）
                path_parts = parts[6:]
                path = ' '.join(path_parts).strip("'\"")

                # 移除路径中可能的分号
                if path.endswith(';'):
                    path = path[:-1]

                # 直接调用 adapter 的 export_table 方法
                success = self.adapter.export_table(table_name, format_type, path)
                if success:
                    print(f"✓ 导出成功: {table_name} → {path}")
                else:
                    print("❌ 导出失败")
            else:
                print("❌ 导出命令格式错误")
                self._show_export_help()

        except Exception as e:
            print(f"❌ 导出命令解析错误: {str(e)}")
            self._show_export_help()

    def _show_export_help(self):
        """显示导出帮助信息"""
        help_text = """
📤 数据导出命令格式:
    EXPORT TABLE table_name TO format PATH 'file_path'

💡 示例:
    EXPORT TABLE students TO csv PATH 'data/students.csv'
    EXPORT TABLE employees TO json PATH 'exports/employees.json'

📊 支持的导出格式:
    csv  - 逗号分隔值文件
    json - JSON数据文件
        """
        print(help_text)

    def _handle_import_command(self, command: str):
        """处理导入命令: IMPORT TABLE table_name FROM format PATH 'path'"""
        try:
            # 移除末尾的分号（如果存在）
            if command.endswith(';'):
                command = command[:-1].strip()

            parts = command.split()
            if len(parts) >= 7 and parts[0].upper() == "IMPORT" and parts[1].upper() == "TABLE":
                table_name = parts[2]
                if parts[3].upper() != "FROM":
                    raise ValueError("缺少 FROM 关键字")

                format_type = parts[4].lower()
                if parts[5].upper() != "PATH":
                    raise ValueError("缺少 PATH 关键字")

                # 处理路径（可能包含空格，需要合并）
                path_parts = parts[6:]
                path = ' '.join(path_parts).strip("'\"")

                # 移除路径中可能的分号
                if path.endswith(';'):
                    path = path[:-1]

                # 调用导入方法
                success = self.adapter.import_table(table_name, format_type, path)
                if success:
                    print(f"✓ 导入成功: {path} → {table_name}")
                else:
                    print("❌ 导入失败")
            else:
                print("❌ 导入命令格式错误")
                self._show_import_help()

        except Exception as e:
            print(f"❌ 导入命令解析错误: {str(e)}")
            self._show_import_help()

    def _show_import_help(self):
        """显示导入帮助信息"""
        help_text = """
    📥 数据导入命令格式:
        IMPORT TABLE table_name FROM format PATH 'file_path'
    
    💡 示例:
        IMPORT TABLE students FROM csv PATH 'data/students.csv'
        IMPORT TABLE employees FROM json PATH 'exports/employees.json'
    
    📊 支持的导入格式:
        csv  - 逗号分隔值文件
        json - JSON数据文件
    
    ⚠️  注意:
        - CSV文件第一行应为列名
        - 表会自动创建（如果不存在）
        - 数据类型会自动推断
        """
        print(help_text)


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
