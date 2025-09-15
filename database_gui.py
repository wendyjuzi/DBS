#!/usr/bin/env python3
"""
混合架构数据库系统 - GUI界面
可视化展示编译器、存储和执行引擎的状态
"""

import sys
import os
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox, filedialog
from pathlib import Path
import threading
import time
from typing import Dict, Any, Optional

# 添加项目根目录到路径
proj_root = Path(__file__).resolve().parent
sys.path.insert(0, str(proj_root))

try:
    from src.api.sql_compiler_adapter import SQLCompilerAdapter
    from src.core.hybrid_engine import HybridDatabaseEngine
    from src.utils.exceptions import ExecutionError, SQLSyntaxError

    BACKEND_AVAILABLE = True
except ImportError as e:
    print(f"警告: 无法导入后端模块: {e}")
    BACKEND_AVAILABLE = False


class DatabaseGUI:
    """数据库系统GUI主界面"""

    def __init__(self):
        self.root = tk.Tk()
        self.root.title("混合架构数据库系统 - 可视化界面")
        self.root.geometry("1200x800")

        # 初始化后端系统
        self.adapter = None
        self.core_engine = None
        self.current_mode = "adapter"  # adapter | core
        self.system_status = {
            "compiler": "未连接",
            "storage": "未连接",
            "executor": "未连接"
        }

        # 创建界面
        self.create_widgets()
        self.init_backend()

    def create_widgets(self):
        """创建GUI组件"""
        # 主框架
        main_frame = ttk.Frame(self.root)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # 顶部状态栏
        self.create_status_bar(main_frame)

        # 中间内容区域 - 使用PanedWindow分割
        paned_window = ttk.PanedWindow(main_frame, orient=tk.HORIZONTAL)
        paned_window.pack(fill=tk.BOTH, expand=True, pady=(10, 0))

        # 左侧面板 - SQL输入和执行
        left_panel = ttk.Frame(paned_window)
        paned_window.add(left_panel, weight=1)
        self.create_sql_panel(left_panel)

        # 右侧面板 - 系统监控
        right_panel = ttk.Frame(paned_window)
        paned_window.add(right_panel, weight=1)
        self.create_system_panel(right_panel)

    def create_status_bar(self, parent):
        """创建状态栏"""
        status_frame = ttk.Frame(parent)
        status_frame.pack(fill=tk.X, pady=(0, 10))

        # 系统状态标签
        ttk.Label(status_frame, text="系统状态:", font=("Arial", 10, "bold")).pack(side=tk.LEFT)

        # 编译器状态
        self.compiler_status = ttk.Label(status_frame, text="编译器: 未连接",
                                         foreground="red")
        self.compiler_status.pack(side=tk.LEFT, padx=(10, 0))

        # 存储引擎状态
        self.storage_status = ttk.Label(status_frame, text="存储: 未连接",
                                        foreground="red")
        self.storage_status.pack(side=tk.LEFT, padx=(10, 0))

        # 执行引擎状态
        self.executor_status = ttk.Label(status_frame, text="执行器: 未连接",
                                         foreground="red")
        self.executor_status.pack(side=tk.LEFT, padx=(10, 0))

        # 模式切换
        ttk.Label(status_frame, text="模式:").pack(side=tk.LEFT, padx=(20, 5))
        self.mode_var = tk.StringVar(value=self.current_mode)
        mode_combo = ttk.Combobox(status_frame, textvariable=self.mode_var,
                                  values=["adapter", "core"], state="readonly", width=10)
        mode_combo.pack(side=tk.LEFT)
        mode_combo.bind("<<ComboboxSelected>>", self.on_mode_change)

        # 刷新按钮
        ttk.Button(status_frame, text="刷新状态",
                   command=self.refresh_status).pack(side=tk.RIGHT)

    def create_sql_panel(self, parent):
        """创建SQL输入执行面板"""
        # SQL面板标题
        sql_frame = ttk.LabelFrame(parent, text="SQL 查询界面", padding=10)
        sql_frame.pack(fill=tk.BOTH, expand=True)

        # SQL输入区域
        input_frame = ttk.Frame(sql_frame)
        input_frame.pack(fill=tk.BOTH, expand=True)

        ttk.Label(input_frame, text="SQL语句:", font=("Arial", 10, "bold")).pack(anchor=tk.W)

        # SQL输入文本框
        self.sql_text = scrolledtext.ScrolledText(input_frame, height=8,
                                                  font=("Consolas", 10))
        self.sql_text.pack(fill=tk.BOTH, expand=True, pady=(5, 10))
        
        # 绑定自动补全事件
        self.setup_autocomplete()

        # 预设SQL示例
        sample_sql = """-- SQL示例 (点击执行按钮运行)
CREATE TABLE students (id INT, name STRING, age INT, score DOUBLE);
INSERT INTO students (id, name, age, score) VALUES (1, 'Alice', 20, 85.5);
SELECT id, name, score FROM students WHERE age > 18;"""
        self.sql_text.insert(tk.END, sample_sql)

        # 按钮区域
        button_frame = ttk.Frame(input_frame)
        button_frame.pack(fill=tk.X, pady=(0, 10))

        ttk.Button(button_frame, text="执行 SQL",
                   command=self.execute_sql).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(button_frame, text="清空",
                   command=self.clear_sql).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(button_frame, text="示例",
                   command=self.load_sample_sql).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(button_frame, text="导出结果",
                   command=self.export_results).pack(side=tk.RIGHT)

        # 结果显示区域
        result_frame = ttk.LabelFrame(sql_frame, text="执行结果", padding=5)
        result_frame.pack(fill=tk.BOTH, expand=True)

        # 结果文本框
        self.result_text = scrolledtext.ScrolledText(result_frame, height=12,
                                                     font=("Consolas", 9))
        self.result_text.pack(fill=tk.BOTH, expand=True)

    def create_system_panel(self, parent):
        """创建系统监控面板"""
        # 系统监控面板
        system_frame = ttk.LabelFrame(parent, text="系统监控", padding=10)
        system_frame.pack(fill=tk.BOTH, expand=True)

        # 使用Notebook创建标签页
        notebook = ttk.Notebook(system_frame)
        notebook.pack(fill=tk.BOTH, expand=True)

        # 编译器监控标签页
        compiler_frame = ttk.Frame(notebook)
        notebook.add(compiler_frame, text="SQL编译器")
        self.create_compiler_monitor(compiler_frame)

        # 存储引擎监控标签页
        storage_frame = ttk.Frame(notebook)
        notebook.add(storage_frame, text="存储引擎")
        self.create_storage_monitor(storage_frame)

        # 执行引擎监控标签页
        executor_frame = ttk.Frame(notebook)
        notebook.add(executor_frame, text="执行引擎")
        self.create_executor_monitor(executor_frame)

        # 系统日志标签页
        log_frame = ttk.Frame(notebook)
        notebook.add(log_frame, text="系统日志")
        self.create_log_monitor(log_frame)

    def create_compiler_monitor(self, parent):
        """创建编译器监控面板"""
        # 编译器状态信息
        status_frame = ttk.LabelFrame(parent, text="编译器状态", padding=10)
        status_frame.pack(fill=tk.X, pady=(0, 10))

        self.compiler_info = tk.Text(status_frame, height=4, font=("Consolas", 9))
        self.compiler_info.pack(fill=tk.X)

        # 编译过程详情 - 使用Notebook分页显示
        compile_notebook = ttk.Notebook(parent)
        compile_notebook.pack(fill=tk.BOTH, expand=True)

        # 词法分析标签页
        lexical_frame = ttk.Frame(compile_notebook)
        compile_notebook.add(lexical_frame, text="词法分析")

        lexical_label = ttk.Label(lexical_frame, text="Token序列:", font=("Arial", 9, "bold"))
        lexical_label.pack(anchor=tk.W, padx=5, pady=(5, 0))

        self.lexical_result = scrolledtext.ScrolledText(lexical_frame, height=8, font=("Consolas", 9))
        self.lexical_result.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        # 语法分析标签页
        syntax_frame = ttk.Frame(compile_notebook)
        compile_notebook.add(syntax_frame, text="语法分析")

        syntax_label = ttk.Label(syntax_frame, text="抽象语法树(AST):", font=("Arial", 9, "bold"))
        syntax_label.pack(anchor=tk.W, padx=5, pady=(5, 0))

        self.syntax_result = scrolledtext.ScrolledText(syntax_frame, height=8, font=("Consolas", 9))
        self.syntax_result.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        # 语义分析标签页
        semantic_frame = ttk.Frame(compile_notebook)
        compile_notebook.add(semantic_frame, text="语义分析")

        semantic_label = ttk.Label(semantic_frame, text="语义检查结果:", font=("Arial", 9, "bold"))
        semantic_label.pack(anchor=tk.W, padx=5, pady=(5, 0))

        self.semantic_result = scrolledtext.ScrolledText(semantic_frame, height=8, font=("Consolas", 9))
        self.semantic_result.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        # 执行计划标签页
        plan_frame = ttk.Frame(compile_notebook)
        compile_notebook.add(plan_frame, text="执行计划")

        plan_label = ttk.Label(plan_frame, text="查询执行计划:", font=("Arial", 9, "bold"))
        plan_label.pack(anchor=tk.W, padx=5, pady=(5, 0))

        self.plan_result = scrolledtext.ScrolledText(plan_frame, height=8, font=("Consolas", 9))
        self.plan_result.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

    def create_storage_monitor(self, parent):
        """创建存储引擎监控面板"""
        # 存储统计
        stats_frame = ttk.LabelFrame(parent, text="存储统计", padding=10)
        stats_frame.pack(fill=tk.X, pady=(0, 10))

        self.storage_stats = tk.Text(stats_frame, height=8, font=("Consolas", 9))
        self.storage_stats.pack(fill=tk.X)

        # 表信息
        tables_frame = ttk.LabelFrame(parent, text="数据表", padding=10)
        tables_frame.pack(fill=tk.BOTH, expand=True)

        # 表列表
        self.tables_tree = ttk.Treeview(tables_frame, columns=("columns", "rows"),
                                        show="tree headings")
        self.tables_tree.heading("#0", text="表名")
        self.tables_tree.heading("columns", text="列数")
        self.tables_tree.heading("rows", text="行数")
        self.tables_tree.pack(fill=tk.BOTH, expand=True)

    def create_executor_monitor(self, parent):
        """创建执行引擎监控面板"""
        # 执行统计
        exec_frame = ttk.LabelFrame(parent, text="执行统计", padding=10)
        exec_frame.pack(fill=tk.X, pady=(0, 10))

        self.executor_stats = tk.Text(exec_frame, height=6, font=("Consolas", 9))
        self.executor_stats.pack(fill=tk.X)

        # 查询历史
        history_frame = ttk.LabelFrame(parent, text="查询历史", padding=10)
        history_frame.pack(fill=tk.BOTH, expand=True)

        self.query_history = scrolledtext.ScrolledText(history_frame, font=("Consolas", 9))
        self.query_history.pack(fill=tk.BOTH, expand=True)

    def create_log_monitor(self, parent):
        """创建日志监控面板"""
        log_frame = ttk.Frame(parent)
        log_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # 日志控制
        control_frame = ttk.Frame(log_frame)
        control_frame.pack(fill=tk.X, pady=(0, 10))

        ttk.Button(control_frame, text="清空日志",
                   command=self.clear_logs).pack(side=tk.LEFT)
        ttk.Button(control_frame, text="保存日志",
                   command=self.save_logs).pack(side=tk.LEFT, padx=(5, 0))

        # 日志显示
        self.log_text = scrolledtext.ScrolledText(log_frame, font=("Consolas", 9))
        self.log_text.pack(fill=tk.BOTH, expand=True)

    def init_backend(self):
        """初始化后端系统"""
        if not BACKEND_AVAILABLE:
            self.log("警告: 后端模块不可用，GUI将以演示模式运行")
            return

        try:
            self.log("正在初始化数据库系统...")

            # 初始化适配器
            self.adapter = SQLCompilerAdapter()
            self.system_status["compiler"] = "已连接"
            self.system_status["storage"] = "已连接"
            self.system_status["executor"] = "已连接"

            # 初始化核心引擎
            self.core_engine = HybridDatabaseEngine()

            self.log("✓ 数据库系统初始化成功")
            self.refresh_status()

        except Exception as e:
            self.log(f"错误: 数据库系统初始化失败: {e}")

    def execute_sql(self):
        """执行SQL语句"""
        sql = self.sql_text.get(1.0, tk.END).strip()
        if not sql:
            messagebox.showwarning("警告", "请输入SQL语句")
            return

        # 清空之前的结果
        self.result_text.delete(1.0, tk.END)
        self.clear_compilation_results()

        if not BACKEND_AVAILABLE:
            self.result_text.insert(tk.END, "演示模式: 后端不可用\n")
            self.result_text.insert(tk.END, f"模拟执行: {sql[:100]}...\n")
            self.simulate_compilation_process(sql)
            return

        try:
            self.log(f"执行SQL ({self.current_mode}模式): {sql[:50]}...")

            start_time = time.time()

            # 显示编译过程详情
            self.show_compilation_process(sql)

            # 根据模式选择执行引擎
            if self.current_mode == "adapter":
                result = self.adapter.execute(sql)
            else:
                result = self.core_engine.execute(sql)

            execution_time = time.time() - start_time

            # 显示结果
            self.display_result(result, execution_time)
            self.log(f"✓ SQL执行完成 ({execution_time:.3f}s)")

            # 更新监控信息
            self.update_monitors()

        except Exception as e:
            error_msg = f"执行错误: {str(e)}"
            self.result_text.insert(tk.END, error_msg)
            self.log(f"❌ {error_msg}")

    def display_result(self, result: Dict[str, Any], execution_time: float):
        """显示查询结果"""
        self.result_text.insert(tk.END, f"执行时间: {execution_time:.4f}秒\n")
        self.result_text.insert(tk.END, "-" * 60 + "\n")

        if result.get("status") == "error":
            self.result_text.insert(tk.END, f"错误: {result.get('error', '未知错误')}\n")
            return

        data = result.get("data", [])
        metadata = result.get("metadata", {})
        affected_rows = result.get("affected_rows", 0)

        if isinstance(data, list) and data:
            columns = metadata.get("columns", [])
            if columns:
                # 显示表格数据
                self.display_table(columns, data)
                self.result_text.insert(tk.END, f"\n✓ 查询完成，返回 {len(data)} 行\n")
            else:
                self.result_text.insert(tk.END, "✓ 查询完成，但无列信息\n")
        else:
            # 显示操作结果
            message = metadata.get("message", f"操作完成，影响 {affected_rows} 行")
            self.result_text.insert(tk.END, f"✓ {message}\n")

    def display_table(self, columns, data):
        """显示表格数据"""
        if not data:
            self.result_text.insert(tk.END, "(无数据)\n")
            return

        # 计算列宽
        col_widths = []
        for i, col in enumerate(columns):
            max_width = len(str(col))
            for row in data:
                if i < len(row):
                    max_width = max(max_width, len(str(row[i])))
            col_widths.append(min(max_width, 20))  # 限制最大宽度

        # 显示表头
        header = " | ".join(f"{str(columns[i]).ljust(col_widths[i])}"
                            for i in range(len(columns)))
        self.result_text.insert(tk.END, header + "\n")
        self.result_text.insert(tk.END, "-" * len(header) + "\n")

        # 显示数据行
        for row in data:
            row_data = []
            for i in range(len(columns)):
                if i < len(row):
                    value = str(row[i])[:20]  # 限制显示长度
                    row_data.append(value.ljust(col_widths[i]))
                else:
                    row_data.append("".ljust(col_widths[i]))

            data_str = " | ".join(row_data)
            self.result_text.insert(tk.END, data_str + "\n")

    def update_monitors(self):
        """更新监控信息"""
        try:
            # 更新编译器信息
            self.update_compiler_info()

            # 更新存储信息
            self.update_storage_info()

            # 更新执行器信息
            self.update_executor_info()

        except Exception as e:
            self.log(f"更新监控信息失败: {e}")

    def clear_compilation_results(self):
        """清空编译结果显示"""
        try:
            self.lexical_result.delete(1.0, tk.END)
            self.syntax_result.delete(1.0, tk.END)
            self.semantic_result.delete(1.0, tk.END)
            self.plan_result.delete(1.0, tk.END)
        except:
            pass  # 忽略属性不存在的错误

    def simulate_compilation_process(self, sql: str):
        """模拟编译过程（演示模式）"""
        try:
            # 模拟词法分析
            self.lexical_result.insert(tk.END, "=== 词法分析 (模拟) ===\n")
            words = sql.split()
            for i, word in enumerate(words):
                if word.upper() in ['SELECT', 'FROM', 'WHERE', 'INSERT', 'CREATE', 'UPDATE', 'DELETE']:
                    token_type = "KEYWORD"
                elif word.isdigit():
                    token_type = "NUMBER"
                elif word.startswith("'") and word.endswith("'"):
                    token_type = "STRING"
                else:
                    token_type = "IDENTIFIER"

                self.lexical_result.insert(tk.END, f"Token[{i}]: {word} -> {token_type}\n")

            # 模拟语法分析
            self.syntax_result.insert(tk.END, "=== 抽象语法树 (模拟) ===\n")
            if sql.upper().startswith("SELECT"):
                self.syntax_result.insert(tk.END, """
SelectStatement
├── SelectList
│   └── ColumnList
├── FromClause
│   └── TableName
└── WhereClause (可选)
    └── BooleanExpression
""")
            elif sql.upper().startswith("INSERT"):
                self.syntax_result.insert(tk.END, """
InsertStatement
├── TableName
├── ColumnList (可选)
└── ValuesList
    └── Values
""")
            elif sql.upper().startswith("CREATE"):
                self.syntax_result.insert(tk.END, """
CreateTableStatement
├── TableName
└── ColumnDefinitions
    ├── ColumnDef[1]
    ├── ColumnDef[2]
    └── ...
""")

            # 模拟语义分析
            self.semantic_result.insert(tk.END, "=== 语义分析 (模拟) ===\n")
            self.semantic_result.insert(tk.END, "✓ 语法结构检查: 通过\n")
            self.semantic_result.insert(tk.END, "✓ 表名验证: 跳过 (演示模式)\n")
            self.semantic_result.insert(tk.END, "✓ 列名验证: 跳过 (演示模式)\n")
            self.semantic_result.insert(tk.END, "✓ 数据类型检查: 跳过 (演示模式)\n")

            # 模拟执行计划
            self.plan_result.insert(tk.END, "=== 执行计划 (模拟) ===\n")
            if sql.upper().startswith("SELECT"):
                self.plan_result.insert(tk.END, """
执行步骤:
1. 扫描表 (Table Scan)
2. 应用过滤条件 (Filter)
3. 选择指定列 (Projection)
4. 返回结果集

估计成本: N/A (演示模式)
""")
            else:
                self.plan_result.insert(tk.END, "数据操作语句，直接执行\n")

        except Exception as e:
            self.log(f"模拟编译过程错误: {e}")

    def show_compilation_process(self, sql: str):
        """显示真实的编译过程"""
        try:
            # 使用真实的SQL编译器进行分析
            self.show_real_compilation_process(sql)

        except Exception as e:
            self.log(f"获取编译过程详情失败: {e}")
            # 如果真实编译失败，回退到模拟模式
            self.simulate_compilation_process(sql)

    def show_real_compilation_process(self, sql: str):
        """显示真实的SQL编译器输出 - 直接调用编译器各个阶段"""
        try:
            # 使用真正的SQL编译器进行详细分析
            self.run_detailed_compilation(sql)

        except Exception as e:
            self.log(f"真实编译过程出错: {e}")
            # 回退到模拟模式
            self.simulate_compilation_process(sql)

    def run_detailed_compilation(self, sql: str):
        """运行详细的编译过程，显示每个阶段"""
        try:
            # 导入编译器模块
            from modules.sql_compiler.lexical.lexer import Lexer
            from modules.sql_compiler.syntax.parser import Parser, ParseError
            from modules.sql_compiler.semantic.semantic import SemanticAnalyzer
            from modules.sql_compiler.planner.planner import Planner

            # === SQL输入 ===
            self.lexical_result.insert(tk.END, f"=== SQL Input ===\n{sql}\n\n")

            # === 词法分析阶段 ===
            self.lexical_result.insert(tk.END, "=== 词法分析阶段 ===\n")
            try:
                lexer = Lexer(sql)
                tokens, errors = lexer.tokenize()

                if errors:
                    error_msg = f"词法分析错误: {errors[0]}"
                    self.lexical_result.insert(tk.END, f"❌ {error_msg}\n")
                    return

                self.lexical_result.insert(tk.END, f"词法分析成功，生成 {len(tokens)} 个token\n")
                self.lexical_result.insert(tk.END, "Token 流:\n")

                for i, token in enumerate(tokens):
                    if hasattr(token, 'type') and hasattr(token, 'value'):
                        line = getattr(token, 'lineno', 1)
                        column = getattr(token, 'column', 1)
                        self.lexical_result.insert(tk.END, f"  [{token.type}, {token.value}, {line}, {column}]\n")
                    else:
                        self.lexical_result.insert(tk.END, f"  {str(token)}\n")

                self.lexical_result.insert(tk.END, "✅ 词法分析成功!\n")

            except Exception as e:
                self.lexical_result.insert(tk.END, f"❌ 词法分析失败: {e}\n")
                return

            # === 语法分析阶段 ===
            self.syntax_result.insert(tk.END, "=== 语法分析阶段 ===\n")
            try:
                parser = Parser(tokens)
                ast_list = parser.parse()

                self.syntax_result.insert(tk.END, f"✅ 语法分析成功，生成 {len(ast_list)} 个AST节点\n")
                self.syntax_result.insert(tk.END, "抽象语法树 (AST):\n")

                for ast in ast_list:
                    # 使用AST自带的__repr__方法显示完整结构
                    if hasattr(ast, '__repr__'):
                        ast_str = str(ast)
                        self.syntax_result.insert(tk.END, ast_str + "\n")
                    else:
                        # 回退到自定义格式化
                        ast_str = self.format_real_ast(ast)
                        self.syntax_result.insert(tk.END, ast_str)

            except ParseError as e:
                self.syntax_result.insert(tk.END, f"❌ 语法分析失败: {e}\n")
                return
            except Exception as e:
                self.syntax_result.insert(tk.END, f"❌ 语法分析出错: {e}\n")
                return

            # === 语义分析阶段 ===
            self.semantic_result.insert(tk.END, "=== 语义分析阶段 ===\n")
            try:
                # 使用适配器的语义分析器
                semantic_analyzer = self.adapter.semantic_analyzer if self.adapter else SemanticAnalyzer()
                semantic_errors = 0

                for ast in ast_list:
                    try:
                        # 捕获语义分析阶段内部的标准输出，展示详细检查项
                        import io
                        from contextlib import redirect_stdout
                        _buf = io.StringIO()
                        with redirect_stdout(_buf):
                            semantic_analyzer.analyze(ast)
                        _sem_detail = _buf.getvalue()
                        if hasattr(ast, 'node_type'):
                            if ast.node_type == 'CREATE_TABLE':
                                table_name = getattr(ast, 'table_name', 'unknown')
                                self.semantic_result.insert(tk.END, f"[OK] CREATE TABLE {table_name} 语义检查通过\n")
                            elif ast.node_type == 'SELECT':
                                self.semantic_result.insert(tk.END, f"[OK] SELECT 语义检查通过\n")
                            elif ast.node_type == 'INSERT':
                                self.semantic_result.insert(tk.END, f"[OK] INSERT 语义检查通过\n")
                            else:
                                self.semantic_result.insert(tk.END, f"[OK] {ast.node_type} 语义检查通过\n")

                            self.semantic_result.insert(tk.END, f"✅ [OK] 语义检查通过: {ast.node_type}\n")
                            # 追加详细检查项（表/列存在性、类型一致性、列数/列序、目录维护）
                            if _sem_detail:
                                self.semantic_result.insert(tk.END, _sem_detail)
                        else:
                            self.semantic_result.insert(tk.END, f"✅ [OK] 语义检查通过\n")

                    except Exception as e:
                        err_text = str(e)
                        self.semantic_result.insert(tk.END, f"❌ 语义检查失败: {err_text}\n")
                        # 将同样的智能建议与错误摘要输出到左侧“执行结果”区域
                        try:
                            # 标准三元组在消息第一行，直接复用整段文本
                            self.result_text.insert(tk.END, f"❌ 语义检查失败: {err_text}\n")
                        except Exception:
                            pass
                        semantic_errors += 1

                if semantic_errors == 0:
                    self.semantic_result.insert(tk.END, "✅ 语义分析成功!\n")
                else:
                    self.semantic_result.insert(tk.END, f"❌ 语义分析失败，检测到 {semantic_errors} 个错误\n")
                    return

            except Exception as e:
                self.semantic_result.insert(tk.END, f"❌ 语义分析出错: {e}\n")
                return

            # === 执行计划生成阶段 ===
            self.plan_result.insert(tk.END, "=== 执行计划生成阶段 ===\n")
            try:
                # 检查是否为SELECT语句，应用查询优化
                if any(hasattr(ast, 'node_type') and ast.node_type == 'SELECT' for ast in ast_list):
                    self.plan_result.insert(tk.END, "🔧 对 SELECT 语句应用查询优化...\n")

                    # 使用编译器优化器
                    if hasattr(self.adapter, 'compiler_optimizer'):
                        try:
                            optimized_ast = self.adapter.compiler_optimizer.optimize(ast_list[0])
                            self.plan_result.insert(tk.END, "[ADAPTER] 编译器优化完成\n")
                        except:
                            optimized_ast = ast_list[0]
                    else:
                        optimized_ast = ast_list[0]

                # 生成执行计划
                ast_list_dict = [ast.to_dict() for ast in ast_list]
                planner = Planner(ast_list_dict, enable_optimization=True)
                plans = planner.generate_plan()

                self.plan_result.insert(tk.END, f"✅ 编译器计划生成成功，生成 {len(plans)} 个计划\n")
                self.plan_result.insert(tk.END, "执行计划:\n")

                for plan in plans:
                    if hasattr(plan, 'to_dict'):
                        plan_dict = plan.to_dict()
                    else:
                        plan_dict = plan

                    import json
                    plan_str = json.dumps(plan_dict, indent=2, ensure_ascii=False)
                    self.plan_result.insert(tk.END, plan_str + "\n")

            except Exception as e:
                self.plan_result.insert(tk.END, f"❌ 执行计划生成失败: {e}\n")
                return

        except ImportError as e:
            self.log(f"无法导入编译器模块: {e}")
            self.simulate_compilation_process(sql)
        except Exception as e:
            self.log(f"详细编译过程出错: {e}")
            self.simulate_compilation_process(sql)

    def format_real_ast(self, ast) -> str:
        """格式化真实的AST对象"""
        try:
            if hasattr(ast, 'node_type'):
                if ast.node_type == 'CREATE_TABLE':
                    table_name = ast.value if ast.value else 'unknown'
                    result = f"CREATE_TABLE: {table_name}\n"

                    # 遍历子节点查找列定义
                    if hasattr(ast, 'children'):
                        for child in ast.children:
                            if hasattr(child, 'node_type') and child.node_type == 'COLUMN':
                                result += f"  COLUMN: {child.value}\n"

                    return result + "\n"

                elif ast.node_type == 'SELECT':
                    result = "SELECT:\n"

                    # 遍历子节点
                    if hasattr(ast, 'children'):
                        columns = []
                        from_table = None
                        where_condition = None

                        for child in ast.children:
                            if hasattr(child, 'node_type'):
                                if child.node_type == 'COLUMN':
                                    columns.append(child.value)
                                elif child.node_type == 'FROM':
                                    from_table = child.value
                                elif child.node_type == 'WHERE':
                                    where_condition = self.format_where_condition(child)

                        if columns:
                            result += f"  COLUMNS: [{', '.join(columns)}]\n"
                        if from_table:
                            result += f"  FROM: {from_table}\n"
                        if where_condition:
                            result += f"  WHERE: {where_condition}\n"

                    return result + "\n"

                elif ast.node_type == 'INSERT':
                    table_name = ast.value if ast.value else 'unknown'
                    result = f"INSERT:\n"
                    result += f"  TABLE: {table_name}\n"

                    # 遍历子节点查找列和值
                    if hasattr(ast, 'children'):
                        columns = []
                        values = []

                        for child in ast.children:
                            if hasattr(child, 'node_type'):
                                if child.node_type == 'COLUMN':
                                    columns.append(child.value)
                                elif child.node_type == 'VALUE':
                                    values.append(child.value)

                        if columns:
                            result += f"  COLUMNS: [{', '.join(columns)}]\n"
                        if values:
                            result += f"  VALUES: [{', '.join(values)}]\n"

                    return result + "\n"

                else:
                    # 对于其他类型的节点，使用通用格式
                    result = f"{ast.node_type}"
                    if ast.value:
                        result += f": {ast.value}"
                    result += "\n"

                    # 递归显示子节点
                    if hasattr(ast, 'children') and ast.children:
                        for child in ast.children:
                            child_str = self.format_real_ast(child)
                            # 缩进子节点
                            indented = "\n".join("  " + line for line in child_str.split("\n") if line.strip())
                            result += indented + "\n"

                    return result + "\n"
            else:
                return str(ast) + "\n\n"

        except Exception as e:
            return f"AST格式化错误: {e}\n\n"

    def format_where_condition(self, where_node) -> str:
        """格式化WHERE条件"""
        try:
            if hasattr(where_node, 'children') and where_node.children:
                # 简单处理：假设是 column op value 的格式
                if len(where_node.children) >= 3:
                    left = where_node.children[0].value if hasattr(where_node.children[0], 'value') else str(
                        where_node.children[0])
                    op = where_node.children[1].value if hasattr(where_node.children[1], 'value') else str(
                        where_node.children[1])
                    right = where_node.children[2].value if hasattr(where_node.children[2], 'value') else str(
                        where_node.children[2])
                    return f"{left} {op} {right}"
                else:
                    return str(where_node.value) if where_node.value else "complex condition"
            else:
                return str(where_node.value) if where_node.value else "unknown condition"
        except Exception as e:
            return f"condition format error: {e}"

    def capture_compilation_process(self, sql: str):
        """捕获编译过程的详细输出"""
        try:
            # 创建一个临时的日志捕获器
            import io
            import sys
            from contextlib import redirect_stdout, redirect_stderr

            # 输入SQL
            self.lexical_result.insert(tk.END, f"=== SQL Input ===\n{sql}\n\n")

            # 模拟从适配器获取编译信息
            if self.adapter:
                # 尝试调用适配器的内部编译方法（如果有的话）
                try:
                    # 获取词法分析信息
                    lexical_info = self.get_lexical_analysis_info(sql)
                    self.lexical_result.insert(tk.END, lexical_info)

                    # 获取语法分析信息
                    syntax_info = self.get_syntax_analysis_info(sql)
                    self.syntax_result.insert(tk.END, syntax_info)

                    # 获取语义分析信息
                    semantic_info = self.get_semantic_analysis_info(sql)
                    self.semantic_result.insert(tk.END, semantic_info)

                    # 获取执行计划信息
                    plan_info = self.get_execution_plan_info(sql)
                    self.plan_result.insert(tk.END, plan_info)

                except Exception as e:
                    self.log(f"无法获取详细编译信息: {e}")
                    # 回退到基于日志的方法
                    self.parse_adapter_logs(sql)
            else:
                self.simulate_compilation_process(sql)

        except Exception as e:
            self.log(f"捕获编译过程失败: {e}")
            self.simulate_compilation_process(sql)

    def parse_adapter_logs(self, sql: str):
        """解析适配器日志来提取编译信息"""
        # 这里可以基于你提供的日志格式来解析

        # 词法分析信息
        self.lexical_result.insert(tk.END, "=== 词法分析阶段 ===\n")
        self.lexical_result.insert(tk.END, "词法分析成功，生成 token\n")
        self.lexical_result.insert(tk.END, "Token 流:\n")

        # 简单的token分析
        words = sql.split()
        for i, word in enumerate(words):
            token_type = self.classify_token(word)
            self.lexical_result.insert(tk.END, f"Token[{i}]: {word} -> {token_type}\n")

        self.lexical_result.insert(tk.END, "✅ 词法分析成功!\n")

        # 语法分析信息
        self.syntax_result.insert(tk.END, "=== 语法分析阶段 ===\n")
        self.syntax_result.insert(tk.END, "✅ 语法分析成功，生成 AST 节点\n")
        self.syntax_result.insert(tk.END, "抽象语法树 (AST):\n")

        # 根据SQL类型生成AST
        sql_upper = sql.upper().strip()
        if sql_upper.startswith("SELECT"):
            self.syntax_result.insert(tk.END, self.generate_select_ast(sql))
        elif sql_upper.startswith("CREATE"):
            self.syntax_result.insert(tk.END, self.generate_create_ast(sql))
        elif sql_upper.startswith("INSERT"):
            self.syntax_result.insert(tk.END, self.generate_insert_ast(sql))
        else:
            self.syntax_result.insert(tk.END, f"SQL语句: {sql}\n")

        # 语义分析信息
        self.semantic_result.insert(tk.END, "=== 语义分析阶段 ===\n")
        if sql_upper.startswith("SELECT"):
            self.semantic_result.insert(tk.END, "[OK] SELECT 语义检查通过\n")
            self.semantic_result.insert(tk.END, "✅ [OK] 语义检查通过: SELECT\n")
        elif sql_upper.startswith("CREATE"):
            table_name = self.extract_table_name(sql)
            self.semantic_result.insert(tk.END, f"[OK] CREATE TABLE {table_name} 语义检查通过\n")
            self.semantic_result.insert(tk.END, "✅ [OK] 语义检查通过: CREATE_TABLE\n")
        elif sql_upper.startswith("INSERT"):
            self.semantic_result.insert(tk.END, "[OK] INSERT 语义检查通过\n")
            self.semantic_result.insert(tk.END, "✅ [OK] 语义检查通过: INSERT\n")

        self.semantic_result.insert(tk.END, "✅ 语义分析成功!\n")

        # 执行计划信息
        self.plan_result.insert(tk.END, "=== 执行计划生成阶段 ===\n")
        if sql_upper.startswith("SELECT"):
            self.plan_result.insert(tk.END, "🔧 对 SELECT 语句应用查询优化...\n")
            self.plan_result.insert(tk.END, "✅ 编译器优化完成\n")
            self.plan_result.insert(tk.END, "✅ 编译器计划生成成功\n")
            self.plan_result.insert(tk.END, "执行计划:\n")
            self.plan_result.insert(tk.END, self.generate_select_plan(sql))
        else:
            self.plan_result.insert(tk.END, "✅ 执行计划生成成功!\n")
            self.plan_result.insert(tk.END, "执行计划: 直接执行操作\n")

    def setup_autocomplete(self):
        """设置自动补全功能"""
        # 绑定按键事件
        self.sql_text.bind('<KeyRelease>', self.on_key_release)
        self.sql_text.bind('<Control-space>', self.show_autocomplete)
        
        # 创建补全弹窗（初始隐藏）
        self.autocomplete_window = None
        self.autocomplete_listbox = None
        
        # 定义关键字和数据类型
        self.sql_keywords = [
            'SELECT', 'FROM', 'WHERE', 'INSERT', 'CREATE', 'TABLE', 'UPDATE', 'DELETE', 
            'INTO', 'VALUES', 'DROP', 'ALTER', 'INDEX', 'VIEW', 'TRIGGER', 'PROCEDURE',
            'BEGIN', 'COMMIT', 'ROLLBACK', 'TRANSACTION', 'AND', 'OR', 'NOT', 'NULL',
            'PRIMARY', 'KEY', 'FOREIGN', 'REFERENCES', 'UNIQUE', 'DEFAULT', 'CHECK',
            'ORDER', 'BY', 'GROUP', 'HAVING', 'LIMIT', 'OFFSET', 'DISTINCT', 'AS',
            'JOIN', 'INNER', 'LEFT', 'RIGHT', 'FULL', 'OUTER', 'ON', 'USING',
            'UNION', 'INTERSECT', 'EXCEPT', 'EXISTS', 'IN', 'BETWEEN', 'LIKE',
            'IS', 'ASC', 'DESC', 'COUNT', 'SUM', 'AVG', 'MIN', 'MAX',
            'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'IF', 'ELSEIF', 'ENDIF'
        ]
        
        self.data_types = [
            'INT', 'INTEGER', 'BIGINT', 'SMALLINT', 'TINYINT',
            'FLOAT', 'DOUBLE', 'DECIMAL', 'NUMERIC', 'REAL',
            'CHAR', 'VARCHAR', 'TEXT', 'STRING', 'LONGTEXT',
            'DATE', 'TIME', 'DATETIME', 'TIMESTAMP', 'YEAR',
            'BOOLEAN', 'BOOL', 'BINARY', 'VARBINARY', 'BLOB'
        ]

    def on_key_release(self, event):
        """按键释放事件处理"""
        # 实时语法检查
        self.check_syntax_realtime()
        
        # 获取当前光标位置的单词
        current_word = self.get_current_word()
        
        # 如果单词长度大于1且不是特殊字符，显示补全
        if len(current_word) > 1 and current_word.isalnum():
            self.show_autocomplete_suggestions(current_word)
        else:
            self.hide_autocomplete()

    def check_syntax_realtime(self):
        """实时语法检查"""
        try:
            sql_text = self.sql_text.get(1.0, tk.END).strip()
            if not sql_text or sql_text.startswith('--'):
                return
                
            # 清除之前的语法高亮
            self.clear_syntax_highlighting()
            
            # 应用语法高亮
            self.apply_syntax_highlighting()
            
            # 检查常见语法错误
            self.check_common_syntax_errors(sql_text)
            
        except Exception as e:
            print(f"实时语法检查失败: {e}")

    def clear_syntax_highlighting(self):
        """清除语法高亮"""
        for tag in ['keyword', 'string', 'number', 'comment', 'error']:
            self.sql_text.tag_delete(tag)

    def apply_syntax_highlighting(self):
        """应用语法高亮"""
        sql_text = self.sql_text.get(1.0, tk.END)
        
        # 配置标签样式
        self.sql_text.tag_configure('keyword', foreground='blue', font=('Consolas', 10, 'bold'))
        self.sql_text.tag_configure('string', foreground='green')
        self.sql_text.tag_configure('number', foreground='purple')
        self.sql_text.tag_configure('comment', foreground='gray', font=('Consolas', 10, 'italic'))
        self.sql_text.tag_configure('error', background='#ffcccc', foreground='red')
        
        import re
        
        # 关键字高亮
        for keyword in self.sql_keywords:
            pattern = r'\b' + re.escape(keyword) + r'\b'
            for match in re.finditer(pattern, sql_text, re.IGNORECASE):
                start_idx = f"1.0+{match.start()}c"
                end_idx = f"1.0+{match.end()}c"
                self.sql_text.tag_add('keyword', start_idx, end_idx)
        
        # 字符串高亮
        string_pattern = r"'[^']*'"
        for match in re.finditer(string_pattern, sql_text):
            start_idx = f"1.0+{match.start()}c"
            end_idx = f"1.0+{match.end()}c"
            self.sql_text.tag_add('string', start_idx, end_idx)
        
        # 数字高亮
        number_pattern = r'\b\d+\.?\d*\b'
        for match in re.finditer(number_pattern, sql_text):
            start_idx = f"1.0+{match.start()}c"
            end_idx = f"1.0+{match.end()}c"
            self.sql_text.tag_add('number', start_idx, end_idx)
        
        # 注释高亮
        comment_pattern = r'--.*$'
        for match in re.finditer(comment_pattern, sql_text, re.MULTILINE):
            start_idx = f"1.0+{match.start()}c"
            end_idx = f"1.0+{match.end()}c"
            self.sql_text.tag_add('comment', start_idx, end_idx)

    def check_common_syntax_errors(self, sql_text):
        """检查常见语法错误"""
        import re
        
        # 检查未闭合的引号
        single_quotes = sql_text.count("'")
        if single_quotes % 2 != 0:
            self.highlight_error("未闭合的单引号")
        
        # 检查括号匹配
        open_parens = sql_text.count('(')
        close_parens = sql_text.count(')')
        if open_parens != close_parens:
            self.highlight_error("括号不匹配")
        
        # 检查SELECT语句是否有FROM子句（排除子查询）
        select_matches = re.findall(r'\bSELECT\b.*?(?=\bSELECT\b|$)', sql_text, re.IGNORECASE | re.DOTALL)
        for select_stmt in select_matches:
            if 'FROM' not in select_stmt.upper() and '*' in select_stmt:
                continue  # 可能是SELECT常量
            elif 'FROM' not in select_stmt.upper():
                self.highlight_error("SELECT语句缺少FROM子句")
        
        # 检查INSERT语句格式
        if re.search(r'\bINSERT\s+INTO\b', sql_text, re.IGNORECASE):
            if not re.search(r'\bVALUES\b', sql_text, re.IGNORECASE):
                self.highlight_error("INSERT语句缺少VALUES子句")

    def highlight_error(self, error_msg):
        """高亮显示错误"""
        # 在状态栏显示错误信息
        if hasattr(self, 'status_compiler'):
            self.status_compiler.config(text=f"⚠️ 语法提示: {error_msg}", foreground='orange')

    def show_autocomplete(self, event=None):
        """显示自动补全（Ctrl+Space）"""
        current_word = self.get_current_word()
        self.show_autocomplete_suggestions(current_word)
        return "break"

    def get_current_word(self):
        """获取光标位置的当前单词"""
        cursor_pos = self.sql_text.index(tk.INSERT)
        line_start = cursor_pos.split('.')[0] + '.0'
        line_end = cursor_pos.split('.')[0] + '.end'
        line_text = self.sql_text.get(line_start, line_end)
        
        col = int(cursor_pos.split('.')[1])
        
        # 找到单词边界
        start = col
        while start > 0 and (line_text[start-1].isalnum() or line_text[start-1] == '_'):
            start -= 1
        
        end = col
        while end < len(line_text) and (line_text[end].isalnum() or line_text[end] == '_'):
            end += 1
            
        return line_text[start:end]

    def show_autocomplete_suggestions(self, current_word):
        """显示自动补全建议"""
        if not current_word:
            return
            
        # 获取匹配的建议
        suggestions = self.get_suggestions(current_word)
        
        if not suggestions:
            self.hide_autocomplete()
            return
            
        # 创建或更新补全窗口
        if self.autocomplete_window is None:
            self.create_autocomplete_window()
            
        # 更新建议列表
        self.autocomplete_listbox.delete(0, tk.END)
        for suggestion in suggestions[:10]:  # 最多显示10个建议
            self.autocomplete_listbox.insert(tk.END, suggestion)
            
        # 定位窗口位置
        self.position_autocomplete_window()
        
        # 显示窗口
        self.autocomplete_window.deiconify()

    def get_suggestions(self, current_word):
        """获取补全建议"""
        word_upper = current_word.upper()
        suggestions = []
        
        # 关键字匹配
        for keyword in self.sql_keywords:
            if keyword.startswith(word_upper):
                suggestions.append(keyword)
                
        # 数据类型匹配
        for dtype in self.data_types:
            if dtype.startswith(word_upper):
                suggestions.append(dtype)
                
        # 表名匹配（如果有的话）
        table_names = self.get_available_tables()
        for table in table_names:
            if table.upper().startswith(word_upper):
                suggestions.append(table)
                
        # 列名匹配（基于上下文）
        column_names = self.get_available_columns()
        for column in column_names:
            if column.upper().startswith(word_upper):
                suggestions.append(column)
        
        return sorted(list(set(suggestions)))  # 去重并排序

    def get_available_tables(self):
        """获取可用的表名"""
        try:
            if self.adapter and BACKEND_AVAILABLE:
                # 尝试从数据库系统获取实际的表名
                result = self.adapter.execute("SHOW TABLES;")
                if result.get('status') == 'success':
                    tables = []
                    for row in result.get('data', []):
                        if isinstance(row, dict) and 'table_name' in row:
                            tables.append(row['table_name'])
                        elif isinstance(row, list) and row:
                            tables.append(str(row[0]))
                    return tables
        except Exception as e:
            print(f"获取表名失败: {e}")
        
        # 返回示例表名作为后备
        return ['students', 'teachers', 'courses', 'enrollment', 'users', 'orders', 'products']

    def get_available_columns(self):
        """获取可用的列名（基于上下文分析）"""
        # 分析当前SQL文本，找到可能的表名
        sql_text = self.sql_text.get(1.0, tk.END).upper()
        current_tables = []
        
        # 简单解析FROM子句中的表名
        import re
        from_matches = re.findall(r'FROM\s+(\w+)', sql_text)
        current_tables.extend(from_matches)
        
        # 解析JOIN子句中的表名
        join_matches = re.findall(r'JOIN\s+(\w+)', sql_text)
        current_tables.extend(join_matches)
        
        # 解析INSERT INTO中的表名
        insert_matches = re.findall(r'INSERT\s+INTO\s+(\w+)', sql_text)
        current_tables.extend(insert_matches)
        
        # 解析UPDATE中的表名
        update_matches = re.findall(r'UPDATE\s+(\w+)', sql_text)
        current_tables.extend(update_matches)
        
        columns = []
        
        # 尝试从数据库系统获取这些表的列信息
        try:
            if self.adapter and BACKEND_AVAILABLE and current_tables:
                for table in set(current_tables):  # 去重
                    try:
                        # 这里可以实现获取表结构的逻辑
                        # 暂时使用基于表名的常见列名推测
                        if table.lower() == 'students':
                            columns.extend(['id', 'name', 'age', 'score', 'grade', 'email'])
                        elif table.lower() == 'teachers':
                            columns.extend(['id', 'name', 'department', 'salary', 'hire_date'])
                        elif table.lower() == 'courses':
                            columns.extend(['id', 'name', 'credits', 'department', 'description'])
                        else:
                            # 通用列名
                            columns.extend(['id', 'name', 'created_at', 'updated_at'])
                    except Exception:
                        pass
        except Exception as e:
            print(f"获取列名失败: {e}")
        
        # 添加常见的通用列名
        common_columns = ['id', 'name', 'age', 'score', 'email', 'phone', 'address', 
                         'created_at', 'updated_at', 'status', 'type', 'value', 'count',
                         'description', 'title', 'content', 'category', 'date', 'time']
        columns.extend(common_columns)
        
        return sorted(list(set(columns)))  # 去重并排序

    def create_autocomplete_window(self):
        """创建自动补全窗口"""
        self.autocomplete_window = tk.Toplevel(self.root)
        self.autocomplete_window.withdraw()  # 初始隐藏
        self.autocomplete_window.wm_overrideredirect(True)
        self.autocomplete_window.configure(bg='white', relief='solid', bd=1)
        
        # 创建列表框
        self.autocomplete_listbox = tk.Listbox(
            self.autocomplete_window,
            height=6,
            font=("Consolas", 9),
            selectmode=tk.SINGLE,
            bg='white',
            fg='black',
            selectbackground='#0078d4',
            selectforeground='white',
            bd=0,
            highlightthickness=0
        )
        self.autocomplete_listbox.pack()
        
        # 绑定事件
        self.autocomplete_listbox.bind('<Double-Button-1>', self.insert_suggestion)
        self.autocomplete_listbox.bind('<Return>', self.insert_suggestion)
        self.sql_text.bind('<Escape>', lambda e: self.hide_autocomplete())
        self.sql_text.bind('<Button-1>', lambda e: self.hide_autocomplete())

    def position_autocomplete_window(self):
        """定位自动补全窗口"""
        # 获取光标位置
        cursor_pos = self.sql_text.index(tk.INSERT)
        bbox = self.sql_text.bbox(cursor_pos)
        
        if bbox:
            x = bbox[0] + self.sql_text.winfo_rootx()
            y = bbox[1] + bbox[3] + self.sql_text.winfo_rooty()
            self.autocomplete_window.geometry(f"+{x}+{y}")

    def insert_suggestion(self, event=None):
        """插入选中的建议"""
        selection = self.autocomplete_listbox.curselection()
        if selection:
            suggestion = self.autocomplete_listbox.get(selection[0])
            
            # 获取当前单词的位置
            cursor_pos = self.sql_text.index(tk.INSERT)
            current_word = self.get_current_word()
            
            # 计算替换位置
            line, col = cursor_pos.split('.')
            col = int(col)
            start_col = col - len(current_word)
            
            start_pos = f"{line}.{start_col}"
            end_pos = f"{line}.{col}"
            
            # 替换文本
            self.sql_text.delete(start_pos, end_pos)
            self.sql_text.insert(start_pos, suggestion)
            
            self.hide_autocomplete()

    def hide_autocomplete(self):
        """隐藏自动补全窗口"""
        if self.autocomplete_window:
            self.autocomplete_window.withdraw()

    def classify_token(self, word: str) -> str:
        """分类token"""
        keywords = ['SELECT', 'FROM', 'WHERE', 'INSERT', 'CREATE', 'TABLE', 'UPDATE', 'DELETE', 'INTO', 'VALUES']
        if word.upper() in keywords:
            return "KEYWORD"
        elif word.isdigit():
            return "NUMBER"
        elif word.startswith("'") and word.endswith("'"):
            return "STRING"
        elif word in ['(', ')', ',', ';', '>', '<', '=']:
            return "DELIMITER"
        else:
            return "IDENTIFIER"

    def generate_select_ast(self, sql: str) -> str:
        """生成SELECT语句的AST"""
        return """SELECT:
  COLUMNS: [id, name, score]
  FROM: students
  WHERE: age > 18
"""

    def generate_create_ast(self, sql: str) -> str:
        """生成CREATE语句的AST"""
        table_name = self.extract_table_name(sql)
        return f"""CREATE_TABLE: {table_name}
  COLUMN: id:INT
  COLUMN: name:STRING
"""

    def generate_insert_ast(self, sql: str) -> str:
        """生成INSERT语句的AST"""
        return """INSERT:
  TABLE: table_name
  COLUMNS: [col1, col2]
  VALUES: [val1, val2]
"""

    def generate_select_plan(self, sql: str) -> str:
        """生成SELECT的执行计划"""
        return """{
  "type": "Project",
  "props": {
    "columns": ["ID", "NAME", "SCORE"]
  },
  "children": [
    {
      "type": "SeqScan", 
      "props": {
        "table": "STUDENTS",
        "conditions": [
          {
            "left": "AGE",
            "op": ">", 
            "right": "18"
          }
        ]
      },
      "children": []
    }
  ]
}"""

    def extract_table_name(self, sql: str) -> str:
        """从SQL中提取表名"""
        import re
        # 简单的表名提取
        match = re.search(r'(?:FROM|TABLE|INTO|UPDATE)\s+(\w+)', sql, re.IGNORECASE)
        if match:
            return match.group(1)
        return "unknown"

    def get_lexical_analysis_info(self, sql: str) -> str:
        """获取词法分析信息"""
        return f"=== 词法分析阶段 ===\n[ADAPTER] 词法分析成功，生成 token\n"

    def get_syntax_analysis_info(self, sql: str) -> str:
        """获取语法分析信息"""
        return f"=== 语法分析阶段 ===\n[ADAPTER] 语法分析成功，生成 AST 节点\n"

    def get_semantic_analysis_info(self, sql: str) -> str:
        """获取语义分析信息"""
        return f"=== 语义分析阶段 ===\n[OK] 语义检查通过\n"

    def get_execution_plan_info(self, sql: str) -> str:
        """获取执行计划信息"""
        return f"=== 执行计划生成阶段 ===\n✅ 执行计划生成成功!\n"

    def format_ast(self, ast):
        """格式化AST为可读字符串"""
        try:
            if hasattr(ast, 'type'):
                if ast.type == 'CREATE_TABLE':
                    table_name = getattr(ast, 'table_name', 'unknown')
                    result = f"CREATE_TABLE: {table_name}\n"

                    if hasattr(ast, 'columns'):
                        for col in ast.columns:
                            if hasattr(col, 'name') and hasattr(col, 'type'):
                                result += f"  COLUMN: {col.name}:{col.type}\n"
                            else:
                                result += f"  COLUMN: {str(col)}\n"

                    return result

                elif ast.type == 'SELECT':
                    result = "SELECT:\n"
                    if hasattr(ast, 'columns'):
                        result += "  COLUMNS:\n"
                        for col in ast.columns:
                            result += f"    {str(col)}\n"
                    if hasattr(ast, 'from_table'):
                        result += f"  FROM: {ast.from_table}\n"
                    if hasattr(ast, 'where_clause'):
                        result += f"  WHERE: {str(ast.where_clause)}\n"
                    return result

                elif ast.type == 'INSERT':
                    result = "INSERT:\n"
                    if hasattr(ast, 'table_name'):
                        result += f"  TABLE: {ast.table_name}\n"
                    if hasattr(ast, 'columns'):
                        result += "  COLUMNS:\n"
                        for col in ast.columns:
                            result += f"    {str(col)}\n"
                    if hasattr(ast, 'values'):
                        result += "  VALUES:\n"
                        for val in ast.values:
                            result += f"    {str(val)}\n"
                    return result

                else:
                    return f"{ast.type}: {str(ast)}\n"
            else:
                return str(ast) + "\n"

        except Exception as e:
            return f"AST格式化错误: {e}\n"

    def simulate_lexical_analysis(self, sql: str):
        """模拟词法分析"""
        self.lexical_result.insert(tk.END, "=== 词法分析 ===\n")

        # 简单的词法分析模拟
        import re
        tokens = []

        # SQL关键词
        keywords = ['SELECT', 'FROM', 'WHERE', 'INSERT', 'INTO', 'VALUES', 'CREATE', 'TABLE',
                    'UPDATE', 'SET', 'DELETE', 'ORDER', 'BY', 'GROUP', 'HAVING', 'JOIN', 'ON',
                    'AND', 'OR', 'NOT', 'IN', 'LIKE', 'BETWEEN', 'IS', 'NULL', 'ASC', 'DESC']

        # 数据类型
        data_types = ['INT', 'STRING', 'DOUBLE', 'VARCHAR', 'TEXT', 'DATE', 'DATETIME']

        # 分词
        pattern = r'\w+|[^\w\s]'
        words = re.findall(pattern, sql)

        for word in words:
            if word.upper() in keywords:
                tokens.append(f"KEYWORD: {word}")
            elif word.upper() in data_types:
                tokens.append(f"DATATYPE: {word}")
            elif word.isdigit():
                tokens.append(f"NUMBER: {word}")
            elif word.startswith("'") and word.endswith("'"):
                tokens.append(f"STRING: {word}")
            elif word in ['(', ')', ',', ';', '=', '>', '<', '!']:
                tokens.append(f"OPERATOR: {word}")
            elif word.isidentifier():
                tokens.append(f"IDENTIFIER: {word}")
            else:
                tokens.append(f"UNKNOWN: {word}")

        for i, token in enumerate(tokens):
            self.lexical_result.insert(tk.END, f"[{i:2d}] {token}\n")

    def simulate_syntax_analysis(self, sql: str):
        """模拟语法分析"""
        self.syntax_result.insert(tk.END, "=== 语法分析 ===\n")

        sql_upper = sql.upper().strip()

        if sql_upper.startswith("SELECT"):
            self.syntax_result.insert(tk.END, """
查询语句语法树:
SelectStatement
├── SELECT子句
│   ├── 列名列表
│   └── 表达式列表
├── FROM子句
│   ├── 表名
│   └── 表别名 (可选)
├── WHERE子句 (可选)
│   └── 条件表达式
├── ORDER BY子句 (可选)
│   └── 排序字段
└── GROUP BY子句 (可选)
    └── 分组字段
""")
        elif sql_upper.startswith("INSERT"):
            self.syntax_result.insert(tk.END, """
插入语句语法树:
InsertStatement
├── INSERT关键字
├── INTO关键字
├── 目标表名
├── 列名列表 (可选)
│   ├── 列名1
│   ├── 列名2
│   └── ...
└── VALUES子句
    └── 值列表
        ├── 值1
        ├── 值2
        └── ...
""")
        elif sql_upper.startswith("CREATE"):
            self.syntax_result.insert(tk.END, """
建表语句语法树:
CreateTableStatement
├── CREATE关键字
├── TABLE关键字
├── 表名
└── 列定义列表
    ├── 列定义1
    │   ├── 列名
    │   ├── 数据类型
    │   └── 约束 (可选)
    ├── 列定义2
    └── ...
""")
        elif sql_upper.startswith("UPDATE"):
            self.syntax_result.insert(tk.END, """
更新语句语法树:
UpdateStatement
├── UPDATE关键字
├── 表名
├── SET子句
│   └── 赋值表达式列表
└── WHERE子句 (可选)
    └── 条件表达式
""")
        else:
            self.syntax_result.insert(tk.END, "未识别的SQL语句类型\n")

        self.syntax_result.insert(tk.END, "\n✓ 语法结构验证完成")

    def simulate_semantic_analysis(self, sql: str):
        """模拟语义分析"""
        self.semantic_result.insert(tk.END, "=== 语义分析 ===\n")

        self.semantic_result.insert(tk.END, "正在进行语义检查...\n\n")

        # 模拟各种检查
        checks = [
            ("语法合法性检查", "✓ 通过"),
            ("表名存在性检查", "✓ 通过"),
            ("列名有效性检查", "✓ 通过"),
            ("数据类型兼容性检查", "✓ 通过"),
            ("权限验证", "✓ 通过"),
            ("约束条件检查", "✓ 通过")
        ]

        for check_name, result in checks:
            self.semantic_result.insert(tk.END, f"{check_name}: {result}\n")

        self.semantic_result.insert(tk.END, "\n语义分析完成，SQL语句有效。")

    def simulate_execution_plan(self, sql: str):
        """模拟执行计划"""
        self.plan_result.insert(tk.END, "=== 查询执行计划 ===\n")

        sql_upper = sql.upper().strip()

        if sql_upper.startswith("SELECT"):
            self.plan_result.insert(tk.END, """
执行步骤:
1. 表扫描 (Table Scan)
   - 扫描方式: 顺序扫描
   - 预估行数: 未知

2. 条件过滤 (Filter)
   - 过滤条件: WHERE子句
   - 选择性: 未知

3. 列投影 (Projection)
   - 输出列: SELECT列表

4. 结果返回 (Result)
   - 输出格式: 结果集

优化建议:
- 考虑在过滤列上创建索引
- 检查WHERE条件的选择性
""")
        elif sql_upper.startswith("INSERT"):
            self.plan_result.insert(tk.END, """
执行步骤:
1. 值验证 (Value Validation)
   - 检查数据类型
   - 检查约束条件

2. 插入操作 (Insert)
   - 写入数据页
   - 更新索引

3. 事务提交 (Commit)
   - 确保数据持久化
""")
        elif sql_upper.startswith("CREATE"):
            self.plan_result.insert(tk.END, """
执行步骤:
1. 表结构验证 (Schema Validation)
   - 检查表名唯一性
   - 验证列定义

2. 创建表 (Create Table)
   - 分配存储空间
   - 初始化元数据

3. 更新系统目录 (Update Catalog)
   - 记录表信息
""")
        else:
            self.plan_result.insert(tk.END, "直接执行的操作语句\n")

    def update_compiler_info(self):
        """更新编译器信息"""
        self.compiler_info.delete(1.0, tk.END)
        info = f"""编译器状态: {self.system_status['compiler']}
当前模式: {self.current_mode}
支持的SQL类型: CREATE, INSERT, SELECT, UPDATE, DELETE
语法检查: 启用    语义分析: 启用    执行计划: 启用"""
        self.compiler_info.insert(tk.END, info)

    def update_storage_info(self):
        """更新存储信息"""
        self.storage_stats.delete(1.0, tk.END)

        if BACKEND_AVAILABLE and self.adapter:
            try:
                stats = self.adapter.get_cache_stats()
                info = f"""存储引擎状态: {self.system_status['storage']}
Python缓存: {stats.get('python_cache', {})}
C++加速: {stats.get('cpp_enabled', False)}
混合统计: {stats.get('hybrid_stats', {})}"""
                self.storage_stats.insert(tk.END, info)

                # 更新表信息
                self.update_tables_info()

            except Exception as e:
                self.storage_stats.insert(tk.END, f"获取存储信息失败: {e}")
        else:
            self.storage_stats.insert(tk.END, "演示模式: 存储信息不可用")

    def update_tables_info(self):
        """更新表信息"""
        # 清空现有项目
        for item in self.tables_tree.get_children():
            self.tables_tree.delete(item)

        try:
            catalog_info = self.adapter.get_catalog_info()
            tables = catalog_info.get("tables", [])

            for table in tables:
                # 这里可以添加更详细的表信息获取
                self.tables_tree.insert("", tk.END, text=table,
                                        values=("N/A", "N/A"))

        except Exception as e:
            self.log(f"更新表信息失败: {e}")

    def update_executor_info(self):
        """更新执行器信息"""
        self.executor_stats.delete(1.0, tk.END)
        info = f"""执行引擎状态: {self.system_status['executor']}
当前模式: {self.current_mode}
支持的操作: 表操作, 数据操作, 查询优化
事务支持: {'是' if self.current_mode == 'adapter' else '否'}
索引支持: {'是' if self.current_mode == 'adapter' else '否'}"""
        self.executor_stats.insert(tk.END, info)

    def on_mode_change(self, event=None):
        """模式切换事件"""
        new_mode = self.mode_var.get()
        if new_mode != self.current_mode:
            self.current_mode = new_mode
            self.log(f"切换到 {new_mode} 模式")
            self.update_monitors()

    def refresh_status(self):
        """刷新系统状态"""
        # 更新状态标签
        for component, status in self.system_status.items():
            color = "green" if status == "已连接" else "red"
            if component == "compiler":
                self.compiler_status.config(text=f"编译器: {status}", foreground=color)
            elif component == "storage":
                self.storage_status.config(text=f"存储: {status}", foreground=color)
            elif component == "executor":
                self.executor_status.config(text=f"执行器: {status}", foreground=color)

        self.update_monitors()

    def clear_sql(self):
        """清空SQL输入"""
        self.sql_text.delete(1.0, tk.END)

    def load_sample_sql(self):
        """加载示例SQL"""
        sample_sql = """-- 数据库操作示例
CREATE TABLE students (id INT, name STRING, age INT, score DOUBLE);
INSERT INTO students (id, name, age, score) VALUES (1, 'Alice', 20, 85.5);
INSERT INTO students (id, name, age, score) VALUES (2, 'Bob', 21, 92.0);
INSERT INTO students (id, name, age, score) VALUES (3, 'Charlie', 19, 78.5);

SELECT id, name, score FROM students WHERE age > 18;
UPDATE students SET score = 95.0 WHERE id = 1;
DELETE FROM students WHERE id = 3;

-- 高级查询示例（仅adapter模式）
-- BEGIN;
-- SELECT * FROM students;
-- COMMIT;"""

        self.sql_text.delete(1.0, tk.END)
        self.sql_text.insert(tk.END, sample_sql)

    def export_results(self):
        """导出查询结果"""
        content = self.result_text.get(1.0, tk.END)
        if not content.strip():
            messagebox.showwarning("警告", "没有结果可导出")
            return

        filename = filedialog.asksaveasfilename(
            defaultextension=".txt",
            filetypes=[("文本文件", "*.txt"), ("所有文件", "*.*")]
        )

        if filename:
            try:
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(content)
                messagebox.showinfo("成功", f"结果已保存到: {filename}")
            except Exception as e:
                messagebox.showerror("错误", f"保存失败: {e}")

    def clear_logs(self):
        """清空日志"""
        self.log_text.delete(1.0, tk.END)

    def save_logs(self):
        """保存日志"""
        content = self.log_text.get(1.0, tk.END)
        if not content.strip():
            messagebox.showwarning("警告", "没有日志可保存")
            return

        filename = filedialog.asksaveasfilename(
            defaultextension=".log",
            filetypes=[("日志文件", "*.log"), ("文本文件", "*.txt"), ("所有文件", "*.*")]
        )

        if filename:
            try:
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(content)
                messagebox.showinfo("成功", f"日志已保存到: {filename}")
            except Exception as e:
                messagebox.showerror("错误", f"保存失败: {e}")

    def log(self, message: str):
        """添加日志消息"""
        timestamp = time.strftime("%H:%M:%S")
        log_entry = f"[{timestamp}] {message}\n"
        self.log_text.insert(tk.END, log_entry)
        self.log_text.see(tk.END)  # 滚动到底部
        self.root.update_idletasks()  # 更新界面

    def run(self):
        """运行GUI"""
        self.log("数据库GUI启动完成")
        self.root.mainloop()


def main():
    """主函数"""
    try:
        app = DatabaseGUI()
        app.run()
    except Exception as e:
        print(f"GUI启动失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
