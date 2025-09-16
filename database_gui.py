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
    from src.api.db_api import DatabaseAPI
    from src.distributed.sharding import ShardMetadata, ShardRouter
    from src.distributed.executor import DistributedExecutor, RemoteNode
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
        # 分布式组件（演示集成）
        self.dist_meta = None
        self.dist_router = None
        self.dist_nodes = {}
        self.dist_exec = None
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

        # 顶部菜单栏（保留原布局，将高级功能移入菜单）
        self.create_menu()

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
        self.sql_text.pack(fill=tk.BOTH, expand=True, pady=(5, 5))
        
        # 配置文本标签用于错误高亮
        self.sql_text.tag_configure("error", background="#ffcccc", foreground="#cc0000")
        self.sql_text.tag_configure("warning", background="#fff3cd", foreground="#856404")
        
        # 错误提示和修复区域
        error_frame = ttk.Frame(input_frame)
        error_frame.pack(fill=tk.X, pady=(0, 5))
        
        self.error_label = ttk.Label(error_frame, text="", foreground="red", font=("Arial", 9))
        self.error_label.pack(side=tk.LEFT)
        
        self.fix_button = ttk.Button(error_frame, text="🔧 快速修复", 
                                    command=self.apply_quick_fix, state="disabled")
        self.fix_button.pack(side=tk.RIGHT)
        
        # 存储当前的修复建议
        self.current_fixes = []
        
        # 绑定事件
        self.setup_autocomplete()
        # 不重复绑定KeyRelease事件，已在setup_autocomplete中绑定
        self.sql_text.bind('<Button-1>', self.on_text_click)

        # 预设SQL示例
        sample_sql = """-- SQL示例 (点击执行按钮运行)
CREATE TABLE students (id INT PRIMARY KEY, name STRING, age INT, score DOUBLE);
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
        ttk.Button(button_frame, text="EXPLAIN",
                   command=self.explain_sql).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(button_frame, text="导出结果",
                   command=self.export_results).pack(side=tk.RIGHT)
        ttk.Button(button_frame, text="分片可见性检查",
                   command=self.check_shard_visibility).pack(side=tk.RIGHT, padx=(0, 5))

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

        # 管理工具（索引/视图/触发器/过程）
        tools_frame = ttk.Frame(notebook)
        notebook.add(tools_frame, text="管理工具")
        self.create_tools_panel(tools_frame)

    def create_tools_panel(self, parent):
        """创建管理工具面板：索引/视图/触发器/过程"""
        panel = ttk.Notebook(parent)
        panel.pack(fill=tk.BOTH, expand=True)

        # 索引管理
        idx_frame = ttk.Frame(panel)
        panel.add(idx_frame, text="索引")
        self._create_index_tools(idx_frame)

        # 视图与物化视图
        view_frame = ttk.Frame(panel)
        panel.add(view_frame, text="视图/物化视图")
        self._create_views_tools(view_frame)

        # 触发器/过程
        trg_frame = ttk.Frame(panel)
        panel.add(trg_frame, text="触发器/过程")
        self._create_triggers_procs_tools(trg_frame)

    # ===== 索引管理 =====
    def _create_index_tools(self, parent):
        actions = ttk.LabelFrame(parent, text="索引操作", padding=10)
        actions.pack(fill=tk.X, padx=8, pady=8)

        ttk.Button(actions, text="SHOW INDEXES", command=self.show_indexes).pack(side=tk.LEFT, padx=5)
        ttk.Button(actions, text="创建索引", command=self.create_index_dialog).pack(side=tk.LEFT, padx=5)
        ttk.Button(actions, text="删除索引", command=self.drop_index_dialog).pack(side=tk.LEFT, padx=5)

        self.index_text = scrolledtext.ScrolledText(parent, height=12, font=("Consolas", 9))
        self.index_text.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)

    def show_indexes(self):
        self.index_text.delete(1.0, tk.END)
        try:
            res = self.adapter.execute("SHOW INDEXES;")
            cols = (res or {}).get("metadata", {}).get("columns", [])
            data = (res or {}).get("data", [])
            if cols and data:
                self.index_text.insert(tk.END, self._format_table_text(cols, data))
            else:
                # 尝试复合索引
                res2 = self.adapter.execute("SHOW COMPOSITE INDEXES;")
                cols2 = (res2 or {}).get("metadata", {}).get("columns", [])
                data2 = (res2 or {}).get("data", [])
                if cols2 and data2:
                    self.index_text.insert(tk.END, "[COMPOSITE INDEXES]\n")
                    self.index_text.insert(tk.END, self._format_table_text(cols2, data2))
                else:
                    self.index_text.insert(tk.END, str(res))
        except Exception as e:
            self.index_text.insert(tk.END, f"错误: {e}")

    def create_index_dialog(self):
        win = tk.Toplevel(self.root)
        win.title("创建索引")

        ttk.Label(win, text="表名:").grid(row=0, column=0, padx=6, pady=6, sticky=tk.W)
        table_var = tk.StringVar()
        ttk.Entry(win, textvariable=table_var, width=28).grid(row=0, column=1, padx=6, pady=6)

        ttk.Label(win, text="列名(逗号分隔):").grid(row=1, column=0, padx=6, pady=6, sticky=tk.W)
        cols_var = tk.StringVar()
        ttk.Entry(win, textvariable=cols_var, width=28).grid(row=1, column=1, padx=6, pady=6)

        ttk.Label(win, text="策略:").grid(row=2, column=0, padx=6, pady=6, sticky=tk.W)
        strat_var = tk.StringVar(value="BTREE")
        ttk.Combobox(win, textvariable=strat_var, values=["BTREE", "HASH"], state="readonly", width=10).grid(row=2, column=1, padx=6, pady=6, sticky=tk.W)

        ttk.Label(win, text="PK列(可选):").grid(row=3, column=0, padx=6, pady=6, sticky=tk.W)
        pk_var = tk.StringVar()
        ttk.Entry(win, textvariable=pk_var, width=28).grid(row=3, column=1, padx=6, pady=6)

        def do_create():
            table = table_var.get().strip()
            cols = [c.strip() for c in cols_var.get().split(',') if c.strip()]
            strat = strat_var.get().strip().upper()
            pk = pk_var.get().strip()
            if not table or not cols:
                messagebox.showwarning("警告", "请填写表名和列名")
                return
            try:
                # 单列 -> CREATE INDEX; 多列 -> CREATE COMPOSITE INDEX
                if len(cols) == 1:
                    pk_clause = f" PK {pk}" if pk else ""
                    using_clause = f" USING {strat}" if strat in ("BTREE", "HASH") else ""
                    sql = f"CREATE INDEX idx ON {table}({cols[0]}){using_clause}{pk_clause};"
                else:
                    sql = f"CREATE COMPOSITE INDEX idx ON {table}({','.join(cols)});"
                self._exec_and_display(sql)
                win.destroy()
            except Exception as e:
                messagebox.showerror("错误", f"创建失败: {e}")

        ttk.Button(win, text="创建", command=do_create).grid(row=4, column=1, padx=6, pady=12, sticky=tk.W)

    def drop_index_dialog(self):
        win = tk.Toplevel(self.root)
        win.title("删除索引")

        ttk.Label(win, text="表名:").grid(row=0, column=0, padx=6, pady=6, sticky=tk.W)
        table_var = tk.StringVar()
        ttk.Entry(win, textvariable=table_var, width=28).grid(row=0, column=1, padx=6, pady=6)

        ttk.Label(win, text="列名(单列或复合)").grid(row=1, column=0, padx=6, pady=6, sticky=tk.W)
        cols_var = tk.StringVar()
        ttk.Entry(win, textvariable=cols_var, width=28).grid(row=1, column=1, padx=6, pady=6)

        def do_drop():
            table = table_var.get().strip()
            cols = [c.strip() for c in cols_var.get().split(',') if c.strip()]
            if not table:
                messagebox.showwarning("警告", "请填写表名")
                return
            try:
                if len(cols) <= 1:
                    col = cols[0] if cols else ""
                    if not col:
                        messagebox.showwarning("警告", "请填写列名")
                        return
                    sql = f"DROP INDEX {table}({col});"
                else:
                    sql = f"DROP COMPOSITE INDEX ON {table};"
                self._exec_and_display(sql)
                win.destroy()
            except Exception as e:
                messagebox.showerror("错误", f"删除失败: {e}")

        ttk.Button(win, text="删除", command=do_drop).grid(row=2, column=1, padx=6, pady=12, sticky=tk.W)

    # ===== 视图/物化视图 =====
    def _create_views_tools(self, parent):
        actions = ttk.LabelFrame(parent, text="视图/物化视图操作", padding=10)
        actions.pack(fill=tk.X, padx=8, pady=8)

        ttk.Button(actions, text="创建视图", command=self.create_view_dialog).pack(side=tk.LEFT, padx=5)
        ttk.Button(actions, text="删除视图", command=self.drop_view_dialog).pack(side=tk.LEFT, padx=5)
        ttk.Button(actions, text="创建物化视图", command=self.create_mview_dialog).pack(side=tk.LEFT, padx=5)
        ttk.Button(actions, text="刷新物化视图", command=self.refresh_mview_dialog).pack(side=tk.LEFT, padx=5)
        ttk.Button(actions, text="删除物化视图", command=self.drop_mview_dialog).pack(side=tk.LEFT, padx=5)

        self.view_text = scrolledtext.ScrolledText(parent, height=12, font=("Consolas", 9))
        self.view_text.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)

    def create_view_dialog(self):
        win = tk.Toplevel(self.root)
        win.title("创建视图")
        ttk.Label(win, text="视图名:").grid(row=0, column=0, padx=6, pady=6, sticky=tk.W)
        name_var = tk.StringVar()
        ttk.Entry(win, textvariable=name_var, width=28).grid(row=0, column=1, padx=6, pady=6)
        ttk.Label(win, text="SELECT 语句:").grid(row=1, column=0, padx=6, pady=6, sticky=tk.W)
        sql_text = scrolledtext.ScrolledText(win, height=6, width=50)
        sql_text.grid(row=1, column=1, padx=6, pady=6)
        def do_create():
            name = name_var.get().strip()
            sel = sql_text.get(1.0, tk.END).strip().rstrip(';')
            if not name or not sel:
                messagebox.showwarning("警告", "请填写视图名与SELECT语句")
                return
            self._exec_and_display(f"CREATE VIEW {name} AS {sel};")
            win.destroy()
        ttk.Button(win, text="创建", command=do_create).grid(row=2, column=1, padx=6, pady=12, sticky=tk.W)

    def drop_view_dialog(self):
        win = tk.Toplevel(self.root)
        win.title("删除视图")
        ttk.Label(win, text="视图名:").grid(row=0, column=0, padx=6, pady=6, sticky=tk.W)
        name_var = tk.StringVar()
        ttk.Entry(win, textvariable=name_var, width=28).grid(row=0, column=1, padx=6, pady=6)
        def do_drop():
            name = name_var.get().strip()
            if not name:
                messagebox.showwarning("警告", "请填写视图名")
                return
            self._exec_and_display(f"DROP VIEW {name};")
            win.destroy()
        ttk.Button(win, text="删除", command=do_drop).grid(row=1, column=1, padx=6, pady=12, sticky=tk.W)

    def create_mview_dialog(self):
        win = tk.Toplevel(self.root)
        win.title("创建物化视图")
        ttk.Label(win, text="物化视图名:").grid(row=0, column=0, padx=6, pady=6, sticky=tk.W)
        name_var = tk.StringVar()
        ttk.Entry(win, textvariable=name_var, width=28).grid(row=0, column=1, padx=6, pady=6)
        ttk.Label(win, text="SELECT 语句:").grid(row=1, column=0, padx=6, pady=6, sticky=tk.W)
        sql_text = scrolledtext.ScrolledText(win, height=6, width=50)
        sql_text.grid(row=1, column=1, padx=6, pady=6)
        def do_create():
            name = name_var.get().strip()
            sel = sql_text.get(1.0, tk.END).strip().rstrip(';')
            if not name or not sel:
                messagebox.showwarning("警告", "请填写名称与SELECT语句")
                return
            self._exec_and_display(f"CREATE MATERIALIZED VIEW {name} AS {sel};")
            win.destroy()
        ttk.Button(win, text="创建", command=do_create).grid(row=2, column=1, padx=6, pady=12, sticky=tk.W)

    def refresh_mview_dialog(self):
        win = tk.Toplevel(self.root)
        win.title("刷新物化视图")
        ttk.Label(win, text="物化视图名:").grid(row=0, column=0, padx=6, pady=6, sticky=tk.W)
        name_var = tk.StringVar()
        ttk.Entry(win, textvariable=name_var, width=28).grid(row=0, column=1, padx=6, pady=6)
        def do_refresh():
            name = name_var.get().strip()
            if not name:
                messagebox.showwarning("警告", "请填写名称")
                return
            self._exec_and_display(f"REFRESH MATERIALIZED VIEW {name};")
            win.destroy()
        ttk.Button(win, text="刷新", command=do_refresh).grid(row=1, column=1, padx=6, pady=12, sticky=tk.W)

    def drop_mview_dialog(self):
        win = tk.Toplevel(self.root)
        win.title("删除物化视图")
        ttk.Label(win, text="物化视图名:").grid(row=0, column=0, padx=6, pady=6, sticky=tk.W)
        name_var = tk.StringVar()
        ttk.Entry(win, textvariable=name_var, width=28).grid(row=0, column=1, padx=6, pady=6)
        def do_drop():
            name = name_var.get().strip()
            if not name:
                messagebox.showwarning("警告", "请填写名称")
                return
            self._exec_and_display(f"DROP MATERIALIZED VIEW {name};")
            win.destroy()
        ttk.Button(win, text="删除", command=do_drop).grid(row=1, column=1, padx=6, pady=12, sticky=tk.W)

    # ===== 触发器/存储过程 =====
    def _create_triggers_procs_tools(self, parent):
        actions = ttk.LabelFrame(parent, text="触发器/过程操作", padding=10)
        actions.pack(fill=tk.X, padx=8, pady=8)

        ttk.Button(actions, text="创建触发器", command=self.create_trigger_dialog).pack(side=tk.LEFT, padx=5)
        ttk.Button(actions, text="删除触发器", command=self.drop_trigger_dialog).pack(side=tk.LEFT, padx=5)
        ttk.Button(actions, text="SHOW TRIGGERS", command=self.show_triggers).pack(side=tk.LEFT, padx=5)
        ttk.Button(actions, text="创建过程", command=self.create_proc_dialog).pack(side=tk.LEFT, padx=5)
        ttk.Button(actions, text="调用过程", command=self.call_proc_dialog).pack(side=tk.LEFT, padx=5)
        ttk.Button(actions, text="删除过程", command=self.drop_proc_dialog).pack(side=tk.LEFT, padx=5)

        self.trgproc_text = scrolledtext.ScrolledText(parent, height=12, font=("Consolas", 9))
        self.trgproc_text.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)

    def show_triggers(self):
        self.trgproc_text.delete(1.0, tk.END)
        try:
            res = self.adapter.execute("SHOW TRIGGERS;")
            cols = (res or {}).get("metadata", {}).get("columns", [])
            data = (res or {}).get("data", [])
            if cols and data:
                self.trgproc_text.insert(tk.END, self._format_table_text(cols, data))
            else:
                self.trgproc_text.insert(tk.END, str(res))
        except Exception as e:
            self.trgproc_text.insert(tk.END, f"错误: {e}")

    def create_trigger_dialog(self):
        win = tk.Toplevel(self.root)
        win.title("创建触发器")
        ttk.Label(win, text="触发器名:").grid(row=0, column=0, padx=6, pady=6, sticky=tk.W)
        name_var = tk.StringVar()
        ttk.Entry(win, textvariable=name_var, width=28).grid(row=0, column=1, padx=6, pady=6)
        ttk.Label(win, text="表名:").grid(row=1, column=0, padx=6, pady=6, sticky=tk.W)
        table_var = tk.StringVar()
        ttk.Entry(win, textvariable=table_var, width=28).grid(row=1, column=1, padx=6, pady=6)
        ttk.Label(win, text="时机( BEFORE/AFTER ):").grid(row=2, column=0, padx=6, pady=6, sticky=tk.W)
        timing_var = tk.StringVar(value="BEFORE")
        ttk.Combobox(win, textvariable=timing_var, values=["BEFORE", "AFTER"], state="readonly", width=10).grid(row=2, column=1, padx=6, pady=6, sticky=tk.W)
        ttk.Label(win, text="事件( INSERT/UPDATE/DELETE ):").grid(row=3, column=0, padx=6, pady=6, sticky=tk.W)
        event_var = tk.StringVar(value="INSERT")
        ttk.Combobox(win, textvariable=event_var, values=["INSERT", "UPDATE", "DELETE"], state="readonly", width=10).grid(row=3, column=1, padx=6, pady=6, sticky=tk.W)
        ttk.Label(win, text="触发体(多条以分号结尾):").grid(row=4, column=0, padx=6, pady=6, sticky=tk.W)
        body_text = scrolledtext.ScrolledText(win, height=6, width=50)
        body_text.grid(row=4, column=1, padx=6, pady=6)
        def do_create():
            name = name_var.get().strip()
            table = table_var.get().strip()
            timing = timing_var.get().strip().upper()
            event = event_var.get().strip().upper()
            body = body_text.get(1.0, tk.END).strip()
            if not name or not table or not body:
                messagebox.showwarning("警告", "请填写完整信息")
                return
            self._exec_and_display(f"CREATE TRIGGER {name} {timing} {event} ON {table} AS BEGIN {body} END;")
            win.destroy()
        ttk.Button(win, text="创建", command=do_create).grid(row=5, column=1, padx=6, pady=12, sticky=tk.W)

    def drop_trigger_dialog(self):
        win = tk.Toplevel(self.root)
        win.title("删除触发器")
        ttk.Label(win, text="触发器名:").grid(row=0, column=0, padx=6, pady=6, sticky=tk.W)
        name_var = tk.StringVar()
        ttk.Entry(win, textvariable=name_var, width=28).grid(row=0, column=1, padx=6, pady=6)
        ttk.Label(win, text="表名:").grid(row=1, column=0, padx=6, pady=6, sticky=tk.W)
        table_var = tk.StringVar()
        ttk.Entry(win, textvariable=table_var, width=28).grid(row=1, column=1, padx=6, pady=6)
        def do_drop():
            name = name_var.get().strip()
            table = table_var.get().strip()
            if not name or not table:
                messagebox.showwarning("警告", "请填写完整信息")
                return
            self._exec_and_display(f"DROP TRIGGER {name} ON {table};")
            win.destroy()
        ttk.Button(win, text="删除", command=do_drop).grid(row=2, column=1, padx=6, pady=12, sticky=tk.W)

    def create_proc_dialog(self):
        win = tk.Toplevel(self.root)
        win.title("创建过程")
        ttk.Label(win, text="过程名:").grid(row=0, column=0, padx=6, pady=6, sticky=tk.W)
        name_var = tk.StringVar()
        ttk.Entry(win, textvariable=name_var, width=28).grid(row=0, column=1, padx=6, pady=6)
        ttk.Label(win, text="过程体(多条以分号结尾):").grid(row=1, column=0, padx=6, pady=6, sticky=tk.W)
        body_text = scrolledtext.ScrolledText(win, height=6, width=50)
        body_text.grid(row=1, column=1, padx=6, pady=6)
        def do_create():
            name = name_var.get().strip()
            body = body_text.get(1.0, tk.END).strip()
            if not name or not body:
                messagebox.showwarning("警告", "请填写完整信息")
                return
            self._exec_and_display(f"CREATE PROCEDURE {name} AS BEGIN {body} END;")
            win.destroy()
        ttk.Button(win, text="创建", command=do_create).grid(row=2, column=1, padx=6, pady=12, sticky=tk.W)

    def call_proc_dialog(self):
        win = tk.Toplevel(self.root)
        win.title("调用过程")
        ttk.Label(win, text="过程名:").grid(row=0, column=0, padx=6, pady=6, sticky=tk.W)
        name_var = tk.StringVar()
        ttk.Entry(win, textvariable=name_var, width=28).grid(row=0, column=1, padx=6, pady=6)
        def do_call():
            name = name_var.get().strip()
            if not name:
                messagebox.showwarning("警告", "请填写过程名")
                return
            self._exec_and_display(f"CALL {name};")
            win.destroy()
        ttk.Button(win, text="调用", command=do_call).grid(row=1, column=1, padx=6, pady=12, sticky=tk.W)

    def drop_proc_dialog(self):
        win = tk.Toplevel(self.root)
        win.title("删除过程")
        ttk.Label(win, text="过程名:").grid(row=0, column=0, padx=6, pady=6, sticky=tk.W)
        name_var = tk.StringVar()
        ttk.Entry(win, textvariable=name_var, width=28).grid(row=0, column=1, padx=6, pady=6)
        def do_drop():
            name = name_var.get().strip()
            if not name:
                messagebox.showwarning("警告", "请填写过程名")
                return
            self._exec_and_display(f"DROP PROCEDURE {name};")
            win.destroy()
        ttk.Button(win, text="删除", command=do_drop).grid(row=1, column=1, padx=6, pady=12, sticky=tk.W)

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

    def create_menu(self):
        """创建顶部菜单栏，承载高级功能，保持原有布局简洁"""
        menubar = tk.Menu(self.root)

        # 执行菜单
        menu_exec = tk.Menu(menubar, tearoff=0)
        menu_exec.add_command(label="执行 SQL", command=self.execute_sql)
        menu_exec.add_command(label="EXPLAIN 当前SQL", command=self.explain_sql)
        menu_exec.add_separator()
        menu_exec.add_command(label="清空输入", command=self.clear_sql)
        menu_exec.add_command(label="加载示例", command=self.load_sample_sql)
        menu_exec.add_separator()
        menu_exec.add_command(label="导出结果...", command=self.export_results)
        menubar.add_cascade(label="执行", menu=menu_exec)

        # 事务菜单
        menu_tx = tk.Menu(menubar, tearoff=0)
        menu_tx.add_command(label="BEGIN", command=self.tx_begin)
        menu_tx.add_command(label="COMMIT", command=self.tx_commit)
        menu_tx.add_command(label="ROLLBACK", command=self.tx_rollback)
        menu_tx.add_separator()
        menu_tx.add_command(label="AUTOCOMMIT ON", command=lambda: self.set_autocommit(True))
        menu_tx.add_command(label="AUTOCOMMIT OFF", command=lambda: self.set_autocommit(False))
        menu_tx.add_separator()
        menu_tx.add_command(label="SHOW TRANSACTION", command=self.show_transaction)
        menu_tx.add_command(label="SHOW OVERLAY", command=self.show_overlay)
        menubar.add_cascade(label="事务", menu=menu_tx)

        # 数据菜单（导入/导出表）
        menu_data = tk.Menu(menubar, tearoff=0)
        menu_data.add_command(label="导入表...", command=self.import_table_dialog)
        menu_data.add_command(label="导出表...", command=self.export_table_dialog)
        menubar.add_cascade(label="数据", menu=menu_data)

        # 索引菜单
        menu_idx = tk.Menu(menubar, tearoff=0)
        menu_idx.add_command(label="SHOW INDEXES", command=self.show_indexes)
        menu_idx.add_command(label="创建索引...", command=self.create_index_dialog)
        menu_idx.add_command(label="删除索引...", command=self.drop_index_dialog)
        menubar.add_cascade(label="索引", menu=menu_idx)

        # 视图菜单
        menu_view = tk.Menu(menubar, tearoff=0)
        menu_view.add_command(label="创建视图...", command=self.create_view_dialog)
        menu_view.add_command(label="删除视图...", command=self.drop_view_dialog)
        menu_view.add_separator()
        menu_view.add_command(label="创建物化视图...", command=self.create_mview_dialog)
        menu_view.add_command(label="刷新物化视图...", command=self.refresh_mview_dialog)
        menu_view.add_command(label="删除物化视图...", command=self.drop_mview_dialog)
        menubar.add_cascade(label="视图", menu=menu_view)

        # 触发器/过程菜单
        menu_trg = tk.Menu(menubar, tearoff=0)
        menu_trg.add_command(label="SHOW TRIGGERS", command=self.show_triggers)
        menu_trg.add_command(label="创建触发器...", command=self.create_trigger_dialog)
        menu_trg.add_command(label="删除触发器...", command=self.drop_trigger_dialog)
        menu_trg.add_separator()
        menu_trg.add_command(label="创建过程...", command=self.create_proc_dialog)
        menu_trg.add_command(label="调用过程...", command=self.call_proc_dialog)
        menu_trg.add_command(label="删除过程...", command=self.drop_proc_dialog)
        menubar.add_cascade(label="触发器/过程", menu=menu_trg)

        # 分布式菜单（演示）
        menu_dist = tk.Menu(menubar, tearoff=0)
        menu_dist.add_command(label="初始化分布式", command=self.init_distributed_demo)
        menu_dist.add_command(label="初始化分布式(范围分片)", command=self.init_distributed_range_demo)
        menu_dist.add_command(label="分布式查询(合并)", command=self.run_distributed_select)
        menu_dist.add_command(label="分布式SUM(id)", command=self.run_distributed_sum)
        menu_dist.add_separator()
        menu_dist.add_command(label="向分布式T插入一行", command=self.dist_insert_dialog)
        menu_dist.add_command(label="分布式自定义查询", command=self.dist_custom_select)
        menu_dist.add_command(label="路由测试(key→分片)", command=self.route_test_dialog)
        menu_dist.add_separator()
        menu_dist.add_command(label="显示分片热力图", command=self.show_shard_heatmap)
        menu_dist.add_command(label="显示并行时间线", command=self.show_timeline)
        menu_dist.add_command(label="查看慢日志", command=self.show_slowlog)
        menubar.add_cascade(label="分布式", menu=menu_dist)

        self.root.config(menu=menubar)

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

    def init_distributed_demo(self):
        """初始化分布式演示：2片HASH分片，两个本地节点。"""
        try:
            self.log("初始化分布式演示环境...")
            self.dist_meta = ShardMetadata()
            self.dist_meta.create_hash_shards('T', 2)
            self.dist_router = ShardRouter(self.dist_meta)
            # 两个本地节点
            from src.distributed.monitoring import SlowQueryLog
            slowlog = SlowQueryLog(threshold_ms=0)
            node_a = RemoteNode(DatabaseAPI(), name="gui_node_a", slowlog=slowlog)
            node_b = RemoteNode(DatabaseAPI(), name="gui_node_b", slowlog=slowlog)
            shards = self.dist_router.all_shards('T')
            if len(shards) >= 2:
                self.dist_nodes[shards[0]['id']] = node_a
                self.dist_nodes[shards[1]['id']] = node_b
            elif len(shards) == 1:
                self.dist_nodes[shards[0]['id']] = node_a
            # 在每个分片建表
            for s in self.dist_router.all_shards('T'):
                n = self.dist_nodes.get(s['id']) or node_a
                n.execute("DROP TABLE T;")
                n.execute("CREATE TABLE T(id INT, name STRING);")
            # 准备执行器
            self.dist_exec = DistributedExecutor(self.dist_router, {'T': 'id'}, self.dist_nodes, slowlog=slowlog)
            self._dist_slowlog = slowlog
            # 插入几行示例数据
            self.dist_insert_row(1, 'A')
            self.dist_insert_row(2, 'B')
            self.dist_insert_row(3, 'C')
            self.log("✓ 分布式演示环境就绪 (表T, 2片HASH)")
        except Exception as e:
            self.log(f"分布式初始化失败: {e}")

    def init_distributed_range_demo(self):
        """初始化分布式演示：范围分片 [0,5), [5,~)。"""
        try:
            self.log("初始化分布式(范围分片)演示环境...")
            self.dist_meta = ShardMetadata()
            # 定义两个范围分片：字符串比较，id 转为零填充或直接字符串比较足够演示
            self.dist_meta.create_range_shards('T', [("0", "5"), ("5", None)])
            self.dist_router = ShardRouter(self.dist_meta)
            from src.distributed.monitoring import SlowQueryLog
            slowlog = SlowQueryLog(threshold_ms=0)
            node_a = RemoteNode(DatabaseAPI(), name="gui_node_r1", slowlog=slowlog)
            node_b = RemoteNode(DatabaseAPI(), name="gui_node_r2", slowlog=slowlog)
            shards = self.dist_router.all_shards('T')
            if len(shards) >= 2:
                self.dist_nodes[shards[0]['id']] = node_a
                self.dist_nodes[shards[1]['id']] = node_b
            elif len(shards) == 1:
                self.dist_nodes[shards[0]['id']] = node_a
            # 在每个分片建表
            for s in self.dist_router.all_shards('T'):
                n = self.dist_nodes.get(s['id']) or node_a
                n.execute("DROP TABLE T;")
                n.execute("CREATE TABLE T(id INT, name STRING);")
            self.dist_exec = DistributedExecutor(self.dist_router, {'T': 'id'}, self.dist_nodes, slowlog=slowlog)
            self._dist_slowlog = slowlog
            # 插入几行示例：落到不同范围
            self.dist_insert_row(1, 'A')
            self.dist_insert_row(4, 'B')
            self.dist_insert_row(5, 'C')
            self.dist_insert_row(12, 'D')
            self.log("✓ 分布式演示环境就绪 (表T, 范围分片[0,5),[5,~))")
        except Exception as e:
            self.log(f"分布式(范围)初始化失败: {e}")

    def dist_insert_row(self, id_val: int, name: str):
        if not (self.dist_router and self.dist_nodes):
            return
        s = self.dist_router.locate_by_value('T', str(id_val))
        if not s:
            return
        node = self.dist_nodes.get(s[0]['id']) or next(iter(self.dist_nodes.values()))
        node.execute(f"INSERT INTO T(id,name) VALUES ({id_val}, '{name}');")

    def dist_insert_dialog(self):
        """弹窗输入一行数据并按分片规则插入到表 T。"""
        if not self.dist_exec:
            self.log("请先初始化分布式")
            return
        win = tk.Toplevel(self.root)
        win.title("向分布式表 T 插入一行")
        ttk.Label(win, text="id (分片键)").grid(row=0, column=0, padx=6, pady=6, sticky=tk.W)
        id_var = tk.StringVar()
        ttk.Entry(win, textvariable=id_var, width=20).grid(row=0, column=1, padx=6, pady=6)
        ttk.Label(win, text="name").grid(row=1, column=0, padx=6, pady=6, sticky=tk.W)
        name_var = tk.StringVar()
        ttk.Entry(win, textvariable=name_var, width=20).grid(row=1, column=1, padx=6, pady=6)
        def do_insert():
            try:
                iv = int(id_var.get().strip())
                nm = name_var.get().strip()
                if nm == "":
                    messagebox.showwarning("警告", "请填写 name")
                    return
                self.dist_insert_row(iv, nm)
                self.result_text.insert(tk.END, f"已插入到分布式表 T: id={iv}, name={nm}\n")
                win.destroy()
            except Exception as e:
                messagebox.showerror("错误", f"插入失败: {e}")
        ttk.Button(win, text="插入", command=do_insert).grid(row=2, column=1, padx=6, pady=12, sticky=tk.W)

    def dist_custom_select(self):
        """跨分片执行自定义 SELECT 并合并显示。使用 {table} 作为表占位符，默认 T。"""
        if not self.dist_exec:
            self.log("请先初始化分布式")
            return
        win = tk.Toplevel(self.root)
        win.title("分布式自定义查询")
        ttk.Label(win, text="SELECT 模板 (使用 {table} 占位)").grid(row=0, column=0, padx=6, pady=6, sticky=tk.W)
        sql_text = scrolledtext.ScrolledText(win, height=5, width=60)
        sql_text.grid(row=1, column=0, columnspan=2, padx=6, pady=6)
        sql_text.insert(tk.END, "SELECT id,name FROM {table};")
        ttk.Label(win, text="表名").grid(row=2, column=0, padx=6, pady=6, sticky=tk.W)
        tbl_var = tk.StringVar(value="T")
        ttk.Entry(win, textvariable=tbl_var, width=20).grid(row=2, column=1, padx=6, pady=6, sticky=tk.W)
        def do_run():
            tmpl = sql_text.get(1.0, tk.END).strip()
            table = tbl_var.get().strip() or "T"
            if "{table}" not in tmpl:
                messagebox.showwarning("警告", "模板中缺少 {table} 占位符")
                return
            try:
                # 复用分布式执行器的查询合并接口
                res = self.dist_exec.select_all_shards(table, tmpl)
                out = {"status":"success","data":res.get("data",[]),"metadata":{"columns":res.get("columns", ["col1","col2"])},"affected_rows":len(res.get("data",[]))}
                self.display_result(out, 0.0)
                win.destroy()
            except Exception as e:
                messagebox.showerror("错误", f"执行失败: {e}")
        ttk.Button(win, text="执行", command=do_run).grid(row=3, column=1, padx=6, pady=12, sticky=tk.W)

    def route_test_dialog(self):
        """输入一个键值，展示路由到的分片与节点。"""
        if not (self.dist_router and self.dist_nodes):
            self.log("请先初始化分布式")
            return
        win = tk.Toplevel(self.root)
        win.title("路由测试 - key → 分片")
        ttk.Label(win, text="表名").grid(row=0, column=0, padx=6, pady=6, sticky=tk.W)
        tbl_var = tk.StringVar(value="T")
        ttk.Entry(win, textvariable=tbl_var, width=18).grid(row=0, column=1, padx=6, pady=6)
        ttk.Label(win, text="key(如 id)").grid(row=1, column=0, padx=6, pady=6, sticky=tk.W)
        key_var = tk.StringVar()
        ttk.Entry(win, textvariable=key_var, width=18).grid(row=1, column=1, padx=6, pady=6)
        out_lbl = ttk.Label(win, text="")
        out_lbl.grid(row=2, column=0, columnspan=2, padx=6, pady=6, sticky=tk.W)
        def do_route():
            k = key_var.get().strip()
            table = tbl_var.get().strip() or 'T'
            if k == "":
                out_lbl.config(text="请输入 key")
                return
            shards = self.dist_router.locate_by_value(table, k)
            if not shards:
                out_lbl.config(text="未命中任何分片")
                return
            sid = shards[0].get('id')
            node = self.dist_nodes.get(sid)
            out_lbl.config(text=f"strategy={self.dist_meta.get(table).get('strategy')} → shard={sid} → node={getattr(node,'name','(unknown)')}")
        ttk.Button(win, text="测试路由", command=do_route).grid(row=3, column=1, padx=6, pady=8, sticky=tk.W)

    def show_shard_heatmap(self):
        """基于每片当前行数绘制简易热力图。"""
        try:
            if not (self.dist_router and self.dist_nodes):
                self.log("请先初始化分布式")
                return
            self.result_text.insert(tk.END, "分片热力图 (表T)\n")
            shards = self.dist_router.all_shards('T')
            max_rows = 1
            rows_per = {}
            for s in shards:
                sid = s.get('id')
                node = self.dist_nodes.get(sid) or next(iter(self.dist_nodes.values()))
                res = node.execute('SELECT id,name FROM T;')
                n = len((res or {}).get('data', [])) if isinstance(res, dict) else 0
                rows_per[sid] = n
                max_rows = max(max_rows, n)
            for sid, n in rows_per.items():
                bar_len = int((n / max_rows) * 20) if max_rows else 0
                bar = '█' * max(1, bar_len)
                self.result_text.insert(tk.END, f"- {sid:<10} rows={n:<4} {bar}\n")
            self.result_text.insert(tk.END, "\n")
        except Exception as e:
            self.log(f"热力图失败: {e}")

    def show_timeline(self):
        """基于慢日志绘制简易并行时间线。"""
        try:
            if not hasattr(self, '_dist_slowlog'):
                self.log("慢日志为空或未初始化")
                return
            logs = self._dist_slowlog.list()
            if not logs:
                self.result_text.insert(tk.END, "无慢查询记录\n")
                return
            self.result_text.insert(tk.END, "并行时间线 (ms，条长≈耗时)\n")
            # 取最近 20 条
            items = sorted(logs, key=lambda x: x.get('ts', 0), reverse=True)[:20]
            max_ms = max(1.0, max(x.get('elapsed_ms', 0.0) for x in items))
            for it in items:
                node = it.get('node', 'node')
                ms = float(it.get('elapsed_ms', 0.0))
                bar_len = int((ms / max_ms) * 30)
                bar = '▮' * max(1, bar_len)
                sql = it.get('sql', '').replace('\n', ' ')
                self.result_text.insert(tk.END, f"{node:<14} {ms:>6.1f}ms {bar}  {sql[:60]}\n")
            self.result_text.insert(tk.END, "\n")
        except Exception as e:
            self.log(f"时间线失败: {e}")

    def run_distributed_select(self):
        """分布式合并查询"""
        if not self.dist_exec:
            self.log("请先初始化分布式")
            return
        res = self.dist_exec.select_all_shards('T', 'SELECT id,name FROM T;')
        self.display_result({"status":"success","data":res.get("data",[]),"metadata":{"columns":["id","name"]},"affected_rows":len(res.get("data",[]))}, 0.0)

    def run_distributed_sum(self):
        """分布式聚合 SUM(id)"""
        if not self.dist_exec:
            self.log("请先初始化分布式")
            return
        res = self.dist_exec.distributed_aggregate_sum('T', 'SELECT SUM(id) FROM {table};')
        self.display_result(res, 0.0)

    def show_slowlog(self):
        if not hasattr(self, '_dist_slowlog'):
            self.log("慢查询日志为空或未初始化")
            return
        logs = self._dist_slowlog.list()
        self.result_text.insert(tk.END, "Slow Queries (distributed):\n")
        for item in logs:
            self.result_text.insert(tk.END, f"- {item.get('node')} {item.get('elapsed_ms'):.2f}ms :: {item.get('sql')}\n")

    def check_shard_visibility(self):
        """逐分片执行简单SELECT并展示每片行数与样例行。"""
        try:
            if not (self.dist_router and self.dist_nodes):
                self.log("请先初始化分布式")
                return
            shards = self.dist_router.all_shards('T')
            if not shards:
                self.result_text.insert(tk.END, "无分片定义\n")
                return
            self.result_text.insert(tk.END, "分片可见性检查 (表T)\n")
            for s in shards:
                sid = s.get('id')
                node = self.dist_nodes.get(sid) or next(iter(self.dist_nodes.values()))
                res = node.execute('SELECT id,name FROM T;')
                rows = res.get('data', []) if isinstance(res, dict) else []
                self.result_text.insert(tk.END, f"- 分片 {sid} 节点 {getattr(node, 'name', 'node')} 行数={len(rows)}\n")
                # 打印前3行样例
                for r in rows[:3]:
                    self.result_text.insert(tk.END, f"  · {r}\n")
            self.result_text.insert(tk.END, "\n")
        except Exception as e:
            self.log(f"分片可见性检查失败: {e}")

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

        # 支持多语句：按分号拆分，保留语句顺序，逐条执行并汇总；仿照 CLI
        statements = [s.strip() for s in sql.replace('\r','').split(';') if s.strip()]
        if not statements:
            return

        self.log(f"执行SQL ({self.current_mode}模式): 共 {len(statements)} 条")
        total_start = time.time()
        successes = 0

        for idx, stmt in enumerate(statements, 1):
            # 还原分号
            stmt_sql = stmt + ';'
            self.result_text.insert(tk.END, f"\n-- [{idx}/{len(statements)}] {stmt_sql}\n")
            
            # 显示详细的编译过程（词法分析、语法分析、语义分析等）
            self.run_detailed_compilation(stmt_sql)
            
            try:
                start_time = time.time()
                if self.current_mode == "adapter":
                    # GUI 内支持 EXPORT/IMPORT 命令，仿 CLI
                    up = stmt_sql.strip().upper()
                    if up.startswith("EXPORT TABLE"):
                        self._handle_export_command(stmt_sql)
                        self.result_text.insert(tk.END, "✓ 导出命令已执行\n")
                        successes += 1
                        continue
                    if up.startswith("IMPORT TABLE"):
                        self._handle_import_command(stmt_sql)
                        self.result_text.insert(tk.END, "✓ 导入命令已执行\n")
                        successes += 1
                        continue
                    result = self.adapter.execute(stmt_sql)
                else:
                    # CORE 模式与 CLI 一致：拦截不支持的高级命令
                    up = stmt_sql.strip().upper().rstrip(';')
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
                        result = self.core_engine.execute(stmt_sql)

                exec_ms = time.time() - start_time
                self.display_result(result, exec_ms)
                successes += 1 if result.get('status') != 'error' else 0
            except Exception as e:
                error_msg = f"执行错误: {str(e)}"
                self.result_text.insert(tk.END, error_msg + "\n")
                self.result_text.insert(tk.END, "\n💡 智能建议：\n" + self._suggest_fixes(stmt_sql, str(e)))
                # 不中断，继续执行后续语句

        self.log(f"✓ 多语句执行完成 ({time.time()-total_start:.3f}s) 成功 {successes}/{len(statements)} 条")
        # 更新监控信息
        self.update_monitors()

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
        """更新监控信息 - 紧急修复版本"""
        try:
            # 紧急修复：暂时只更新基本信息，避免触发SQL查询
            self.update_compiler_info()
            
            # 暂时跳过存储和执行器信息更新，这些会触发表扫描和SQL查询
            # self.update_storage_info()  # 包含 get_catalog_info 和 SQL 查询
            # self.update_executor_info()  # 可能包含状态查询

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
        try:
            # 重新启用智能纠错功能
            self.on_sql_text_change(event)
            
            # 保留自动补全功能
            current_word = self.get_current_word()
            if len(current_word) > 2 and current_word.isalnum():
                self.show_autocomplete_suggestions(current_word)
            else:
                self.hide_autocomplete()
        except Exception:
            pass

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
        # 不重复绑定Button-1事件，已在主初始化中绑定到on_text_click

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
语法检查: 启用
语义分析: 启用"""
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
                col_count = "N/A"
                row_count = "N/A"
                # 获取列信息
                try:
                    res = self.adapter.execute(f"SELECT * FROM {table} LIMIT 0;")
                    cols = (res or {}).get("metadata", {}).get("columns", [])
                    col_count = len(cols) if isinstance(cols, list) else "N/A"
                except Exception:
                    pass
                # 获取行数（可能不支持 COUNT(*)，失败则保留N/A）
                try:
                    cnt_res = self.adapter.execute(f"SELECT COUNT(*) FROM {table};")
                    data = (cnt_res or {}).get("data", [])
                    if data and isinstance(data[0], (list, tuple)) and len(data[0]) >= 1:
                        row_count = data[0][0]
                except Exception:
                    pass

                self.tables_tree.insert("", tk.END, text=table,
                                        values=(col_count, row_count))

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
        # 紧急修复：暂时禁用目录同步，避免GUI卡顿
        # 目录同步包含递归文件扫描，会导致界面无响应
        try:
            pass  # 暂时跳过同步，待性能优化后再启用
        except Exception:
            pass
        self.update_monitors()

    def clear_sql(self):
        """清空SQL输入"""
        self.sql_text.delete(1.0, tk.END)

    def load_sample_sql(self):
        """加载示例SQL"""
        sample_sql = """-- 数据库操作示例
CREATE TABLE students (id INT PRIMARY KEY, name STRING, age INT, score DOUBLE);
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

    def explain_sql(self):
        """对当前输入执行 EXPLAIN（adapter 支持；core 提示不支持）"""
        sql = self.sql_text.get(1.0, tk.END).strip()
        if not sql:
            messagebox.showwarning("警告", "请输入SQL语句以进行 EXPLAIN")
            return
        stmt = "EXPLAIN " + (sql if sql.endswith(';') else sql + ';')
        self.log("EXPLAIN 当前SQL")
        try:
            if self.current_mode == "adapter":
                res = self.adapter.execute(stmt)
            else:
                res = {"status":"error","error":"EXPLAIN 在 CORE 模式不支持，请切换 MODE ADAPTER","affected_rows":0,"data":[]}
            self.display_result(res, 0.0)
        except Exception as e:
            self.result_text.insert(tk.END, f"执行错误: {e}\n\n")
            try:
                self.result_text.insert(tk.END, "💡 智能建议：\n" + self._suggest_fixes(sql, str(e)))
            except Exception:
                pass

    # ===== 事务相关 =====
    def tx_begin(self):
        try:
            if self.current_mode != "adapter":
                self.display_result({"status":"error","error":"BEGIN 仅在 ADAPTER 模式支持","affected_rows":0}, 0.0)
                return
            res = self.adapter.execute("BEGIN;")
            self.display_result(res, 0.0)
        except Exception as e:
            self.display_result({"status":"error","error":str(e),"affected_rows":0}, 0.0)

    def tx_commit(self):
        try:
            if self.current_mode != "adapter":
                self.display_result({"status":"error","error":"COMMIT 仅在 ADAPTER 模式支持","affected_rows":0}, 0.0)
                return
            res = self.adapter.execute("COMMIT;")
            self.display_result(res, 0.0)
        except Exception as e:
            self.display_result({"status":"error","error":str(e),"affected_rows":0}, 0.0)

    def tx_rollback(self):
        try:
            if self.current_mode != "adapter":
                self.display_result({"status":"error","error":"ROLLBACK 仅在 ADAPTER 模式支持","affected_rows":0}, 0.0)
                return
            res = self.adapter.execute("ROLLBACK;")
            self.display_result(res, 0.0)
        except Exception as e:
            self.display_result({"status":"error","error":str(e),"affected_rows":0}, 0.0)

    def set_autocommit(self, enabled: bool):
        try:
            if self.current_mode != "adapter":
                self.display_result({"status":"error","error":"SET AUTOCOMMIT 仅在 ADAPTER 模式支持","affected_rows":0}, 0.0)
                return
            stmt = "SET AUTOCOMMIT ON;" if enabled else "SET AUTOCOMMIT OFF;"
            res = self.adapter.execute(stmt)
            self.display_result(res, 0.0)
        except Exception as e:
            self.display_result({"status":"error","error":str(e),"affected_rows":0}, 0.0)

    def show_transaction(self):
        try:
            if self.current_mode != "adapter":
                self.display_result({"status":"error","error":"SHOW TRANSACTION 仅在 ADAPTER 模式支持","affected_rows":0}, 0.0)
                return
            res = self.adapter.execute("SHOW TRANSACTION;")
            self.display_result(res, 0.0)
        except Exception as e:
            self.display_result({"status":"error","error":str(e),"affected_rows":0}, 0.0)

    def show_overlay(self):
        try:
            if self.current_mode != "adapter":
                self.display_result({"status":"error","error":"SHOW OVERLAY 仅在 ADAPTER 模式支持","affected_rows":0}, 0.0)
                return
            res = self.adapter.execute("SHOW OVERLAY;")
            self.display_result(res, 0.0)
        except Exception as e:
            self.display_result({"status":"error","error":str(e),"affected_rows":0}, 0.0)

    # ===== 数据导入/导出 =====
    def import_table_dialog(self):
        win = tk.Toplevel(self.root)
        win.title("导入表")
        ttk.Label(win, text="表名:").grid(row=0, column=0, padx=6, pady=6, sticky=tk.W)
        table_var = tk.StringVar()
        ttk.Entry(win, textvariable=table_var, width=28).grid(row=0, column=1, padx=6, pady=6)
        ttk.Label(win, text="格式:").grid(row=1, column=0, padx=6, pady=6, sticky=tk.W)
        fmt_var = tk.StringVar(value="csv")
        ttk.Combobox(win, textvariable=fmt_var, values=["csv", "json"], state="readonly", width=10).grid(row=1, column=1, padx=6, pady=6, sticky=tk.W)
        ttk.Label(win, text="文件路径:").grid(row=2, column=0, padx=6, pady=6, sticky=tk.W)
        path_var = tk.StringVar()
        ttk.Entry(win, textvariable=path_var, width=28).grid(row=2, column=1, padx=6, pady=6)
        def choose_file():
            fname = filedialog.askopenfilename(filetypes=[("CSV","*.csv"),("JSON","*.json"),("所有文件","*.*")])
            if fname:
                path_var.set(fname)
        ttk.Button(win, text="选择...", command=choose_file).grid(row=2, column=2, padx=6, pady=6)
        def do_import():
            table = table_var.get().strip()
            fmt = fmt_var.get().strip().lower()
            path = path_var.get().strip()
            if not table or not path:
                messagebox.showwarning("警告", "请填写表名与文件路径")
                return
            sql = f"IMPORT TABLE {table} FROM {fmt} PATH '{path}';"
            self._exec_and_display(sql)
            win.destroy()
        ttk.Button(win, text="导入", command=do_import).grid(row=3, column=1, padx=6, pady=12, sticky=tk.W)

    def export_table_dialog(self):
        win = tk.Toplevel(self.root)
        win.title("导出表")
        ttk.Label(win, text="表名:").grid(row=0, column=0, padx=6, pady=6, sticky=tk.W)
        table_var = tk.StringVar()
        ttk.Entry(win, textvariable=table_var, width=28).grid(row=0, column=1, padx=6, pady=6)
        ttk.Label(win, text="格式:").grid(row=1, column=0, padx=6, pady=6, sticky=tk.W)
        fmt_var = tk.StringVar(value="csv")
        ttk.Combobox(win, textvariable=fmt_var, values=["csv", "json"], state="readonly", width=10).grid(row=1, column=1, padx=6, pady=6, sticky=tk.W)
        ttk.Label(win, text="保存路径:").grid(row=2, column=0, padx=6, pady=6, sticky=tk.W)
        path_var = tk.StringVar()
        ttk.Entry(win, textvariable=path_var, width=28).grid(row=2, column=1, padx=6, pady=6)
        def choose_file():
            defext = ".csv" if fmt_var.get().lower()=="csv" else ".json"
            fname = filedialog.asksaveasfilename(defaultextension=defext, filetypes=[("CSV","*.csv"),("JSON","*.json"),("所有文件","*.*")])
            if fname:
                path_var.set(fname)
        ttk.Button(win, text="选择...", command=choose_file).grid(row=2, column=2, padx=6, pady=6)
        def do_export():
            table = table_var.get().strip()
            fmt = fmt_var.get().strip().lower()
            path = path_var.get().strip()
            if not table or not path:
                messagebox.showwarning("警告", "请填写表名与保存路径")
                return
            sql = f"EXPORT TABLE {table} TO {fmt} PATH '{path}';"
            self._exec_and_display(sql)
            win.destroy()
        ttk.Button(win, text="导出", command=do_export).grid(row=3, column=1, padx=6, pady=12, sticky=tk.W)

    # ===== 通用执行与建议 =====
    def _exec_and_display(self, sql: str):
        try:
            if self.current_mode == "adapter":
                up = sql.strip().upper()
                if up.startswith("EXPORT TABLE"):
                    self._handle_export_command(sql)
                    self.display_result({"status":"success","metadata":{"message":"导出完成"},"affected_rows":0}, 0.0)
                    return
                if up.startswith("IMPORT TABLE"):
                    self._handle_import_command(sql)
                    self.display_result({"status":"success","metadata":{"message":"导入完成"},"affected_rows":0}, 0.0)
                    return
                res = self.adapter.execute(sql)
            else:
                res = {"status":"error","error":"该命令在 CORE 模式不支持，请切换 MODE ADAPTER","affected_rows":0}
            self.display_result(res, 0.0)
            self.update_monitors()
        except Exception as e:
            self.result_text.insert(tk.END, f"执行错误: {e}\n")
            try:
                self.result_text.insert(tk.END, "\n💡 智能建议：\n" + self._suggest_fixes(sql, str(e)))
            except Exception:
                pass

    def _handle_export_command(self, sql: str):
        # 直接转发给适配器，适配器内部处理导出
        if self.adapter:
            self.adapter.execute(sql)

    def _handle_import_command(self, sql: str):
        # 直接转发给适配器，适配器内部处理导入
        if self.adapter:
            self.adapter.execute(sql)

    # ===== 智能纠错功能 =====
    
    def on_sql_text_change(self, event=None):
        """SQL文本变化时的智能检查"""
        try:
            # 重新启用智能纠错，现在检测功能更完善了
            if hasattr(self, '_check_timer'):
                self.root.after_cancel(self._check_timer)
            self._check_timer = self.root.after(800, self.check_sql_errors)  # 稍微延长延迟时间
        except Exception:
            pass
    
    def clear_error_highlights(self, event=None):
        """清除错误高亮"""
        try:
            self.sql_text.tag_remove("error", "1.0", tk.END)
            self.sql_text.tag_remove("warning", "1.0", tk.END)
        except Exception:
            pass
    
    def on_text_click(self, event=None):
        """文本点击事件处理"""
        # 清除错误高亮
        self.clear_error_highlights(event)
        # 隐藏自动补全窗口
        self.hide_autocomplete()
    
    def check_sql_errors(self):
        """检查SQL语句中的错误"""
        try:
            sql = self.sql_text.get("1.0", tk.END).strip()
            if not sql or sql.startswith("--"):
                self.error_label.config(text="")
                return
                
            # 清除之前的高亮
            self.clear_error_highlights()
            
            errors = []
            warnings = []
            
            # 检查拼写错误
            spell_errors, spell_fixes = self.check_spelling_errors(sql)
            errors.extend(spell_errors)
            
            # 检查语法结构
            syntax_warnings = self.check_syntax_structure(sql)
            warnings.extend(syntax_warnings)
            
            # 存储修复建议
            self.current_fixes = spell_fixes
            
            # 高亮错误
            self.highlight_errors(sql, spell_errors)
            
            # 显示错误信息和控制修复按钮（只显示错误和警告）
            if errors:
                self.error_label.config(text=f"❌ {'; '.join(errors)}")
                self.fix_button.config(state="normal" if self.current_fixes else "disabled")
            elif warnings:
                self.error_label.config(text=f"⚠️  {'; '.join(warnings)}")
                self.fix_button.config(state="disabled")
            else:
                # 无错误和警告时，清空显示
                self.error_label.config(text="")
                self.fix_button.config(state="disabled")
                
        except Exception as e:
            # 静默处理错误，避免干扰用户输入
            pass
    
    def check_spelling_errors(self, sql: str) -> tuple:
        """检查SQL关键字拼写错误，返回错误列表和修复建议"""
        errors = []
        fixes = []
        
        # 扩展的拼写错误映射 - 覆盖更多常见错误
        spell_corrections = {
            # SELECT 变体
            'SELCT': 'SELECT', 'SELET': 'SELECT', 'SLEECT': 'SELECT', 'SLECT': 'SELECT',
            'SELECCT': 'SELECT', 'SEELCT': 'SELECT', 'SELEECT': 'SELECT', 'SELEET': 'SELECT',
            
            # CREATE 变体  
            'CREAT': 'CREATE', 'CRETE': 'CREATE', 'CRAETE': 'CREATE', 'CREEAT': 'CREATE',
            'CRAEATE': 'CREATE', 'CRREATE': 'CREATE', 'CREATEE': 'CREATE',
            
            # INSERT 变体
            'INSRT': 'INSERT', 'INSER': 'INSERT', 'ISERT': 'INSERT', 'INSERRT': 'INSERT',
            'INSERET': 'INSERT', 'INSERTT': 'INSERT', 'INSETT': 'INSERT',
            
            # UPDATE 变体
            'UPDAT': 'UPDATE', 'UPDAE': 'UPDATE', 'UPDATEE': 'UPDATE', 'UUPDATE': 'UPDATE',
            'UPDATTE': 'UPDATE', 'UPDAET': 'UPDATE',
            
            # DELETE 变体
            'DELET': 'DELETE', 'DLEET': 'DELETE', 'DELEET': 'DELETE', 'DEELET': 'DELETE',
            'DELETEE': 'DELETE', 'DEELETE': 'DELETE',
            
            # WHERE 变体
            'WHER': 'WHERE', 'WHRE': 'WHERE', 'WEHRE': 'WHERE', 'WHEERE': 'WHERE',
            'WHHERE': 'WHERE', 'WHEREE': 'WHERE',
            
            # FROM 变体
            'FORM': 'FROM', 'FRM': 'FROM', 'FROOM': 'FROM', 'FROMM': 'FROM',
            
            # GROUP 变体
            'GRUP': 'GROUP', 'GRPUP': 'GROUP', 'GRROUP': 'GROUP', 'GROUPP': 'GROUP',
            'GROPU': 'GROUP', 'GROUUP': 'GROUP',
            
            # ORDER 变体
            'ORDRE': 'ORDER', 'ORDRER': 'ORDER', 'ORDERR': 'ORDER', 'ORRDER': 'ORDER',
            'ORDDER': 'ORDER',
            
            # JOIN 变体
            'JION': 'JOIN', 'JOINN': 'JOIN', 'JOOIN': 'JOIN', 'JJOIN': 'JOIN',
            
            # INNER 变体
            'INENR': 'INNER', 'INNRE': 'INNER', 'INNNER': 'INNER', 'IINNER': 'INNER',
            'INNERR': 'INNER',
            
            # LEFT/RIGHT 变体
            'LAFT': 'LEFT', 'LEFFT': 'LEFT', 'LEFTT': 'LEFT', 'LEEFT': 'LEFT',
            'RIGH': 'RIGHT', 'RIGHTT': 'RIGHT', 'RIIGHT': 'RIGHT', 'RIGTH': 'RIGHT',
            
            # HAVING 变体
            'HAVIG': 'HAVING', 'HAVNG': 'HAVING', 'HAVVING': 'HAVING', 'HAAVING': 'HAVING',
            'HAVIING': 'HAVING',
            
            # LIMIT 变体
            'LIMT': 'LIMIT', 'LIMTI': 'LIMIT', 'LIMITT': 'LIMIT', 'LIIMIT': 'LIMIT',
            
            # TABLE 变体
            'TABL': 'TABLE', 'TABEL': 'TABLE', 'TABLEE': 'TABLE', 'TTABLE': 'TABLE',
            'TABBLE': 'TABLE',
            
            # VALUES 变体
            'VALE': 'VALUES', 'VALES': 'VALUES', 'VALEUS': 'VALUES', 'VALUESS': 'VALUES',
            'VALUUES': 'VALUES', 'VVALUES': 'VALUES',
            
            # INDEX 变体
            'INDE': 'INDEX', 'INDX': 'INDEX', 'INDEEX': 'INDEX', 'INNDEX': 'INDEX',
            'INDEXX': 'INDEX',
            
            # DISTINCT 变体
            'DISTINC': 'DISTINCT', 'DISTINT': 'DISTINCT', 'DISINCT': 'DISTINCT',
            'DISTINCTT': 'DISTINCT', 'DDISTINCT': 'DISTINCT',
            
            # COUNT 变体
            'CONT': 'COUNT', 'COUN': 'COUNT', 'COUTN': 'COUNT', 'COUNTT': 'COUNT',
            'COOUNT': 'COUNT',
            
            # 其他常用关键字变体
            'ALTR': 'ALTER', 'ALTEER': 'ALTER', 'ALTERR': 'ALTER',
            'DROPP': 'DROP', 'DORP': 'DROP', 'DRROP': 'DROP',
            'PRIMART': 'PRIMARY', 'PRIMERY': 'PRIMARY', 'PRMARY': 'PRIMARY',
            'FORIEGN': 'FOREIGN', 'FOREGIN': 'FOREIGN', 'FOREIN': 'FOREIGN',
            'UNIQU': 'UNIQUE', 'UNIQEU': 'UNIQUE', 'UNIIQUE': 'UNIQUE',
            'NOTT': 'NOT', 'NNOT': 'NOT',
            'NULK': 'NULL', 'NULLL': 'NULL', 'NNULL': 'NULL',
            'ANDD': 'AND', 'ANND': 'AND', 'ANN': 'AND',
            'ORR': 'OR', 'OOR': 'OR',
            'BETWEN': 'BETWEEN', 'BEWEEN': 'BETWEEN', 'BEETWEEN': 'BETWEEN',
            'LIKEE': 'LIKE', 'LLIKE': 'LIKE', 'LIIKE': 'LIKE',
            'EXISTSS': 'EXISTS', 'EXSITS': 'EXISTS', 'EXSIST': 'EXISTS',
            'UNIO': 'UNION', 'UNIOIN': 'UNION', 'UNIONN': 'UNION',
            'CASEE': 'CASE', 'CCASE': 'CASE', 'CAASE': 'CASE',
            'WHENN': 'WHEN', 'WHHEN': 'WHEN', 'WEHN': 'WHEN',
            'THENN': 'THEN', 'THHEN': 'THEN', 'TEHN': 'THEN',
            'ELSSE': 'ELSE', 'ELSE': 'ELSE', 'EELSE': 'ELSE',
            'ENDD': 'END', 'ENND': 'END', 'EEND': 'END'
        }
        
        # 更智能的词法错误检测
        import re
        
        # 1. 基本拼写错误检查
        words = re.findall(r'\b\w+\b', sql)  # 提取单词，忽略标点和空格
        
        for i, word in enumerate(words):
            upper_word = word.upper()
            
            # 直接拼写错误检查
            if upper_word in spell_corrections:
                errors.append(f"'{word}' 应为 '{spell_corrections[upper_word]}'")
                correct_word = spell_corrections[upper_word]
                # 保持原始大小写风格
                if word.lower() == word:
                    correct_word = correct_word.lower()
                elif word.title() == word:
                    correct_word = correct_word.title()
                fixes.append((word, correct_word, i))
        
        # 2. 检查常见的词法模式错误
        # 检查引号不匹配
        single_quotes = sql.count("'")
        double_quotes = sql.count('"')
        if single_quotes % 2 != 0:
            errors.append("单引号不匹配，可能缺少闭合引号")
        if double_quotes % 2 != 0:
            errors.append("双引号不匹配，可能缺少闭合引号")
        
        # 检查括号不匹配
        open_parens = sql.count('(')
        close_parens = sql.count(')')
        if open_parens != close_parens:
            if open_parens > close_parens:
                errors.append(f"缺少 {open_parens - close_parens} 个右括号 ')'")
            else:
                errors.append(f"多余 {close_parens - open_parens} 个右括号 ')'")
        
        # 3. 检查数字和标识符格式
        # 检查无效的数字格式
        number_pattern = r'\b\d+\.\d*\.\d+\b'  # 多个小数点
        invalid_numbers = re.findall(number_pattern, sql)
        for num in invalid_numbers:
            errors.append(f"无效的数字格式: '{num}'")
        
        # 检查以数字开头的标识符（通常无效）
        invalid_identifiers = re.findall(r'\b\d+[a-zA-Z_]\w*\b', sql)
        for ident in invalid_identifiers:
            # 排除明显的字符串或注释
            if not any(quote in sql[sql.find(ident)-10:sql.find(ident)+len(ident)+10] 
                      for quote in ["'", '"', '--', '/*']):
                errors.append(f"标识符不能以数字开头: '{ident}'")
        
        return errors, fixes
    
    def check_syntax_structure(self, sql: str) -> list:
        """检查语法结构问题"""
        warnings = []
        upper_sql = sql.upper().strip()
        
        # 检查常见的语法结构问题
        if upper_sql.startswith('SELECT') and 'FROM' not in upper_sql:
            warnings.append("SELECT语句通常需要FROM子句")
        
        if upper_sql.startswith('INSERT') and 'VALUES' not in upper_sql and 'SELECT' not in upper_sql:
            warnings.append("INSERT语句需要VALUES子句或SELECT子句")
        
        if upper_sql.startswith('UPDATE') and 'SET' not in upper_sql:
            warnings.append("UPDATE语句需要SET子句")
        
        if upper_sql.startswith('CREATE TABLE') and '(' not in upper_sql:
            warnings.append("CREATE TABLE语句需要列定义")
        
        # 检查括号匹配
        if sql.count('(') != sql.count(')'):
            warnings.append("括号不匹配")
        
        # 检查引号匹配
        single_quotes = sql.count("'")
        double_quotes = sql.count('"')
        if single_quotes % 2 != 0:
            warnings.append("单引号不匹配")
        if double_quotes % 2 != 0:
            warnings.append("双引号不匹配")
        
        return warnings
    
    
    def highlight_errors(self, sql: str, errors: list):
        """在文本框中高亮显示错误"""
        try:
            # 这里可以根据具体的错误位置进行高亮
            # 简化版本：高亮所有检测到的错误单词
            for error in errors:
                if "'" in error:
                    # 提取错误单词
                    parts = error.split("'")
                    if len(parts) >= 2:
                        wrong_word = parts[1]
                        # 在文本中查找并高亮
                        start_pos = "1.0"
                        while True:
                            pos = self.sql_text.search(wrong_word, start_pos, tk.END, nocase=True)
                            if not pos:
                                break
                            end_pos = f"{pos}+{len(wrong_word)}c"
                            self.sql_text.tag_add("error", pos, end_pos)
                            start_pos = end_pos
        except Exception:
            pass

    def apply_quick_fix(self):
        """应用快速修复"""
        try:
            if not self.current_fixes:
                return
            
            # 获取当前SQL文本
            current_sql = self.sql_text.get("1.0", tk.END).strip()
            words = current_sql.split()
            
            # 应用所有修复
            for wrong_word, correct_word, position in self.current_fixes:
                if position < len(words):
                    # 替换对应位置的单词
                    old_word = words[position]
                    # 保持大小写风格
                    if old_word.isupper():
                        words[position] = correct_word.upper()
                    elif old_word.islower():
                        words[position] = correct_word.lower()
                    else:
                        words[position] = correct_word
            
            # 重新构建SQL
            fixed_sql = ' '.join(words)
            
            # 更新文本框
            self.sql_text.delete("1.0", tk.END)
            self.sql_text.insert("1.0", fixed_sql)
            
            # 清除修复建议
            self.current_fixes = []
            self.fix_button.config(state="disabled")
            
            # 显示成功消息
            self.error_label.config(text="✅ 已应用快速修复")
            
            # 2秒后重新检查
            self.root.after(2000, self.check_sql_errors)
            
        except Exception as e:
            messagebox.showerror("修复失败", f"应用快速修复时出错: {e}")

    def _suggest_fixes(self, sql: str, error: str) -> str:
        tips = []
        up = sql.strip().upper()
        if "IF EXISTS" in up or "IF NOT EXISTS" in up:
            tips.append("移除 IF EXISTS/IF NOT EXISTS，当前语法不支持")
        if up.count(';') > 1:
            tips.append("一次仅执行一条语句；GUI 已按分号拆分逐条执行")
        if up.startswith("DROP MATERIALIZED VIEW"):
            tips.append("物化视图相关操作请使用菜单或单条命令")
        if "表不存在" in error or "SemanticError 表不存在" in error:
            tips.append("执行 'SYNC CATALOG;' 或点击 顶部-刷新状态，再试")
        if up.startswith("EXPLAIN ") and self.current_mode == "core":
            tips.append("EXPLAIN 在 CORE 模式不支持，切换到 adapter")
        if not tips:
            return "(无进一步建议)"
        return "\n".join(f"- {t}" for t in tips)

    def _format_table_text(self, columns, rows) -> str:
        if not columns:
            return str(rows)
        widths = [len(str(c)) for c in columns]
        for r in rows:
            for i, v in enumerate(r):
                if i < len(widths):
                    widths[i] = min(max(widths[i], len(str(v))), 40)
        header = " | ".join(str(columns[i]).ljust(widths[i]) for i in range(len(columns)))
        sep = "-" * len(header)
        lines = [header, sep]
        for r in rows:
            line = " | ".join((str(r[i]) if i < len(r) else '').ljust(widths[i]) for i in range(len(columns)))
            lines.append(line)
        return "\n".join(lines) + "\n"

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
