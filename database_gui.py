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
        self.sql_text.pack(fill=tk.BOTH, expand=True, pady=(5, 10))

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

        self.compiler_info = tk.Text(status_frame, height=6, font=("Consolas", 9))
        self.compiler_info.pack(fill=tk.X)

        # 语法分析结果
        syntax_frame = ttk.LabelFrame(parent, text="语法分析", padding=10)
        syntax_frame.pack(fill=tk.BOTH, expand=True)

        self.syntax_tree = scrolledtext.ScrolledText(syntax_frame, font=("Consolas", 9))
        self.syntax_tree.pack(fill=tk.BOTH, expand=True)

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
        menu_dist.add_command(label="分布式查询(合并)", command=self.run_distributed_select)
        menu_dist.add_command(label="分布式SUM(id)", command=self.run_distributed_sum)
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

    def dist_insert_row(self, id_val: int, name: str):
        if not (self.dist_router and self.dist_nodes):
            return
        s = self.dist_router.locate_by_value('T', str(id_val))
        if not s:
            return
        node = self.dist_nodes.get(s[0]['id']) or next(iter(self.dist_nodes.values()))
        node.execute(f"INSERT INTO T(id,name) VALUES ({id_val}, '{name}');")

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

        if not BACKEND_AVAILABLE:
            self.result_text.insert(tk.END, "演示模式: 后端不可用\n")
            self.result_text.insert(tk.END, f"模拟执行: {sql[:100]}...\n")
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
        # 同步一次目录，确保能识别历史表
        try:
            if BACKEND_AVAILABLE and self.adapter and hasattr(self.adapter, 'sync_catalog'):
                self.adapter.sync_catalog()
        except Exception:
            pass
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

    # ====== 错误启发式智能建议（GUI侧） ======
    def _suggest_fixes(self, sql_text: str, err_text: str) -> str:
        tips = []
        up_sql = (sql_text or "").upper()
        up_err = (err_text or "").upper()
        # 1) 多条语句一次执行
        if (';' in sql_text.strip() and sql_text.strip().count(';') > 1) or "\n" in sql_text:
            tips.append("💭 1. 每次只执行一条SQL语句，带分号，逐条运行")
        # 2) 不支持 IF EXISTS
        if "IF EXISTS" in up_sql:
            tips.append("🎯 2. 适配器不支持 IF EXISTS，请去掉 IF EXISTS 重试")
        # 3) DROP MATERIALIZED VIEW 语法
        if "DROP MATERIALIZED VIEW" in up_sql or "MATERIALIZED" in up_err:
            tips.append("💭 3. 物化视图建议用菜单‘视图→删除物化视图...’操作")
        # 4) 表不存在/已存在
        if ("表不存在" in err_text) or ("TABLE" in up_err and "NOT EXIST" in up_err):
            tips.append("🎯 4. 请先 CREATE TABLE 再操作，或检查表名拼写；用“执行→EXPLAIN”查看计划")
        if ("已存在" in err_text) or ("ALREADY EXISTS" in up_err):
            tips.append("💭 5. 对象已存在，请先 DROP 再 CREATE，或换个名称")
        # 5) 标识符/关键字冲突
        if "EXPECTED TOKEN TYPE IDENTIFIER" in up_err and "KEYWORD" in up_err:
            tips.append("🎯 6. 关键字与标识符冲突，检查是否误写关键字或缺少空格/逗号/括号")
        # 6) 事务/分号缺失
        if "EXPECTED ;" in up_err or "EXPECTED DELIMITER" in up_err:
            tips.append("💭 7. 语句结尾缺少分号；请在每条语句末尾添加 ;")
        # 7) 视图/过程/触发器语法
        if any(k in up_sql for k in ["CREATE VIEW", "CREATE MATERIALIZED VIEW", "CREATE PROCEDURE", "CREATE TRIGGER"]):
            tips.append("💭 8. 这些高级语句建议先在菜单中使用相应向导创建，便于校验语法")
        # 汇总
        if not tips:
            return "(无进一步建议)\n"
        return "\n".join(tips) + "\n"

    # 公共：将列/数据格式化为文本表格
    def _format_table_text(self, columns, data, max_width=30):
        try:
            if not columns or not data:
                return "(无数据)\n"
            widths = []
            for i, col in enumerate(columns):
                m = len(str(col))
                for row in data:
                    if i < len(row):
                        m = max(m, len(str(row[i])))
                widths.append(min(m, max_width))
            header = " | ".join(str(columns[i]).ljust(widths[i]) for i in range(len(columns)))
            sep = "-" * len(header)
            lines = [header, sep]
            for row in data:
                vals = []
                for i in range(len(columns)):
                    val = str(row[i]) if i < len(row) else ""
                    vals.append(val[:max_width].ljust(widths[i]))
                lines.append(" | ".join(vals))
            return "\n".join(lines) + "\n"
        except Exception:
            return "(格式化失败)\n"

    # ====== 事务与管理动作 ======
    def _exec_and_display(self, sql: str):
        if not BACKEND_AVAILABLE:
            self.result_text.insert(tk.END, "演示模式: 后端不可用\n")
            return
        try:
            start_time = time.time()
            result = self.adapter.execute(sql)
            self.display_result(result, time.time() - start_time)
            self.update_monitors()
        except Exception as e:
            self.result_text.insert(tk.END, f"错误: {e}\n")
            self.log(f"❌ 执行失败: {e}")

    def tx_begin(self):
        self.log("BEGIN 事务")
        self._exec_and_display("BEGIN;")

    def tx_commit(self):
        self.log("COMMIT 提交")
        self._exec_and_display("COMMIT;")

    def tx_rollback(self):
        self.log("ROLLBACK 回滚")
        self._exec_and_display("ROLLBACK;")

    def set_autocommit(self, on: bool):
        state = "ON" if on else "OFF"
        self.log(f"设置 AUTOCOMMIT = {state}")
        self._exec_and_display(f"SET AUTOCOMMIT = {state};")

    def show_transaction(self):
        self.log("SHOW TRANSACTION")
        self._exec_and_display("SHOW TRANSACTION;")

    def show_overlay(self):
        self.log("SHOW OVERLAY")
        # SHOW OVERLAY 不是适配器标准命令，但 CLI 支持展示；尝试通过 UnifiedAPI，如果失败则退化为执行命令
        try:
            from src.api.unified_api import UnifiedDB
            uni = getattr(self, "_unified", None) or UnifiedDB()
            self._unified = uni
            snap = uni.show_tx_overlay()
            self.result_text.insert(tk.END, "事务覆盖层:\n")
            self.result_text.insert(tk.END, str(snap) + "\n")
        except Exception:
            self._exec_and_display("SHOW OVERLAY;")

    def explain_sql(self):
        sql = self.sql_text.get(1.0, tk.END).strip()
        if not sql:
            messagebox.showwarning("警告", "请输入SQL语句以进行 EXPLAIN")
            return
        self.log("EXPLAIN 当前SQL")
        self._exec_and_display("EXPLAIN " + (sql if sql.endswith(';') else sql + ';'))

    def import_table_dialog(self):
        if not BACKEND_AVAILABLE:
            messagebox.showwarning("提示", "后端不可用")
            return
        try:
            import_win = tk.Toplevel(self.root)
            import_win.title("导入表")

            ttk.Label(import_win, text="表名:").grid(row=0, column=0, padx=6, pady=6, sticky=tk.W)
            table_var = tk.StringVar()
            ttk.Entry(import_win, textvariable=table_var, width=28).grid(row=0, column=1, padx=6, pady=6)

            ttk.Label(import_win, text="格式(csv/json):").grid(row=1, column=0, padx=6, pady=6, sticky=tk.W)
            fmt_var = tk.StringVar(value="csv")
            ttk.Combobox(import_win, textvariable=fmt_var, values=["csv", "json"], state="readonly", width=10).grid(row=1, column=1, padx=6, pady=6, sticky=tk.W)

            ttk.Label(import_win, text="文件:").grid(row=2, column=0, padx=6, pady=6, sticky=tk.W)
            path_var = tk.StringVar()
            path_entry = ttk.Entry(import_win, textvariable=path_var, width=40)
            path_entry.grid(row=2, column=1, padx=6, pady=6)
            def choose_file():
                f = filedialog.askopenfilename(filetypes=[("CSV", "*.csv"), ("JSON", "*.json"), ("所有文件", "*.*")])
                if f:
                    path_var.set(f)
            ttk.Button(import_win, text="选择...", command=choose_file).grid(row=2, column=2, padx=6, pady=6)

            def do_import():
                table = table_var.get().strip()
                fmt = fmt_var.get().strip().lower()
                path = path_var.get().strip()
                if not table or not path or fmt not in ("csv", "json"):
                    messagebox.showwarning("警告", "请填写完整参数")
                    return
                try:
                    ok = self.adapter.import_table(table, fmt, path)
                    if ok:
                        self.log(f"✓ 导入成功: {path} → {table}")
                        self.update_tables_info()
                        import_win.destroy()
                    else:
                        messagebox.showerror("错误", "导入失败")
                except Exception as e:
                    messagebox.showerror("错误", f"导入失败: {e}")

            ttk.Button(import_win, text="开始导入", command=do_import).grid(row=3, column=1, padx=6, pady=12, sticky=tk.W)

        except Exception as e:
            messagebox.showerror("错误", f"打开导入对话框失败: {e}")

    def export_table_dialog(self):
        if not BACKEND_AVAILABLE:
            messagebox.showwarning("提示", "后端不可用")
            return
        try:
            export_win = tk.Toplevel(self.root)
            export_win.title("导出表")

            ttk.Label(export_win, text="表名:").grid(row=0, column=0, padx=6, pady=6, sticky=tk.W)
            table_var = tk.StringVar()
            ttk.Entry(export_win, textvariable=table_var, width=28).grid(row=0, column=1, padx=6, pady=6)

            ttk.Label(export_win, text="格式(csv/json):").grid(row=1, column=0, padx=6, pady=6, sticky=tk.W)
            fmt_var = tk.StringVar(value="csv")
            ttk.Combobox(export_win, textvariable=fmt_var, values=["csv", "json"], state="readonly", width=10).grid(row=1, column=1, padx=6, pady=6, sticky=tk.W)

            ttk.Label(export_win, text="保存到:").grid(row=2, column=0, padx=6, pady=6, sticky=tk.W)
            path_var = tk.StringVar()
            path_entry = ttk.Entry(export_win, textvariable=path_var, width=40)
            path_entry.grid(row=2, column=1, padx=6, pady=6)
            def choose_path():
                f = filedialog.asksaveasfilename(defaultextension=f".{fmt_var.get()}", filetypes=[("CSV", "*.csv"), ("JSON", "*.json"), ("所有文件", "*.*")])
                if f:
                    path_var.set(f)
            ttk.Button(export_win, text="选择...", command=choose_path).grid(row=2, column=2, padx=6, pady=6)

            def do_export():
                table = table_var.get().strip()
                fmt = fmt_var.get().strip().lower()
                path = path_var.get().strip()
                if not table or not path or fmt not in ("csv", "json"):
                    messagebox.showwarning("警告", "请填写完整参数")
                    return
                try:
                    ok = self.adapter.export_table(table, fmt, path)
                    if ok:
                        self.log(f"✓ 导出成功: {table} → {path}")
                        export_win.destroy()
                    else:
                        messagebox.showerror("错误", "导出失败")
                except Exception as e:
                    messagebox.showerror("错误", f"导出失败: {e}")

            ttk.Button(export_win, text="开始导出", command=do_export).grid(row=3, column=1, padx=6, pady=12, sticky=tk.W)

        except Exception as e:
            messagebox.showerror("错误", f"打开导出对话框失败: {e}")

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
