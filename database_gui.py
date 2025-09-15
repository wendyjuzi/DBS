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

        if not BACKEND_AVAILABLE:
            self.result_text.insert(tk.END, "演示模式: 后端不可用\n")
            self.result_text.insert(tk.END, f"模拟执行: {sql[:100]}...\n")
            return

        try:
            self.log(f"执行SQL ({self.current_mode}模式): {sql[:50]}...")

            start_time = time.time()

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
