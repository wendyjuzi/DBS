from __future__ import annotations

from typing import Any, Dict, List
import sys
import os

from ..core.parser.simple_sql_parser import SimpleSQLParser
from ..core.optimizer.query_optimizer import QueryOptimizer
from ..core.executor.hybrid_executor import HybridExecutionEngine
from ..utils.exceptions import (
    SQLSyntaxError,
    ExecutionError,
    DatabaseError,
)

# 添加 cpp_core 目录到 Python 路径
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'cpp_core'))


class DatabaseAPI:
    """对外数据库 API，封装 SQL 编译与 C++ 引擎的对接。"""

    def __init__(self) -> None:
        # 延迟导入 C++ 扩展，确保已编译
        try:
            import db_core  # type: ignore
            from db_core import StorageEngine, ExecutionEngine  # type: ignore
        except Exception as exc:  # pragma: no cover - 明确报错
            raise DatabaseError(
                f"无法导入C++核心模块: {exc}. 请先在 cpp_core/ 目录编译扩展。"
            )

        # C++ 存储与执行引擎
        self._storage = db_core.StorageEngine()  # type: ignore
        self._executor = db_core.ExecutionEngine(self._storage)  # type: ignore

        # SQL 编译链路：优先使用 modules 版解析器，优化器暂沿用现有版本
        try:
            from modules.database_system.parser.simple_sql_parser import SimpleSQLParser as MSimpleSQLParser  # type: ignore
            self._parser = MSimpleSQLParser()
        except Exception:
            self._parser = SimpleSQLParser()
        self._optimizer = QueryOptimizer(self._storage, getattr(self._storage, "get_catalog", lambda: None)())
        self._runner = HybridExecutionEngine(self._storage, self._executor)
        # 兼容对外访问：允许通过 db.runner 访问执行器
        self.runner = self._runner

    def execute(self, sql: str) -> Dict[str, Any]:
        """
        执行一条 SQL，返回标准化结果：
        { status, data, affected_rows, metadata, execution_time }
        """
        try:
            ast = self._parser.parse(sql)
            plan = self._optimizer.optimize(ast)
            result = self._runner.execute(plan)
            return {
                "status": "success",
                "data": result.get("data", []),
                "affected_rows": result.get("affected_rows", 0),
                "metadata": result.get("metadata", {}),
                "execution_time": result.get("execution_time", 0.0),
            }
        except (SQLSyntaxError, ExecutionError, DatabaseError) as err:
            return {
                "status": "error",
                "error": str(err),
                "data": [],
                "affected_rows": 0,
                "metadata": {},
                "execution_time": 0.0,
            }

    def flush(self) -> None:
        """主动刷盘。"""
        try:
            self._runner.flush_all_dirty_pages()
        except Exception:
            pass

    # --- 事务 API ---
    def begin(self) -> str:
        return getattr(self._runner, "begin")()

    def commit(self) -> None:
        return getattr(self._runner, "commit")()

    def rollback(self) -> None:
        return getattr(self._runner, "rollback")()

# --- compat helpers for REST layer ---
_db_singleton = None

def get_database(data_dir: str = 'data') -> DatabaseAPI:
    """兼容 REST 层调用，返回单例实例。data_dir 参数保留但当前未使用。"""
    global _db_singleton
    if _db_singleton is None:
        _db_singleton = DatabaseAPI()
    return _db_singleton

def clear_database_instances() -> None:
    """清理单例（测试或重置时使用）。"""
    global _db_singleton
    _db_singleton = None