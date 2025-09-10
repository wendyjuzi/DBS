"""
数据库API接口层

实现 SQL → 解析 → 优化 → 执行 的完整链路，
并将执行下推到 C++ 执行/存储引擎（插入、顺序扫描、过滤、投影、删除）。
"""

from __future__ import annotations

from typing import Any, Dict, List

from ..core.parser.simple_sql_parser import SimpleSQLParser
from ..core.optimizer.query_optimizer import QueryOptimizer
from ..core.executor.hybrid_executor import HybridExecutionEngine
from ..utils.exceptions import (
    SQLSyntaxError,
    ExecutionError,
    DatabaseError,
)


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

        # SQL 编译链路
        self._parser = SimpleSQLParser()
        self._optimizer = QueryOptimizer(self._storage, getattr(self._storage, "get_catalog", lambda: None)())
        self._runner = HybridExecutionEngine(self._storage, self._executor)

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
