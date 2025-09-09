"""
查询优化器
"""

from __future__ import annotations

from typing import Any, Dict


class QueryOptimizer:
    """占位优化器，直接返回计划。"""

    def optimize(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        return plan
