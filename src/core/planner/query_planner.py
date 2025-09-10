"""
查询计划生成器
"""

from __future__ import annotations

from typing import Any, Dict


class QueryPlanner:
    """将AST映射为极简逻辑计划。这里基本是直传。"""

    def generate_plan(self, ast: Dict[str, Any], catalog) -> Dict[str, Any]:
        return ast
