"""
查询优化器 - 实现各种查询优化技术

支持的优化技术：
1. 谓词下推 (Predicate Pushdown)
2. 投影下推 (Projection Pushdown)
3. 连接重排序 (Join Reordering)
4. 常量折叠 (Constant Folding)
5. 死代码消除 (Dead Code Elimination)
"""

import copy
from typing import List, Dict, Any, Optional, TYPE_CHECKING

# 避免循环导入，仅在类型检查时导入
if TYPE_CHECKING:
    from modules.sql_compiler.planner.planner import LogicalPlan


class QueryOptimizer:
    """查询优化器主类"""

    def __init__(self):
        self.optimization_rules = [
            self._predicate_pushdown,
            self._projection_pushdown,
            self._constant_folding,
            self._dead_code_elimination,
            self._join_reordering
        ]

    def optimize(self, plan: 'LogicalPlan') -> 'LogicalPlan':
        """
        对逻辑执行计划进行优化

        Args:
            plan: 原始逻辑执行计划

        Returns:
            优化后的逻辑执行计划
        """
        optimized_plan = copy.deepcopy(plan)

        # 应用所有优化规则，直到计划不再变化
        max_iterations = 10  # 防止无限循环
        for iteration in range(max_iterations):
            old_plan_str = str(optimized_plan)

            # 依次应用所有优化规则
            for rule in self.optimization_rules:
                optimized_plan = rule(optimized_plan)

            # 如果计划没有变化，停止优化
            if str(optimized_plan) == old_plan_str:
                break

        return optimized_plan

    def _predicate_pushdown(self, plan: 'LogicalPlan') -> 'LogicalPlan':
        """
        谓词下推优化

        将 WHERE 条件尽可能推到靠近数据源的位置，
        减少中间结果集的大小，提高查询性能。

        规则：
        1. 将涉及单表的过滤条件推到表扫描之后
        2. 将 JOIN 条件中的等值条件保留在 JOIN 处
        3. 将 JOIN 后的过滤条件根据涉及的表进行拆分
        """
        return self._apply_predicate_pushdown(plan)

    def _apply_predicate_pushdown(self, plan: 'LogicalPlan') -> 'LogicalPlan':
        """递归应用谓词下推"""
        if plan.node_type == "Filter":
            # 获取过滤条件
            condition = plan.props.get("condition", {})

            # 检查子节点类型
            if plan.children and plan.children[0].node_type.endswith("Join"):
                # 过滤条件在 JOIN 之上
                return self._push_filter_through_join(plan, condition)
            elif plan.children and plan.children[0].node_type == "SeqScan":
                # 过滤条件在表扫描之上，将条件合并到扫描节点
                return self._merge_filter_into_scan(plan, condition)

        # 递归处理子节点
        new_plan = copy.deepcopy(plan)
        for i, child in enumerate(new_plan.children):
            new_plan.children[i] = self._apply_predicate_pushdown(child)

        return new_plan

    def _push_filter_through_join(self, filter_plan: 'LogicalPlan', condition: Dict) -> 'LogicalPlan':
        """将过滤条件推到 JOIN 下方"""
        join_plan = filter_plan.children[0]
        left_child = join_plan.children[0] if join_plan.children else None
        right_child = join_plan.children[1] if len(join_plan.children) > 1 else None

        if not left_child or not right_child:
            return filter_plan

        # 分析过滤条件涉及的表
        left_tables = self._get_tables_in_subtree(left_child)
        right_tables = self._get_tables_in_subtree(right_child)

        # 根据条件涉及的表决定推送位置
        filter_column = condition.get("left", "")

        if self._column_belongs_to_tables(filter_column, left_tables):
            # 条件只涉及左侧表，推到左侧
            from modules.sql_compiler.planner.planner import LogicalPlan
            new_filter = LogicalPlan("Filter", condition=condition)
            new_filter.add_child(left_child)
            join_plan.children[0] = new_filter
            return join_plan
        elif self._column_belongs_to_tables(filter_column, right_tables):
            # 条件只涉及右侧表，推到右侧
            from modules.sql_compiler.planner.planner import LogicalPlan
            new_filter = LogicalPlan("Filter", condition=condition)
            new_filter.add_child(right_child)
            join_plan.children[1] = new_filter
            return join_plan
        else:
            # 条件涉及多表，保持在 JOIN 之上
            return filter_plan

    def _merge_filter_into_scan(self, filter_plan: 'LogicalPlan', condition: Dict) -> 'LogicalPlan':
        """将过滤条件合并到SeqScan节点中"""
        scan_plan = filter_plan.children[0]
        
        # 创建新的扫描节点，包含过滤条件
        new_scan = copy.deepcopy(scan_plan)
        
        # 将过滤条件添加到扫描节点的属性中
        if 'conditions' not in new_scan.props:
            new_scan.props['conditions'] = []
        new_scan.props['conditions'].append(condition)
        
        # 合并条件到单个condition字段（保持向后兼容）
        new_scan.props['condition'] = condition
        
        return new_scan

    def _projection_pushdown(self, plan: 'LogicalPlan') -> 'LogicalPlan':
        """
        投影下推优化

        将 SELECT 列表尽可能推到靠近数据源的位置，
        减少传输的数据量，特别是在 JOIN 操作中。
        """
        return self._apply_projection_pushdown(plan, set())

    def _apply_projection_pushdown(self, plan: 'LogicalPlan', required_columns: set) -> 'LogicalPlan':
        """递归应用投影下推"""
        if plan.node_type == "Project":
            # 收集需要的列
            columns = plan.props.get("columns", [])
            required_columns.update(columns)

            # 继续向下推送
            if plan.children:
                optimized_child = self._apply_projection_pushdown(plan.children[0], required_columns)
                plan.children[0] = optimized_child

        elif plan.node_type.endswith("Join"):
            # 在 JOIN 节点，需要考虑 JOIN 条件需要的列
            join_condition = plan.props.get("condition", {})
            if join_condition:
                required_columns.add(join_condition.get("left", ""))
                required_columns.add(join_condition.get("right", ""))

            # 为左右子树分别推送相关列
            if plan.children:
                left_child = plan.children[0]
                right_child = plan.children[1] if len(plan.children) > 1 else None

                left_tables = self._get_tables_in_subtree(left_child)
                right_tables = self._get_tables_in_subtree(right_child) if right_child else set()

                left_columns = {col for col in required_columns
                                if self._column_belongs_to_tables(col, left_tables)}
                right_columns = {col for col in required_columns
                                 if self._column_belongs_to_tables(col, right_tables)}

                plan.children[0] = self._apply_projection_pushdown(left_child, left_columns)
                if right_child:
                    plan.children[1] = self._apply_projection_pushdown(right_child, right_columns)

        elif plan.node_type == "SeqScan":
            # 在扫描节点，不创建新的投影节点
            # 扫描节点本身不需要投影，由上层的Project节点处理
            pass

        return plan

    def _constant_folding(self, plan: 'LogicalPlan') -> 'LogicalPlan':
        """
        常量折叠优化
        
        在编译时计算常量表达式，减少运行时计算开销。
        """
        # 递归处理子节点
        new_plan = copy.deepcopy(plan)
        for i, child in enumerate(new_plan.children):
            new_plan.children[i] = self._constant_folding(child)

        # 对当前节点进行常量折叠
        if plan.node_type == "Filter":
            condition = plan.props.get("condition", {})
            if self._is_constant_condition(condition):
                # 如果条件总是为真，移除过滤器
                if self._evaluate_constant_condition(condition):
                    return new_plan.children[0] if new_plan.children else new_plan
                # 如果条件总是为假，返回空结果
                else:
                    # 运行时导入避免循环依赖
                    from modules.sql_compiler.planner.planner import LogicalPlan
                    return LogicalPlan("EmptyResult")

        return new_plan

    def _dead_code_elimination(self, plan: 'LogicalPlan') -> 'LogicalPlan':
        """
        死代码消除优化
        
        移除不被使用的计算和数据获取操作。
        """
        # 递归处理子节点
        new_plan = copy.deepcopy(plan)
        for i, child in enumerate(new_plan.children):
            new_plan.children[i] = self._dead_code_elimination(child)

        # 检查当前节点是否可以消除
        if plan.node_type == "Project":
            columns = plan.props.get("columns", [])
            if not columns:
                # 空投影，返回子节点
                return new_plan.children[0] if new_plan.children else new_plan
            
            # 检查是否有重复的Project节点
            if (new_plan.children and 
                len(new_plan.children) == 1 and 
                new_plan.children[0].node_type == "Project"):
                
                child_columns = new_plan.children[0].props.get("columns", [])
                # 如果子Project的列是当前Project列的超集或相等，可以合并
                if set(columns).issubset(set(child_columns)):
                    # 更新子Project的列为当前Project的列
                    new_plan.children[0].props["columns"] = columns
                    return new_plan.children[0]

        return new_plan

    def _join_reordering(self, plan: 'LogicalPlan') -> 'LogicalPlan':
        """
        连接重排序优化
        
        根据选择性和表大小重新排列 JOIN 顺序，
        通常将选择性高的 JOIN 放在前面执行。
        """
        # 递归处理子节点
        new_plan = copy.deepcopy(plan)
        for i, child in enumerate(new_plan.children):
            new_plan.children[i] = self._join_reordering(child)

        # 如果当前节点是 JOIN，考虑重排序
        if plan.node_type.endswith("Join") and len(plan.children) == 2:
            left_child = plan.children[0]
            right_child = plan.children[1]

            # 简单的启发式：优先选择扫描节点作为右侧
            if (left_child.node_type.endswith("Join") and
                    right_child.node_type == "SeqScan"):
                # 保持当前顺序
                pass
            elif (left_child.node_type == "SeqScan" and
                  right_child.node_type.endswith("Join")):
                # 交换左右子树
                new_plan.children[0] = right_child
                new_plan.children[1] = left_child

        return new_plan

    # 辅助方法
    def _get_tables_in_subtree(self, plan: 'LogicalPlan') -> set:
        """获取子树中涉及的所有表"""
        tables = set()

        if plan.node_type == "SeqScan":
            table = plan.props.get("table", "")
            if table:
                tables.add(table)

        # 递归收集子节点的表
        for child in plan.children:
            tables.update(self._get_tables_in_subtree(child))

        return tables

    def _column_belongs_to_tables(self, column: str, tables: set) -> bool:
        """检查列是否属于指定的表集合"""
        if '.' in column:
            table_name = column.split('.')[0]
            return table_name in tables
        else:
            # 简单列名，假设属于所有表
            return len(tables) > 0

    def _is_constant_condition(self, condition: Dict) -> bool:
        """检查条件是否为常量条件"""
        left = condition.get("left", "")
        right = condition.get("right", "")

        # 如果左右都是常量，则为常量条件
        return (self._is_constant(left) and self._is_constant(right))

    def _is_constant(self, value: str) -> bool:
        """检查值是否为常量"""
        try:
            float(value)
            return True
        except:
            return value.startswith("'") and value.endswith("'")

    def _evaluate_constant_condition(self, condition: Dict) -> bool:
        """计算常量条件的值"""
        # 简化实现，实际应该根据操作符进行计算
        return True  # 假设条件总是为真


class OptimizationStatistics:
    """优化统计信息"""

    def __init__(self):
        self.rules_applied = {}
        self.optimization_time = 0
        self.plan_complexity_before = 0
        self.plan_complexity_after = 0

    def record_rule_application(self, rule_name: str):
        """记录规则应用"""
        self.rules_applied[rule_name] = self.rules_applied.get(rule_name, 0) + 1

    def get_summary(self) -> Dict[str, Any]:
        """获取优化摘要"""
        return {
            "rules_applied": self.rules_applied,
            "optimization_time": self.optimization_time,
            "complexity_reduction": self.plan_complexity_before - self.plan_complexity_after,
            "total_rules": sum(self.rules_applied.values())
        }
