# modules/sql_compiler/plan/planner.py

"""
执行计划生成器 (Logical Planner)

- 输入: AST (语法树)
- 输出: 执行计划 (JSON / 树结构)
- 支持语句: CREATE TABLE, INSERT, SELECT, DELETE
"""

import json


import json


class PlanError(Exception):
    """执行计划错误"""
    pass


class LogicalPlan:
    """逻辑执行计划基类"""
    def __init__(self, node_type, **kwargs):
        self.node_type = node_type
        self.children = []
        self.props = kwargs

    def add_child(self, child):
        self.children.append(child)

    def to_dict(self):
        return {
            "type": self.node_type,
            "props": self.props,
            "children": [c.to_dict() for c in self.children]
        }

    def __repr__(self):
        return json.dumps(self.to_dict(), indent=2, ensure_ascii=False)


class Planner:
    def __init__(self, ast_list):
        self.ast_list = ast_list

    def generate_plan(self):
        plans = []
        for ast in self.ast_list:
            stmt_type = ast["type"]
            if stmt_type == "CREATE_TABLE":
                plans.append(self.plan_create(ast))
            elif stmt_type == "INSERT":
                plans.append(self.plan_insert(ast))
            elif stmt_type == "SELECT":
                plans.append(self.plan_select(ast))
            elif stmt_type == "UPDATE":
                plans.append(self.plan_update(ast))
            elif stmt_type == "DELETE":
                plans.append(self.plan_delete(ast))
            else:
                raise PlanError(f"不支持的语句类型: {stmt_type}")
        return plans

    def plan_create(self, ast):
        return LogicalPlan(
            "CreateTable",
            table=ast["table"],
            columns=ast["columns"]
        )

    def plan_insert(self, ast):
        root = LogicalPlan("Insert", table=ast["table"], columns=ast["columns"])
        values = LogicalPlan("Values", rows=[ast["values"]])
        root.add_child(values)
        return root

    def plan_select(self, ast):
        # 构建基础扫描节点
        scan = LogicalPlan("SeqScan", table=ast["table"])
        current_node = scan
        
        # 处理 JOIN
        if ast.get("joins"):
            for join in ast["joins"]:
                join_scan = LogicalPlan("SeqScan", table=join["table"])
                join_node = LogicalPlan(f"{join['type']}Join", 
                                      condition=join["on"])
                join_node.add_child(current_node)
                join_node.add_child(join_scan)
                current_node = join_node
        
        # 处理 WHERE 子句
        if ast["where"] is not None:
            filter_node = LogicalPlan("Filter", condition=ast["where"])
            filter_node.add_child(current_node)
            current_node = filter_node
        
        # 处理 GROUP BY
        if ast.get("group_by"):
            group_node = LogicalPlan("GroupBy", columns=ast["group_by"])
            group_node.add_child(current_node)
            current_node = group_node
        
        # 处理 ORDER BY
        if ast.get("order_by"):
            sort_node = LogicalPlan("Sort", columns=ast["order_by"])
            sort_node.add_child(current_node)
            current_node = sort_node
        
        # 最终的投影操作
        project_node = LogicalPlan("Project", columns=ast["columns"])
        project_node.add_child(current_node)
        
        return project_node

    def plan_update(self, ast):
        root = LogicalPlan("Update", table=ast["table"], assignments=ast["assignments"])
        scan = LogicalPlan("SeqScan", table=ast["table"])
        if ast["where"] is not None:
            filter_node = LogicalPlan("Filter", condition=ast["where"])
            filter_node.add_child(scan)
            root.add_child(filter_node)
        else:
            root.add_child(scan)
        return root

    def plan_delete(self, ast):
        root = LogicalPlan("Delete", table=ast["table"])
        scan = LogicalPlan("SeqScan", table=ast["table"])
        if ast["where"] is not None:
            filt = LogicalPlan("Filter", condition=ast["where"])
            filt.add_child(scan)
            root.add_child(filt)
        else:
            root.add_child(scan)
        return root


if __name__ == "__main__":
    # 假设 parser 生成了 AST
    ast_list = [
        {
            "type": "CREATE_TABLE",
            "table": "student",
            "columns": [
                {"name": "id", "type": "INT"},
                {"name": "name", "type": "VARCHAR"},
                {"name": "age", "type": "INT"}
            ]
        },
        {
            "type": "INSERT",
            "table": "student",
            "columns": ["id", "name", "age"],
            "values": [1, "Alice", 20]
        },
        {
            "type": "SELECT",
            "table": "student",
            "columns": ["id", "name"],
            "where": {"left": "age", "op": ">", "right": 18}
        },
        {
            "type": "DELETE",
            "table": "student",
            "where": {"left": "id", "op": "=", "right": 1}
        },
    ]

    planner = Planner(ast_list)
    plans = planner.generate_plan()

    for p in plans:
        print(p)
