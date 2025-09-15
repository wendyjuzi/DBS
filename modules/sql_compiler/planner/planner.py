# modules/sql_compiler/plan/planner.py

"""
执行计划生成器 (Logical Planner)

- 输入: AST (语法树)
- 输出: 优化后的执行计划 (JSON / 树结构)
- 支持语句: CREATE TABLE, INSERT, SELECT, DELETE, UPDATE
- 集成查询优化器进行计划优化
"""

import json
import sys
import os
from modules.sql_compiler.optimizer.query_optimizer import QueryOptimizer



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
    def __init__(self, ast_list, enable_optimization=True):
        self.ast_list = ast_list
        self.enable_optimization = enable_optimization
        self.optimizer = QueryOptimizer() if self.enable_optimization else None

    def generate_plan(self):
        plans = []
        for ast in self.ast_list:
            stmt_type = ast["type"]
            
            # 生成初始逻辑计划
            if stmt_type == "CREATE_TABLE":
                plan = self.plan_create(ast)
            elif stmt_type == "INSERT":
                plan = self.plan_insert(ast)
            elif stmt_type == "SELECT":
                plan = self.plan_select(ast)
            elif stmt_type == "UPDATE":
                plan = self.plan_update(ast)
            elif stmt_type == "DELETE":
                plan = self.plan_delete(ast)
            elif stmt_type == "DROP_TABLE":
                plan = self.plan_drop(ast)
            elif stmt_type in ["BEGIN_TRANSACTION", "COMMIT", "ROLLBACK"]:
                plan = self.plan_transaction(ast)
            elif stmt_type in ["CREATE_INDEX", "DROP_INDEX"]:
                plan = self.plan_index(ast)
            elif stmt_type in ["CREATE_TRIGGER", "DROP_TRIGGER"]:
                plan = self.plan_trigger(ast)
            elif stmt_type in ["CREATE_VIEW", "DROP_VIEW"]:
                plan = self.plan_view(ast)
            elif stmt_type in ["CREATE_PROCEDURE", "CREATE_FUNCTION", "DROP_PROCEDURE", "DROP_FUNCTION", "CALL_PROCEDURE"]:
                plan = self.plan_procedure(ast)
            elif stmt_type == "DELIMITER_STATEMENT":
                plan = self.plan_delimiter(ast)
            elif stmt_type in ["SHOW_TABLES", "SHOW_DATABASES", "SHOW_SCHEMAS"]:
                plan = self.plan_show(ast)
            else:
                raise PlanError(f"不支持的语句类型: {stmt_type}")
            
            # 对 SELECT、UPDATE、DELETE 语句应用优化
            if self.optimizer and stmt_type in ["SELECT", "UPDATE", "DELETE"]:
                print(f"🔧 对 {stmt_type} 语句应用查询优化...")
                optimized_plan = self.optimizer.optimize(plan)
                plans.append(optimized_plan)
            else:
                plans.append(plan)
                
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

    def plan_drop(self, ast):
        return LogicalPlan("DropTable", table=ast["table"])

    def plan_transaction(self, ast):
        """生成事务控制语句的执行计划"""
        stmt_type = ast["type"]
        
        if stmt_type == "BEGIN_TRANSACTION":
            return LogicalPlan("BeginTransaction")
        elif stmt_type == "COMMIT":
            return LogicalPlan("Commit")
        elif stmt_type == "ROLLBACK":
            return LogicalPlan("Rollback")
        else:
            raise PlanError(f"未知的事务语句类型: {stmt_type}")
    
    def plan_index(self, ast):
        """生成索引操作的执行计划"""
        stmt_type = ast["type"]
        
        if stmt_type == "CREATE_INDEX":
            return LogicalPlan(
                "CreateIndex",
                index_name=ast.get("index_name", ""),
                table_name=ast.get("table_name", ""),
                columns=ast.get("columns", []),
                index_type=ast.get("index_type", "BTREE"),
                is_unique=ast.get("is_unique", False),
                where_condition=ast.get("where_condition")
            )
        elif stmt_type == "DROP_INDEX":
            return LogicalPlan(
                "DropIndex",
                index_name=ast.get("index_name", ""),
                table_name=ast.get("table_name")
            )
        else:
            raise PlanError(f"未知的索引语句类型: {stmt_type}")
    
    def plan_trigger(self, ast):
        """生成触发器操作的执行计划"""
        stmt_type = ast["type"]
        
        if stmt_type == "CREATE_TRIGGER":
            # 从 AST 对象中提取触发器信息
            trigger_name = ast.get("value", "")
            timing = ""
            events = []
            table_name = ""
            for_each_row = False
            when_condition = None
            trigger_body = None
            
            # 从 children 中提取信息
            for child in ast.get("children", []):
                if child.get("type") == "TIMING":
                    timing = child.get("value", "")
                elif child.get("type") == "EVENTS":
                    events = child.get("value", "").split(",") if child.get("value") else []
                elif child.get("type") == "TABLE":
                    table_name = child.get("value", "")
                elif child.get("type") == "FOR_EACH_ROW":
                    for_each_row = child.get("value", "False") == "True"
                elif child.get("type") == "WHEN_CONDITION":
                    when_condition = child
                elif child.get("type") == "TRIGGER_BODY":
                    trigger_body = child
            
            return LogicalPlan(
                "CreateTrigger",
                trigger_name=trigger_name,
                timing=timing,
                events=events,
                table_name=table_name,
                for_each_row=for_each_row,
                when_condition=when_condition,
                trigger_body=trigger_body
            )
        elif stmt_type == "DROP_TRIGGER":
            return LogicalPlan(
                "DropTrigger",
                trigger_name=ast.get("trigger_name", ""),
                table_name=ast.get("table_name")
            )
        else:
            raise PlanError(f"未知的触发器语句类型: {stmt_type}")
    
    def plan_view(self, ast):
        """生成视图操作的执行计划"""
        stmt_type = ast["type"]
        
        if stmt_type == "CREATE_VIEW":
            return LogicalPlan(
                "CreateView",
                view_name=ast.get("view", ""),
                materialized=ast.get("materialized", False),
                columns=ast.get("columns", []),
                query=ast.get("query")
            )
        elif stmt_type == "DROP_VIEW":
            return LogicalPlan(
                "DropView",
                view_name=ast.get("view", ""),
                if_exists=ast.get("if_exists", False),
                drop_behavior=ast.get("drop_behavior")
            )
        else:
            raise PlanError(f"未知的视图语句类型: {stmt_type}")

    def plan_procedure(self, ast):
        """生成存储过程操作的执行计划"""
        stmt_type = ast["type"]
        
        if stmt_type in ["CREATE_PROCEDURE", "CREATE_FUNCTION"]:
            return LogicalPlan(
                "CreateProcedure",
                procedure_name=ast.get("procedure", ""),
                is_function=ast.get("is_function", False),
                parameters=ast.get("parameters", []),
                return_type=ast.get("return_type"),
                body=ast.get("body", [])
            )
        elif stmt_type in ["DROP_PROCEDURE", "DROP_FUNCTION"]:
            return LogicalPlan(
                "DropProcedure",
                procedure_name=ast.get("procedure", ""),
                is_function=ast.get("is_function", False),
                if_exists=ast.get("if_exists", False)
            )
        elif stmt_type == "CALL_PROCEDURE":
            return LogicalPlan(
                "CallProcedure",
                procedure_name=ast.get("procedure", ""),
                arguments=ast.get("arguments", [])
            )
        else:
            raise PlanError(f"未知的存储过程语句类型: {stmt_type}")

    def plan_show(self, ast):
        """生成 SHOW 语句的执行计划"""
        show_type = ast["show_type"]
        
        if show_type == "tables":
            return LogicalPlan("ShowTables")
        elif show_type == "databases":
            return LogicalPlan("ShowDatabases")
        elif show_type == "schemas":
            return LogicalPlan("ShowSchemas")
        else:
            raise PlanError(f"不支持的 SHOW 语句类型: {show_type}")

    def plan_delimiter(self, ast):
        """生成 DELIMITER 语句的执行计划"""
        delimiter = ast.get("value", "")
        return LogicalPlan(
            "DelimiterStatement",
            delimiter=delimiter
        )


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
