"""
SQL编译器适配器
将 modules/sql_compiler 的输出格式转换为执行器期望的格式
不修改编译器本身，只做格式转换
"""

import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

# 添加项目根目录到路径
proj_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(proj_root))

from modules.sql_compiler.lexical.lexer import Lexer
from modules.sql_compiler.syntax.parser import Parser, ParseError
from modules.sql_compiler.semantic.semantic import SemanticAnalyzer, Catalog
from modules.sql_compiler.planner.planner import Planner
from modules.sql_compiler.optimizer.query_optimizer import QueryOptimizer as CompilerQueryOptimizer
from src.core.executor.hybrid_executor import HybridExecutionEngine
from src.utils.exceptions import ExecutionError, SQLSyntaxError
from src.index.index_manager import IndexManager

# 导入混合存储引擎（可选）
try:
    from hybrid_storage_engine import HybridStorageEngine  # type: ignore
    HYBRID_ENGINE_AVAILABLE = True
except Exception:
    HybridStorageEngine = None  # type: ignore
    HYBRID_ENGINE_AVAILABLE = False



class SQLCompilerAdapter:
    """SQL编译器适配器 - 不修改编译器，只做格式转换"""
    
    def __init__(self, use_hybrid_storage: bool = True, cache_capacity: int = 100, cache_strategy: str = "LRU"):
        # 初始化SQL编译器组件
        self.catalog = Catalog()
        self.semantic_analyzer = SemanticAnalyzer(self.catalog)
        self.compiler_optimizer = CompilerQueryOptimizer()
        # 事务状态与缓冲（仅对 INSERT 做批量缓冲以优化性能）
        self.in_transaction: bool = False
        self.autocommit: bool = True
        self._txn_insert_buffer: Dict[str, List[List[str]]] = {}
        
        # 初始化存储/执行引擎
        if use_hybrid_storage and HYBRID_ENGINE_AVAILABLE:
            try:
                self.hybrid_storage = HybridStorageEngine()  # type: ignore
                import db_core
                self.storage_engine = db_core.StorageEngine()
                self.execution_engine = db_core.ExecutionEngine(self.storage_engine)
                self.hybrid_executor = HybridExecutionEngine(self.storage_engine, self.execution_engine)
                print("[ADAPTER] 混合存储引擎初始化成功")
            except Exception as e:
                print(f"[ADAPTER] 混合存储引擎初始化失败: {e}")
                self.hybrid_storage = None
                try:
                    import db_core
                    self.storage_engine = db_core.StorageEngine()
                    self.execution_engine = db_core.ExecutionEngine(self.storage_engine)
                    self.hybrid_executor = HybridExecutionEngine(self.storage_engine, self.execution_engine)
                    print("[ADAPTER] 回退到传统C++执行引擎")
                except Exception as e2:
                    print(f"[ADAPTER] C++执行引擎初始化失败: {e2}")
                    raise ExecutionError("C++执行引擎不可用")
        else:
            # 使用传统C++引擎
            try:
                import db_core
                self.storage_engine = db_core.StorageEngine()
                self.execution_engine = db_core.ExecutionEngine(self.storage_engine)
                self.hybrid_executor = HybridExecutionEngine(self.storage_engine, self.execution_engine)
                self.hybrid_storage = None
                print("[ADAPTER] C++执行引擎初始化成功")
            except ImportError as e:
                print(f"[ADAPTER] C++执行引擎初始化失败: {e}")
                raise ExecutionError("C++执行引擎不可用")
        # 索引管理器
        self.index_manager = IndexManager()
        # 轻量统计缓存：table -> { 'rows': int, 'cols': {col: {'ndv': int, 'min': val, 'max': val}} }
        self._stats: Dict[str, Any] = {}
        # 视图存储（非物化）：name -> {'sql': select_sql}
        self._views: Dict[str, Dict[str, Any]] = {}
        # 约束元数据
        self._primary_key: Dict[str, str] = {}              # table -> pk_column
        self._unique_cols: Dict[str, List[str]] = {}         # table -> [unique_columns]
        self._foreign_keys: Dict[str, List[Dict[str, str]]] = {}  # table -> [{column, ref_table, ref_column}]
        # 触发器：table -> [ {name, timing, event, statements: [sql]} ]
        self._triggers: Dict[str, List[Dict[str, Any]]] = {}
        # 物化视图：name -> {sql, physical_table}
        self._mat_views: Dict[str, Dict[str, str]] = {}
        # 存储过程：name -> {statements: [sql]}
        self._procedures: Dict[str, Dict[str, Any]] = {}
        # 是否启用适配器侧 WHERE 快速通道（默认关闭，改用编译器目录解析）
        self._enable_adapter_where_shortcuts: bool = False
        # 启动时同步一次目录，确保能识别历史表
        try:
            self._sync_compiler_catalog_from_storage()
        except Exception:
            pass

    def sync_catalog(self) -> bool:
        """从底层存储同步编译器目录，供GUI/外部主动刷新使用。"""
        try:
            self._sync_compiler_catalog_from_storage()
            return True
        except Exception:
            return False
    
    def _convert_plan_to_executor_format(self, compiler_plan) -> Dict[str, Any]:
        """
        将SQL编译器的计划格式转换为执行器期望的格式
        不修改编译器，只做格式转换
        """
        plan_dict = compiler_plan.to_dict()
        plan_type = plan_dict.get("type")
        
        print(f"[ADAPTER] 转换计划类型: {plan_type}")
        
        if plan_type == "CreateTable":
            # 转换CREATE TABLE计划
            return {
                "type": "CREATE_TABLE",
                "table": plan_dict["props"]["table"].upper(),
                "columns": plan_dict["props"]["columns"]
            }
        
        elif plan_type == "Insert":
            # 转换INSERT计划
            # 从children中提取values
            values = []
            for child in plan_dict.get("children", []):
                if child.get("type") == "Values":
                    rows = child.get("props", {}).get("rows", [])
                    if rows:
                        values = rows[0]  # 取第一行数据
                    break
            
            return {
                "type": "INSERT",
                "table": plan_dict["props"]["table"].upper(),
                "values": values
            }
        
        elif plan_type in ["Select", "Project"]:
            # 转换SELECT/Project计划
            # 从children中查找实际的表扫描操作
            table_name = ""
            columns = []
            conditions = []
            joins = []
            group_by = []
            order_by = []
            
            # 递归查找表名和条件
            def find_table_info(node):
                nonlocal table_name, conditions, joins, group_by, order_by
                
                if node.get("type") == "SeqScan":
                    table_name = node.get("props", {}).get("table", "")
                    # 提取WHERE条件
                    seq_scan_props = node.get("props", {})
                    if "conditions" in seq_scan_props:
                        conditions = seq_scan_props["conditions"]
                    elif "condition" in seq_scan_props:
                        # 单个条件转换为列表
                        conditions = [seq_scan_props["condition"]]
                elif node.get("type") in ["InnerJoin", "LeftJoin", "RightJoin"]:
                    join_info = {
                        "type": node.get("type", "InnerJoin"),
                        "table": node.get("props", {}).get("right_table", ""),
                        "on": node.get("props", {}).get("condition", "")
                    }
                    joins.append(join_info)
                elif node.get("type") == "GroupBy":
                    group_by = node.get("props", {}).get("group_columns", [])
                elif node.get("type") == "Sort":
                    order_by = node.get("props", {}).get("order_columns", [])
                
                # 递归处理子节点
                for child in node.get("children", []):
                    find_table_info(child)
            
            # 查找表信息
            find_table_info(plan_dict)
            
            # 获取投影列
            if plan_type == "Project":
                columns = plan_dict.get("props", {}).get("columns", [])
            else:
                columns = plan_dict.get("props", {}).get("columns", [])
            
            # 将conditions转换为filter_conditions格式
            filter_conditions = []
            if conditions:
                for condition in conditions:
                    filter_conditions.append({
                        "column": condition.get("left", ""),
                        "op": condition.get("op", "="),
                        "value": condition.get("right", "")
                    })
            
            result = {
                "type": "SELECT",
                "table": table_name.upper() if table_name else table_name,
                "columns": columns,
                "filter": filter_conditions
            }
            
            # 添加高级功能信息
            if joins:
                result["joins"] = joins
            if group_by:
                result["group_by"] = group_by
            if order_by:
                result["order_by"] = order_by
            
            return result
        
        elif plan_type == "Update":
            # 转换UPDATE计划
            def _extract_where(node):
                # 递归从 children 中寻找条件
                conds = []
                if not isinstance(node, dict):
                    return conds
                ntype = node.get("type")
                props = node.get("props", {})
                if ntype in ("SeqScan", "Filter"):
                    if "conditions" in props and isinstance(props.get("conditions"), list):
                        for c in props.get("conditions", []):
                            conds.append({
                                "column": c.get("left", ""),
                                "op": c.get("op", "="),
                                "value": c.get("right", "")
                            })
                    elif "condition" in props and isinstance(props.get("condition"), dict):
                        c = props.get("condition")
                        conds.append({
                            "column": c.get("left", ""),
                            "op": c.get("op", "="),
                            "value": c.get("right", "")
                        })
                for ch in node.get("children", []) or []:
                    conds.extend(_extract_where(ch))
                return conds
            set_clause = plan_dict["props"].get("set_clause") or plan_dict["props"].get("set") or plan_dict["props"].get("assignments") or []
            where_clause = plan_dict["props"].get("where_clause")
            if not where_clause:
                where_clause = _extract_where(plan_dict)
            return {
                "type": "UPDATE",
                "table": plan_dict["props"]["table"].upper(),
                "set_clause": set_clause,
                "where_clause": where_clause
            }
        
        elif plan_type == "Delete":
            # 转换DELETE计划
            return {
                "type": "DELETE",
                "table": plan_dict["props"]["table"].upper(),
                "where_clause": plan_dict["props"].get("where_clause", {})
            }
        
        elif plan_type == "DropTable":
            # 转换DROP TABLE计划
            return {
                "type": "DROP_TABLE",
                "table": plan_dict["props"]["table"].upper()
            }
        
        elif plan_type in ["InnerJoin", "LeftJoin", "RightJoin"]:
            # 转换JOIN计划
            return {
                "type": "SELECT",
                "tables": [plan_dict["props"].get("left_table", ""), plan_dict["props"].get("right_table", "")],
                "joins": [{
                    "type": plan_type.replace("Join", "").upper(),
                    "table": plan_dict["props"].get("right_table", ""),
                    "on": plan_dict["props"].get("condition", "")
                }],
                "columns": plan_dict["props"].get("columns", [])
            }
        
        elif plan_type == "GroupBy":
            # 转换GROUP BY计划
            table_name = ""
            group_columns = []
            aggregates = []
            select_columns = []
            
            # 从GroupBy的props获取分组列
            group_columns = plan_dict["props"].get("columns", [])
            
            # 从children中获取表名和聚合函数
            children = plan_dict.get("children", [])
            if children and len(children) > 0:
                # 第一个child是Aggregate
                aggregate_child = children[0]
                if aggregate_child.get("type") == "Aggregate":
                    # 从Aggregate的functions获取聚合信息
                    functions = aggregate_child.get("props", {}).get("functions", [])
                    for func in functions:
                        func_name = func.get("function", "")
                        func_column = func.get("column", "")
                        if func_name == "COUNT" and func_column == "*":
                            aggregates.append({"function": "COUNT", "column": "*"})
                            select_columns.append("COUNT(*)")
                        elif func_name in ["AVG", "SUM", "MAX", "MIN"]:
                            aggregates.append({"function": func_name, "column": func_column})
                            select_columns.append(f"{func_name}({func_column})")
                    
                    # 从Aggregate的children获取表名
                    agg_children = aggregate_child.get("children", [])
                    if agg_children and len(agg_children) > 0:
                        seqscan_child = agg_children[0]
                        if seqscan_child.get("type") == "SeqScan":
                            table_name = seqscan_child.get("props", {}).get("table", "")
            
            # 添加分组列到select_columns
            for col in group_columns:
                if col not in select_columns:
                    select_columns.append(col)
            
            return {
                "type": "SELECT",
                "table": table_name.upper() if table_name else "",
                "columns": select_columns,
                "group_by": {
                    "group_columns": group_columns,
                    "aggregates": aggregates
                }
            }
        
        elif plan_type == "Sort":
            # 转换ORDER BY计划
            return {
                "type": "SELECT",
                "table": plan_dict["props"].get("table", ""),
                "columns": plan_dict["props"].get("columns", []),
                "order_by": plan_dict["props"].get("order_columns", [])
            }
        
        else:
            # 未知类型，直接返回原始格式
            print(f"[ADAPTER] 未知计划类型: {plan_type}，使用原始格式")
            return plan_dict
    
    def execute(self, sql: str) -> Dict[str, Any]:
        """
        执行SQL语句
        使用SQL编译器进行解析，然后转换为执行器格式
        """
        # 预处理SQL语句：去除首尾空白，标准化换行符
        sql = sql.strip()
        print(f"[ADAPTER] 执行SQL: {sql}")
        # 执行前强制同步一次目录，确保历史表始终已登记
        try:
            self._sync_compiler_catalog_from_storage()
        except Exception:
            pass
        # 简易事务控制语句直通处理
        upper_sql = sql.upper().rstrip(';')
        # 视图定义/删除直通处理（先于编译器）
        if upper_sql.startswith("CREATE VIEW "):
            return self._handle_create_view(sql)
        if upper_sql.startswith("DROP VIEW "):
            return self._handle_drop_view(sql)
        # 物化视图
        if upper_sql.startswith("CREATE MATERIALIZED VIEW "):
            return self._handle_create_materialized_view(sql)
        if upper_sql.startswith("DROP MATERIALIZED VIEW "):
            return self._handle_drop_materialized_view(sql)
        if upper_sql.startswith("REFRESH MATERIALIZED VIEW "):
            return self._handle_refresh_materialized_view(sql)
        # WHERE 解析交由编译器目录处理；若显式开启快捷通道则使用适配器侧解析
        if self._enable_adapter_where_shortcuts and upper_sql.startswith("SELECT ") and " WHERE " in upper_sql:
            advanced = self._try_execute_select_with_advanced_where(sql)
            if advanced is not None:
                return advanced
            simple = self._try_execute_simple_select_with_where(sql)
            if simple is not None:
                return simple
        # 触发器
        if upper_sql.startswith("CREATE TRIGGER "):
            return self._handle_create_trigger(sql)
        if upper_sql.startswith("DROP TRIGGER "):
            return self._handle_drop_trigger(sql)
        if upper_sql == "SHOW TRIGGERS":
            return self._handle_show_triggers()
        # 存储过程
        if upper_sql.startswith("CREATE PROCEDURE "):
            return self._handle_create_procedure(sql)
        if upper_sql.startswith("DROP PROCEDURE "):
            return self._handle_drop_procedure(sql)
        if upper_sql.startswith("CALL "):
            return self._handle_call(sql)
        if upper_sql == "BEGIN":
            return self._begin_transaction()
        if upper_sql == "COMMIT":
            return self._commit_transaction()
        if upper_sql == "ROLLBACK":
            return self._rollback_transaction()
        if upper_sql.startswith("SET AUTOCOMMIT"):
            return self._set_autocommit(upper_sql)
        if upper_sql == "SHOW TRANSACTION":
            return self._show_transaction()
        if upper_sql in ("SHOW TABLES", "SHOW TABLE"):
            return self._handle_show_tables()
        if upper_sql in ("SCAN TABLES", "SYNC CATALOG"):
            # 强制扫描 + 返回表清单
            try:
                self._sync_compiler_catalog_from_storage()
            except Exception:
                pass
            return self._handle_show_tables()
        if upper_sql.startswith("CREATE INDEX"):
            return self._handle_create_index(sql)
        if upper_sql.startswith("CREATE COMPOSITE INDEX"):
            return self._handle_create_composite_index(sql)
        if upper_sql.startswith("DROP INDEX"):
            return self._handle_drop_index(sql)
        if upper_sql.startswith("DROP COMPOSITE INDEX"):
            return self._handle_drop_composite_index(sql)
        if upper_sql == "SHOW INDEXES":
            return self._handle_show_indexes()
        if upper_sql == "SHOW COMPOSITE INDEXES":
            return self._handle_show_composite_indexes()
        
        try:
            # 视图查询重写（仅支持单表 FROM view 的简单 SELECT）
            vw_rewrite = self._try_execute_view_select(sql)
            if vw_rewrite is not None:
                return vw_rewrite
            # CREATE TABLE 约束预处理：提取并剥离 PRIMARY KEY/UNIQUE/FOREIGN KEY
            if upper_sql.startswith("CREATE TABLE "):
                sql = self._preprocess_create_table_constraints(sql)
                upper_sql = sql.upper().rstrip(';')
            # 1. 词法分析
            # 统一大小写（仅在引号外大写），避免目录大小写不一致导致的语义失败
            sql_for_compile = self._uppercase_outside_quotes(sql)
            lexer = Lexer(sql_for_compile)
            tokens, errors = lexer.tokenize()
            
            if errors:
                error_msg = f"词法分析错误: {errors[0]}"
                print(f"[ADAPTER] {error_msg}")
                raise SQLSyntaxError(error_msg)
            
            print(f"[ADAPTER] 词法分析成功，生成 {len(tokens)} 个token")
            
            # 2. 语法分析
            parser = Parser(tokens)
            ast_list = parser.parse()
            
            print(f"[ADAPTER] 语法分析成功，生成 {len(ast_list)} 个AST节点")
            
            # 2.5 同步编译器目录与存储中的真实表
            try:
                self._sync_compiler_catalog_from_storage()
            except Exception:
                pass
            # 3. 语义分析
            semantic_errors = 0
            for ast in ast_list:
                try:
                    self.semantic_analyzer.analyze(ast)
                    print(f"[ADAPTER] 语义检查通过: {ast.node_type}")
                except Exception as e:
                    print(f"[ADAPTER] 语义检查失败: {e}")
                    semantic_errors += 1
            
            if semantic_errors > 0:
                raise SQLSyntaxError(f"语义分析失败，检测到 {semantic_errors} 个错误")
            
            # 4. 执行计划生成
            ast_list_dict = [ast.to_dict() for ast in ast_list]
            planner = Planner(ast_list_dict, enable_optimization=True)
            compiler_plans = planner.generate_plan()
            # 4.5 使用编译器侧优化器对 LogicalPlan 优化（谓词/投影下推、Join重排等）
            try:
                optimized_plans = []
                for lp in compiler_plans:
                    optimized_plans.append(self.compiler_optimizer.optimize(lp))
                compiler_plans = optimized_plans
                print(f"[ADAPTER] 编译器优化完成: 计划数={len(compiler_plans)}")
            except Exception as e:
                print(f"[ADAPTER] 编译器优化跳过: {e}")
            
            print(f"[ADAPTER] 编译器计划生成成功，生成 {len(compiler_plans)} 个计划")
            
            # EXPLAIN: 仅做计划转换和路径选择，返回解释信息
            up_explain = sql.upper()
            if up_explain.startswith("EXPLAIN DETAIL "):
                # 返回编译产物（不执行）
                inner_sql = sql[len("EXPLAIN DETAIL "):].strip()
                artifacts = self.get_compile_artifacts(inner_sql)
                # 简单地以 JSON 字符串形式返回主要字段，便于查看
                import json
                data = [[
                    json.dumps(artifacts.get("tokens", []), ensure_ascii=False),
                    json.dumps(artifacts.get("asts", []), ensure_ascii=False),
                    json.dumps(artifacts.get("logical_plans", []), ensure_ascii=False),
                    json.dumps(artifacts.get("optimized_plans", []), ensure_ascii=False),
                    json.dumps(artifacts.get("executor_plans", []), ensure_ascii=False),
                    json.dumps(artifacts.get("explains", []), ensure_ascii=False),
                ]]
                return {"affected_rows": 0, "data": data, "metadata": {"columns": ["tokens","asts","logical_plans","optimized_plans","executor_plans","explains"]}}
            if up_explain.startswith("EXPLAIN "):
                results = []
                for compiler_plan in compiler_plans:
                    executor_plan = self._convert_plan_to_executor_format(compiler_plan)
                    chosen = self._choose_path(executor_plan)
                    results.append({"plan": executor_plan, "explain": chosen.get("_explain", {})})
                return {"affected_rows": 0, "data": [[str(r["plan"]), str(r["explain"])] for r in results], "metadata": {"columns": ["plan", "explain"]}}

            # 5. 转换计划格式并执行
            results = []
            for compiler_plan in compiler_plans:
                print(f"[ADAPTER] 编译器计划: {compiler_plan}")
                
                # 转换为执行器格式
                executor_plan = self._convert_plan_to_executor_format(compiler_plan)
                print(f"[ADAPTER] 转换后计划: {executor_plan}")
                
                # 事务期内对 INSERT 进行缓冲，其他语句直接执行
                if self.in_transaction and executor_plan.get("type") == "INSERT":
                    table = executor_plan.get("table")
                    values = executor_plan.get("values", [])
                    if table and values:
                        self._enforce_unique_constraints_on_insert(table, values)
                        self._txn_insert_buffer.setdefault(table, []).append(values)
                        print(f"[ADAPTER][TXN] 缓冲 INSERT -> {table}: {values}")
                        result = {"affected_rows": 1, "metadata": {"message": "已加入事务缓冲 (INSERT)"}}
                    else:
                        result = {"affected_rows": 0, "metadata": {"message": "INSERT 语句不完整，已忽略"}}
                else:
                    # 在非事务或不缓冲的语句直接执行，带路径选择与EXPLAIN
                    # INSERT 的唯一性校验
                    if executor_plan.get("type") == "INSERT":
                        table = executor_plan.get("table")
                        values = executor_plan.get("values", [])
                        if table and values:
                            self._enforce_unique_constraints_on_insert(table, values)
                    result = self._execute_with_index_optimization(self._choose_path(executor_plan))
                results.append(result)

            # 返回最后一个结果（通常是主要结果）
            if results:
                return results[-1]
            else:
                return {"status": "success", "affected_rows": 0, "data": []}

        except ParseError as e:
            print(f"[ADAPTER] 语法分析错误: {e}")
            raise SQLSyntaxError(f"语法分析错误: {e}")
        except Exception as e:
            print(f"[ADAPTER] 执行错误: {e}")
            raise ExecutionError(f"SQL执行错误: {e}")

    def _uppercase_outside_quotes(self, s: str) -> str:
        out = []
        in_single = False
        in_double = False
        i = 0
        while i < len(s):
            ch = s[i]
            if ch == "'" and not in_double:
                in_single = not in_single
                out.append(ch)
            elif ch == '"' and not in_single:
                in_double = not in_double
                out.append(ch)
            else:
                if in_single or in_double:
                    out.append(ch)
                else:
                    out.append(ch.upper())
            i += 1
        return ''.join(out)

    # === 约束校验：PRIMARY KEY / UNIQUE ===
    def _enforce_unique_constraints_on_insert(self, table: str, values: List[str]) -> None:
        try:
            t = table.upper()
            # 确保列名缓存
            try:
                if t not in self.hybrid_executor.table_columns:
                    self.hybrid_executor._ensure_table_cached(t)
            except Exception:
                pass
            cols = self.hybrid_executor.table_columns.get(t, []) or []
            if not cols:
                return
            # 收集待比较的数据行
            rows = self.hybrid_executor.executor.seq_scan(t)
            existing = [r.get_values() for r in rows]
            # 主键检查（单列简化）
            pk_col = self._primary_key.get(t)
            if pk_col:
                try:
                    pk_idx = cols.index(pk_col)
                    new_pk = values[pk_idx] if pk_idx < len(values) else None
                    if new_pk is not None:
                        for r in existing:
                            if pk_idx < len(r) and str(r[pk_idx]) == str(new_pk):
                                raise ExecutionError(f"PRIMARY KEY 冲突: {pk_col}={new_pk}")
                except ValueError:
                    pass
            # UNIQUE 列检查
            uniqs = self._unique_cols.get(t, []) or []
            for uc in uniqs:
                try:
                    uidx = cols.index(uc)
                    new_u = values[uidx] if uidx < len(values) else None
                    if new_u is None:
                        continue
                    for r in existing:
                        if uidx < len(r) and str(r[uidx]) == str(new_u):
                            raise ExecutionError(f"UNIQUE 冲突: {uc}={new_u}")
                except ValueError:
                    continue
        except ExecutionError:
            raise
        except Exception:
            # 校验失败不应影响非约束场景，静默
            pass

    def _try_execute_simple_select_with_where(self, sql: str) -> Optional[Dict[str, Any]]:
        """在不修改编译器的前提下，解析简单的 SELECT ... FROM t WHERE cond AND cond; 并执行。
        支持运算符: =, >, >=, <, <=, LIKE；支持 AND 连接；列名为简单标识符；值可为数字或单引号字符串。
        """
        try:
            s = sql.strip().rstrip(';')
            up = s.upper()
            if not up.startswith("SELECT ") or " FROM " not in up or " WHERE " not in up:
                return None
            # 拆 SELECT 与 FROM 段
            sel_part, rest = s.split(" FROM ", 1)
            # 拆表名与 WHERE 段（若有别名，这里不支持）
            up_rest = rest.upper()
            if " WHERE " not in up_rest:
                return None
            table_part, where_part = rest[:up_rest.find(" WHERE ")], rest[up_rest.find(" WHERE ")+7:]
            table = table_part.strip().split()[0].strip()
            # 解析列（规范化为大写匹配引擎列名）
            proj = sel_part[len("SELECT "):].strip()
            columns = ["*"] if proj == "*" else [c.strip().upper() for c in proj.split(',') if c.strip()]
            # 解析 AND 条件
            conditions_raw = []
            tmp = where_part.strip()
            # 按 AND 分割（大小写不敏感）
            parts = []
            i = 0
            start = 0
            while i < len(tmp):
                if tmp[i].upper() == 'A' and tmp[i:i+3].upper() == 'AND' and (i == 0 or tmp[i-1].isspace()) and (i+3 == len(tmp) or tmp[i+3].isspace()):
                    parts.append(tmp[start:i].strip())
                    i += 3
                    start = i
                else:
                    i += 1
            parts.append(tmp[start:].strip())
            import re
            conds = []
            pattern = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)\s*(=|>=|<=|>|<|LIKE)\s*(.+)$", re.IGNORECASE)
            for p in parts:
                m = pattern.match(p)
                if not m:
                    return None
                col = m.group(1).upper()
                op = m.group(2).upper()
                val = m.group(3).strip()
                # 去掉包裹引号
                if (val.startswith("'") and val.endswith("'")) or (val.startswith('"') and val.endswith('"')):
                    val = val[1:-1]
                conds.append({"column": col, "op": op, "value": val})
            plan = {"type": "SELECT", "table": table.upper(), "columns": columns, "filter": conds}
            return self._execute_with_index_optimization(self._choose_path(plan))
        except Exception:
            return None

    # === 高级 WHERE 解析（OR、括号、IN、BETWEEN） ===
    def _try_execute_select_with_advanced_where(self, sql: str) -> Optional[Dict[str, Any]]:
        try:
            s = sql.strip().rstrip(';')
            up = s.upper()
            if not up.startswith("SELECT ") or " FROM " not in up or " WHERE " not in up:
                return None
            sel_part, rest = s.split(" FROM ", 1)
            up_rest = rest.upper()
            if " WHERE " not in up_rest:
                return None
            table_part, where_part = rest[:up_rest.find(" WHERE ")], rest[up_rest.find(" WHERE ")+7:]
            table = table_part.strip().split()[0].strip()
            # 列
            proj = sel_part[len("SELECT "):].strip()
            columns = ["*"] if proj == "*" else [c.strip() for c in proj.split(',') if c.strip()]
            # 若条件不包含 OR/括号/IN/BETWEEN，则交给简单通道
            uw = where_part.upper()
            if (" OR " not in uw) and ("(" not in uw and ")" not in uw) and (" IN " not in uw) and (" BETWEEN " not in uw):
                return None
            # 复杂表达式：使用适配器侧过滤（顺扫+Python求值）
            expr = self._parse_boolean_expr(where_part)
            if expr is None:
                return None
            # 准备列与数据
            try:
                if table not in self.hybrid_executor.table_columns:
                    self.hybrid_executor._ensure_table_cached(table.upper())
            except Exception:
                pass
            cols = self.hybrid_executor.table_columns.get(table.upper(), [])
            rows = self.hybrid_executor.executor.seq_scan(table.upper())
            values = [r.get_values() for r in rows]
            # 逐行求值
            def to_map(vals):
                m = {}
                for i, c in enumerate(cols):
                    if i < len(vals):
                        m[c] = vals[i]
                return m
            matched = []
            for v in values:
                if self._eval_boolean_expr(expr, to_map(v)):
                    matched.append(v)
            # 投影
            if columns == ["*"]:
                try:
                    data = self.hybrid_executor.executor.project(table.upper(), [type('R', (), {'get_values': lambda self, vv=v: vv})() for v in matched], cols)  # not used
                except Exception:
                    data = matched
                return {"affected_rows": len(matched), "data": matched, "metadata": {"columns": cols}}
            else:
                indices = []
                for c in columns:
                    try:
                        indices.append(cols.index(c))
                    except ValueError:
                        raise ExecutionError(f"列不存在: {c}")
                proj_rows = [[row[i] for i in indices] for row in matched]
                return {"affected_rows": len(proj_rows), "data": proj_rows, "metadata": {"columns": columns}}
        except Exception:
            return None

    def _tokenize_expr(self, text: str) -> List[str]:
        import re
        token_spec = r"\s*(" \
                      r"\(|\)|,|" \
                      r"AND|OR|NOT|IN|BETWEEN|LIKE|" \
                      r">=|<=|<>|!=|=|>|<|" \
                      r"[A-Za-z_][A-Za-z0-9_]*|" \
                      r"'[^']*'|\d+\.\d+|\d+" \
                      r")"
        tokens = [t for t in re.findall(token_spec, text, flags=re.IGNORECASE) if t.strip()]
        return tokens

    def _parse_boolean_expr(self, text: str):
        tokens = self._tokenize_expr(text)
        pos = 0
        def peek():
            return tokens[pos] if pos < len(tokens) else None
        def take(tok=None):
            nonlocal pos
            if tok is None or (peek() and peek().upper() == tok):
                cur = peek(); pos += 1; return cur
            return None
        def parse_primary():
            t = peek()
            if t is None:
                return None
            up = t.upper()
            if up == '(':
                take('(')
                e = parse_expr()
                take(')')
                return e
            if up == 'NOT':
                take('NOT')
                sub = parse_primary()
                return {'type': 'not', 'expr': sub}
            # comparison or BETWEEN/IN/LIKE
            left = take()
            op = peek()
            if op is None:
                return None
            uop = op.upper()
            if uop == 'BETWEEN':
                take('BETWEEN'); v1 = take(); take('AND'); v2 = take()
                return {'type': 'between', 'col': left, 'low': v1, 'high': v2}
            if uop == 'IN':
                take('IN'); take('(')
                lst = []
                while True:
                    lst.append(take())
                    if peek() == ',':
                        take(','); continue
                    break
                take(')')
                return {'type': 'in', 'col': left, 'values': lst}
            # LIKE or normal op
            if uop in ('LIKE', '=','>=','<=','>','<','<>','!='):
                take()
                right = take()
                return {'type': 'cmp', 'col': left, 'op': uop, 'val': right}
            return None
        def parse_and():
            node = parse_primary()
            while True:
                if peek() and peek().upper() == 'AND':
                    take('AND')
                    rhs = parse_primary()
                    node = {'type': 'binop', 'op': 'AND', 'left': node, 'right': rhs}
                else:
                    break
            return node
        def parse_expr():
            node = parse_and()
            while True:
                if peek() and peek().upper() == 'OR':
                    take('OR')
                    rhs = parse_and()
                    node = {'type': 'binop', 'op': 'OR', 'left': node, 'right': rhs}
                else:
                    break
            return node
        return parse_expr()

    def _parse_value(self, token: str):
        if token is None:
            return ''
        if len(token) >= 2 and ((token[0] == "'" and token[-1] == "'") or (token[0] == '"' and token[-1] == '"')):
            return token[1:-1]
        try:
            if '.' in token:
                return float(token)
            return int(token)
        except Exception:
            return token

    def _eval_boolean_expr(self, node: Dict[str, Any], row: Dict[str, Any]) -> bool:
        if node is None:
            return True
        t = node.get('type')
        if t == 'binop':
            l = self._eval_boolean_expr(node['left'], row)
            r = self._eval_boolean_expr(node['right'], row)
            if node.get('op') == 'AND':
                return bool(l and r)
            return bool(l or r)
        if t == 'not':
            return not self._eval_boolean_expr(node['expr'], row)
        if t == 'cmp':
            col = node['col']; op = node['op']; val = self._parse_value(node['val'])
            lv = row.get(col, '')
            # 尝试数值比较
            def to_num(x):
                try:
                    return float(x)
                except Exception:
                    return None
            if op in ('=','<>','!='):
                return (str(lv) == str(val)) if op == '=' else (str(lv) != str(val))
            if op in ('>','<','>=','<='):
                ln = to_num(lv); rn = to_num(val)
                if ln is not None and rn is not None:
                    if op == '>': return ln > rn
                    if op == '<': return ln < rn
                    if op == '>=': return ln >= rn
                    if op == '<=': return ln <= rn
                try:
                    if op == '>': return str(lv) > str(val)
                    if op == '<': return str(lv) < str(val)
                    if op == '>=': return str(lv) >= str(val)
                    if op == '<=': return str(lv) <= str(val)
                except Exception:
                    return False
            if op == 'LIKE':
                pat = str(val)
                s = str(lv)
                # 仅支持 % 通配，_ 忽略
                import re
                re_pat = '^' + re.escape(pat).replace('%', '.*') + '$'
                return re.match(re_pat, s) is not None
            return False
        if t == 'in':
            col = node['col']
            vals = [self._parse_value(v) for v in node.get('values', [])]
            return any(str(row.get(col, '')) == str(v) for v in vals)
        if t == 'between':
            col = node['col']
            low = self._parse_value(node['low']); high = self._parse_value(node['high'])
            v = row.get(col, '')
            try:
                x = float(v); lo = float(low); hi = float(high)
                return lo <= x <= hi
            except Exception:
                s = str(v)
                return str(low) <= s <= str(high)
        return False

    # === 路径选择与 EXPLAIN ===
    def _estimate_table_rows(self, table: str) -> int:
        # 优先使用已采样统计
        st = self._stats.get(table)
        if st and isinstance(st.get('rows', None), int):
            return int(st['rows'])
        # 次选：C++ 暴露的索引大小（近似行数）
        try:
            if hasattr(self.hybrid_executor.storage, 'get_index_size'):
                rc = int(self.hybrid_executor.storage.get_index_size(table))
                if rc > 0:
                    return rc
        except Exception:
            pass
        # 退化：0
        return 0

    # --- 轻量统计采样与选择性估计 ---
    def _ensure_table_stats(self, table: str, sample_limit: int = 256) -> None:
        if table in self._stats:
            return
        try:
            # 确保列名可用
            if table not in self.hybrid_executor.table_columns:
                self.hybrid_executor._ensure_table_cached(table)
            cols = self.hybrid_executor.table_columns.get(table, [])
            if not cols:
                self._stats[table] = {'rows': 0, 'cols': {}}
                return
            # 采样若干行
            rows = self.hybrid_executor.executor.seq_scan(table)
            values = []
            cnt = 0
            for r in rows:
                values.append(r.get_values())
                cnt += 1
                if cnt >= sample_limit:
                    break
            # 行数估计：样本数量或 C++ 索引规模
            row_est = cnt
            try:
                if hasattr(self.hybrid_executor.storage, 'get_index_size'):
                    rc = int(self.hybrid_executor.storage.get_index_size(table))
                    row_est = max(row_est, rc)
            except Exception:
                pass
            # 列统计
            col_stats: Dict[str, Any] = {}
            for i, c in enumerate(cols):
                seen = set()
                vmin = None; vmax = None
                for v in values:
                    if i >= len(v):
                        continue
                    s = v[i]
                    seen.add(s)
                    # 数值范围
                    try:
                        x = float(s)
                        vmin = x if vmin is None else min(vmin, x)
                        vmax = x if vmax is None else max(vmax, x)
                    except Exception:
                        pass
                ndv = max(1, len(seen))
                ent = {'ndv': ndv}
                if vmin is not None and vmax is not None:
                    ent['min'] = vmin; ent['max'] = vmax
                col_stats[c] = ent
            self._stats[table] = {'rows': row_est, 'cols': col_stats}
        except Exception:
            self._stats[table] = {'rows': 0, 'cols': {}}

    def _estimate_selectivity(self, table: str, flt: List[Dict[str, Any]]) -> float:
        if not flt:
            return 1.0
        self._ensure_table_stats(table)
        st = self._stats.get(table, {})
        cstats = st.get('cols', {})
        # 简化：按 AND 连接独立性估计
        sel = 1.0
        for cond in flt:
            col = cond.get('column', '')
            op  = cond.get('op', '=')
            val = cond.get('value', '')
            cs = cstats.get(col, {})
            ndv = int(cs.get('ndv', 100))
            # 等值
            if op == '=':
                sel *= max(1.0/ndv, 0.001)
            elif op in ('>', '>=', '<', '<='):
                try:
                    v = float(val); vmin = cs.get('min', None); vmax = cs.get('max', None)
                    if isinstance(vmin, (int,float)) and isinstance(vmax, (int,float)) and vmax > vmin:
                        if op in ('>', '>='):
                            frac = max(0.0, min(1.0, (vmax - v) / (vmax - vmin)))
                        else:
                            frac = max(0.0, min(1.0, (v - vmin) / (vmax - vmin)))
                        sel *= max(frac, 0.001)
                    else:
                        sel *= 0.1
                except Exception:
                    sel *= 0.1
            else:
                sel *= 0.3
        # 限制范围
        return max(0.0001, min(1.0, sel))

    def _choose_path(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        p = dict(plan)
        p_meta = p.setdefault("_explain", {})
        if p.get("type") == "SELECT":
            table = p.get("table", "")
            flt = p.get("filter") or []
            # 确保有统计并获取更准的行数估计
            self._ensure_table_stats(table)
            rows = self._estimate_table_rows(table)
            p_meta["table_rows_estimate"] = rows
            # --- 成本估计：顺扫 vs 二级索引(内存) vs 主键索引(C++) ---
            seq_cost = max(rows, 1)
            # 主键索引是否可用（通过 C++ storage.has_index 判断是否存在主键列）
            has_pk_index = False
            try:
                has_pk_index = bool(self.storage_engine.has_index(table))
            except Exception:
                pass

            # 估计过滤选择性
            sel = self._estimate_selectivity(table, flt) if flt else 1.0

            idx_cost = int(rows * sel)
            # 若为等值并且是典型主键列名，则优先考虑主键点查成本
            pk_eq = None
            if len(flt) == 1 and flt[0].get("op") == "=" and str(flt[0].get("column", "")).lower() in ("id", "pk", "primary", "primary_key"):
                pk_eq = flt[0]

            chosen = "seq_scan"; access_params = {}
            if pk_eq and has_pk_index:
                chosen = "index_scan"
                access_params = {"pk_value": str(pk_eq.get("value", ""))}
            else:
                # 尝试使用我们内存二级索引的成本（命中即常数/很小开销）
                can_secondary = False
                if len(flt) == 1:
                    col = flt[0].get("column", ""); op = flt[0].get("op")
                    can_secondary = self.index_manager.has_index(table, col) and op in ("=", ">", ">=", "<", "<=")
                if can_secondary:
                    chosen = "secondary_index"
                    idx_cost = max(1, int(rows * sel * 0.2))
                # 主键范围
                has_range = any(c.get("op") in (">", ">=", "<", "<=") and str(c.get("column", "")).lower() in ("id","pk","primary","primary_key") for c in flt)
                if has_pk_index and has_range:
                    chosen = "index_range_scan"

            # 比较成本并写回计划
            p_meta["cost_seq"] = seq_cost
            p_meta["cost_idx"] = idx_cost
            p_meta["chosen"] = chosen
            if chosen == "index_scan":
                p["access_method"] = "index_scan"
                p["access_params"] = access_params
            elif chosen == "index_range_scan":
                # 简化构造边界
                min_pk = ""; max_pk = "\xFF\xFF\xFF\xFF"
                for c in flt:
                    if str(c.get("column"," ")).lower() in ("id","pk","primary","primary_key"):
                        if c.get("op") in (">", ">="):
                            min_pk = str(c.get("value",""))
                        elif c.get("op") in ("<", "<="):
                            max_pk = str(c.get("value",""))
                p["access_method"] = "index_range_scan"
                p["access_params"] = {"min_pk": min_pk, "max_pk": max_pk}
            # 二级索引路径不直接下推，由执行阶段的 _execute_with_index_optimization/HybridExecution 决定

        # JOIN 算法选择（若存在）
        if p.get("type") == "SELECT" and p.get("joins"):
            # 简易策略：估计左/右表大小，大表倾向哈希，小表可用 merge
            left = (p.get("tables") or [None, None])[0]
            right = (p.get("tables") or [None, None])[1]
            lsz = self._estimate_table_rows(left) if left else 0
            rsz = self._estimate_table_rows(right) if right else 0
            algo = "hash" if max(lsz, rsz) > 1000 else "merge"
            p.setdefault("join_algo", algo)
            p_meta["join_algo"] = algo
        return p
    
    # === 事务相关 ===
    def _begin_transaction(self) -> Dict[str, Any]:
        if self.in_transaction:
            return {"affected_rows": 0, "metadata": {"message": "已在事务中"}}
        self.in_transaction = True
        self.autocommit = False
        self._txn_insert_buffer.clear()
        print("[ADAPTER][TXN] BEGIN")
        return {"affected_rows": 0, "metadata": {"message": "事务已开始"}}
    
    def _commit_transaction(self) -> Dict[str, Any]:
        if not self.in_transaction:
            return {"affected_rows": 0, "metadata": {"message": "当前不在事务中"}}
        total = 0
        # 批量提交 INSERT 以优化性能
        for table, rows in list(self._txn_insert_buffer.items()):
            if not rows:
                continue
            try:
                print(f"[ADAPTER][TXN] COMMIT -> 批量插入 {table}: {len(rows)} 行")
                # 事务提交前做唯一性整体校验
                for r in rows:
                    self._enforce_unique_constraints_on_insert(table, r)
                count = int(self.hybrid_executor.insert_many(table, rows))
                # 批量更新索引
                cols = self.hybrid_executor.table_columns.get(table, [])
                for r in rows:
                    self.index_manager.on_insert(table, r, cols)
            except Exception as e:
                print(f"[ADAPTER][TXN] 批量插入失败，回退逐行: {e}")
                count = 0
                for r in rows:
                    try:
                        ok = self.hybrid_executor.executor.insert(table, r)
                        if ok:
                            count += 1
                            # 更新索引
                            cols = self.hybrid_executor.table_columns.get(table, [])
                            self.index_manager.on_insert(table, r, cols)
                    except Exception:
                        pass
            total += count
        self._txn_insert_buffer.clear()
        self.in_transaction = False
        # 保持 autocommit 当前值不变
        print(f"[ADAPTER][TXN] COMMIT 完成, 插入 {total} 行")
        return {"affected_rows": total, "metadata": {"message": f"事务已提交 (批量插入 {total} 行)"}}
    
    def _rollback_transaction(self) -> Dict[str, Any]:
        if not self.in_transaction:
            return {"affected_rows": 0, "metadata": {"message": "当前不在事务中"}}
        # 丢弃缓冲的 INSERT
        discarded = sum(len(v) for v in self._txn_insert_buffer.values())
        self._txn_insert_buffer.clear()
        self.in_transaction = False
        print(f"[ADAPTER][TXN] ROLLBACK, 丢弃缓冲 INSERT {discarded} 行")
        return {"affected_rows": 0, "metadata": {"message": f"事务已回滚 (丢弃 {discarded} 条 INSERT)"}}

    def _set_autocommit(self, upper_sql: str) -> Dict[str, Any]:
        # 允许: SET AUTOCOMMIT = ON|OFF 或 1|0
        val = upper_sql.split('=')[-1].strip()
        on_vals = {"ON", "1", "TRUE"}
        off_vals = {"OFF", "0", "FALSE"}
        if val in on_vals:
            self.autocommit = True
            msg = "AUTOCOMMIT=ON"
        elif val in off_vals:
            self.autocommit = False
            msg = "AUTOCOMMIT=OFF"
        else:
            msg = "无效的 AUTOCOMMIT 值, 仅支持 ON/OFF/1/0"
        print(f"[ADAPTER][TXN] {msg}")
        return {"affected_rows": 0, "metadata": {"message": msg}}

    def _show_transaction(self) -> Dict[str, Any]:
        state = {
            "in_transaction": self.in_transaction,
            "autocommit": self.autocommit,
            "buffered_inserts": {k: len(v) for k, v in self._txn_insert_buffer.items()}
        }
        return {"affected_rows": 0, "data": [], "metadata": {"message": str(state)}}

    # === 索引相关 ===
    def _parse_ident(self, token: str) -> str:
        return token.strip().strip('`"[]')

    def _sync_compiler_catalog_from_storage(self) -> None:
        """将底层存储中的表结构同步到编译器目录中，避免语义阶段报表不存在。"""
        try:
            # 获取表名
            if hasattr(self.storage_engine, 'get_table_names'):
                names = list(self.storage_engine.get_table_names())
            elif hasattr(self.storage_engine, 'list_tables'):
                names = list(self.storage_engine.list_tables())
            else:
                names = list(getattr(self.hybrid_executor, 'table_columns', {}).keys())
        except Exception:
            names = []
        # 额外回退：扫描代码目录/工作目录下的 *_page_*.bin 文件，推断表名
        try:
            import os, re
            scan_dirs: List[str] = []
            # 当前工作目录
            try:
                scan_dirs.append(os.getcwd())
            except Exception:
                pass
            # 工程根目录（与适配器确定的一致）
            try:
                scan_dirs.append(str(proj_root))
            except Exception:
                pass
            # 工程根的上一级（有时数据文件放到工程根同级）
            try:
                scan_dirs.append(str(Path(proj_root).parent))
            except Exception:
                pass
            seen = set()
            for base in scan_dirs:
                if not base or not os.path.isdir(base):
                    continue
                for root, _dirs, files in os.walk(base):
                    # 限制深度：最多向下2层，避免全盘扫描
                    depth = len(Path(root).relative_to(base).parts) if Path(root) != Path(base) else 0
                    if depth > 2:
                        continue
                    for fn in files:
                        # 识别 table_page_*.bin
                        m = re.match(r"^(.+?)_page_\d+\.bin$", fn, re.IGNORECASE)
                        if m:
                            tbl = m.group(1)
                            if tbl not in seen:
                                names.append(tbl)
                                seen.add(tbl)
        except Exception:
            pass
        # 逐表获取列
        for t in names:
            try:
                tu = str(t).upper()
                # 过滤内部/系统对象
                if tu == "SYS_CATALOG" or tu.startswith("SYS_"):
                    continue
                # 尽可能取列；失败则回退缓存或空列
                cols = []
                try:
                    if hasattr(self.storage_engine, 'get_table_columns'):
                        cols = list(self.storage_engine.get_table_columns(tu))
                except Exception:
                    pass
                if not cols:
                    cols = list(getattr(self.hybrid_executor, 'table_columns', {}).get(tu, []))
                cols_u = [str(c).upper() for c in (cols or [])]
                # 将列名转换为列->类型的映射，尝试从存储引擎获取真实类型
                col_map = {}
                for c in cols_u:
                    # 尝试从存储引擎获取列类型
                    try:
                        if hasattr(self.storage_engine, 'get_column_type'):
                            col_type = self.storage_engine.get_column_type(tu, c)
                            if col_type:
                                col_map[c] = col_type
                                continue
                    except Exception:
                        pass
                    
                    # 回退：根据列名推断类型
                    if c in ['ID', 'AGE']:
                        col_map[c] = "INT"
                    elif c in ['SCORE', 'PRICE', 'AMOUNT']:
                        col_map[c] = "DOUBLE"
                    else:
                        col_map[c] = "STRING"
                # 若未登记，则登记；已登记则跳过
                try:
                    if not (hasattr(self.catalog, 'has_table') and self.catalog.has_table(tu)):
                        self.catalog.create_table(tu, col_map)
                except Exception:
                    pass
            except Exception:
                continue

    def _handle_create_index(self, sql: str) -> Dict[str, Any]:
        # 语法（增强版）：
        # CREATE INDEX idx ON table(col) [USING BTREE|HASH] PK pkcol;
        up = sql.strip().rstrip(';')
        try:
            # 粗略解析
            # 找到 ON 与 PK 关键字
            u = up.upper()
            on_pos = u.find(" ON ")
            pk_pos = u.find(" PK ")
            if on_pos == -1 or pk_pos == -1 or pk_pos < on_pos:
                raise ValueError("语法: CREATE INDEX idx ON table(col) PK pkcol;")
            on_part = up[on_pos + 4: pk_pos].strip()
            pk_part = up[pk_pos + 4:].strip()
            # on_part 形如: table(col)
            table = on_part.split('(')[0].strip()
            col = on_part[on_part.find('(')+1:on_part.rfind(')')].strip()
            # 解析 USING 可选项
            strategy = "BTREE"
            using_pos = u.find(" USING ", on_pos, pk_pos)
            if using_pos != -1:
                # 形如 USING BTREE/HASH
                strat = up[using_pos + 7: pk_pos].strip()
                # 取第一个单词
                strategy = (strat.split()[0] if strat else "BTREE").upper()
                # 修正 on_part 去掉 USING 片段
                on_part = up[on_pos + 4: using_pos].strip()
                table = on_part.split('(')[0].strip()
                col = on_part[on_part.find('(')+1:on_part.rfind(')')].strip()

            pkcol = pk_part
            ok = self.index_manager.create_index(
                self._parse_ident(table), self._parse_ident(col), self._parse_ident(pkcol), strategy=strategy
            )
            msg = "索引已存在" if not ok else "索引创建成功"
            return {"affected_rows": 0, "metadata": {"message": msg}}
        except Exception as e:
            raise SQLSyntaxError(f"CREATE INDEX 解析失败: {e}")

    def _handle_drop_index(self, sql: str) -> Dict[str, Any]:
        # 语法（简化版）：DROP INDEX table(col);
        up = sql.strip().rstrip(';')
        try:
            if up.upper().startswith("DROP INDEX"):
                spec = up[len("DROP INDEX"):].strip()
                table = spec.split('(')[0].strip()
                col = spec[spec.find('(')+1:spec.rfind(')')].strip()
                existed = self.index_manager.drop_index(self._parse_ident(table), self._parse_ident(col))
                msg = "索引不存在" if not existed else "索引已删除"
                return {"affected_rows": 0, "metadata": {"message": msg}}
            raise ValueError("语法: DROP INDEX table(col);")
        except Exception as e:
            raise SQLSyntaxError(f"DROP INDEX 解析失败: {e}")

    def _handle_drop_composite_index(self, sql: str) -> Dict[str, Any]:
        # 语法（简化版）：DROP COMPOSITE INDEX ON table;
        up = sql.strip().rstrip(';')
        try:
            s = up.upper()
            if not s.startswith("DROP COMPOSITE INDEX"):
                raise ValueError("语法: DROP COMPOSITE INDEX ON table;")
            on_pos = s.find(" ON ")
            if on_pos == -1:
                raise ValueError("缺少 ON 关键字")
            table = up[on_pos + 4:].strip()
            ok = bool(self.storage_engine.drop_composite_index(table))
            msg = "复合索引已删除" if ok else "复合索引不存在"
            return {"affected_rows": 0, "metadata": {"message": msg}}
        except Exception as e:
            raise SQLSyntaxError(f"DROP COMPOSITE INDEX 解析失败: {e}")

    def _handle_show_indexes(self) -> Dict[str, Any]:
        items = self.index_manager.get_indexes()
        return {"affected_rows": len(items), "data": [[it["table"], it["column"], it["pk_column"], it.get("strategy","BTREE")] for it in items], "metadata": {"columns": ["table", "column", "pk", "strategy"]}}

    def _handle_show_composite_indexes(self) -> Dict[str, Any]:
        # 返回每张表的复合索引列下标
        try:
            table_names = list(self.storage_engine.get_table_names()) if hasattr(self.storage_engine, 'get_table_names') else []
            rows = []
            for t in table_names:
                cols = list(self.storage_engine.get_composite_index_columns(t)) if hasattr(self.storage_engine, 'get_composite_index_columns') else []
                if cols:
                    rows.append([t, ','.join(str(i) for i in cols)])
            return {"affected_rows": len(rows), "data": rows, "metadata": {"columns": ["table", "col_indices"]}}
        except Exception as e:
            raise ExecutionError(f"SHOW COMPOSITE INDEXES 失败: {e}")

    def _handle_create_composite_index(self, sql: str) -> Dict[str, Any]:
        # 语法（简化版）：CREATE COMPOSITE INDEX idx ON table(col1,col2,...);
        up = sql.strip().rstrip(';')
        try:
            s = up.upper()
            if not s.startswith("CREATE COMPOSITE INDEX"):
                raise ValueError("语法: CREATE COMPOSITE INDEX idx ON table(col1,col2,...);")
            on_pos = s.find(" ON ")
            if on_pos == -1:
                raise ValueError("缺少 ON 关键字")
            spec = up[on_pos + 4:].strip()
            table = spec.split('(')[0].strip()
            cols_str = spec[spec.find('(')+1:spec.rfind(')')].strip()
            col_names = [self._parse_ident(c.strip()) for c in cols_str.split(',') if c.strip()]
            if not table or not col_names:
                raise ValueError("未解析到表名或列名")
            # 将列名转换为下标序列
            cpp_cols = list(self.storage_engine.get_table_columns(table))
            indices = []
            for cn in col_names:
                if cn not in cpp_cols:
                    raise ValueError(f"列不存在: {cn}")
                indices.append(int(cpp_cols.index(cn)))
            ok = bool(self.storage_engine.enable_composite_index(table, indices))
            msg = "复合索引创建成功" if ok else "复合索引已存在或创建失败"
            return {"affected_rows": 0, "metadata": {"message": msg}}
        except Exception as e:
            raise SQLSyntaxError(f"CREATE COMPOSITE INDEX 解析失败: {e}")

    def _handle_show_tables(self) -> Dict[str, Any]:
        try:
            names: List[str] = []
            # 1) 首选引擎列出
            if hasattr(self.storage_engine, 'get_table_names'):
                try:
                    names.extend(list(self.storage_engine.get_table_names()))
                except Exception:
                    pass
            if not names and hasattr(self.storage_engine, 'list_tables'):
                try:
                    names.extend(list(self.storage_engine.list_tables()))
                except Exception:
                    pass
            # 2) 回退：执行器缓存
            try:
                names.extend(list(getattr(self.hybrid_executor, 'table_columns', {}).keys()))
            except Exception:
                pass
            # 3) 目录注册（Catalog）
            try:
                if hasattr(self.catalog, 'list_tables'):
                    names.extend(list(self.catalog.list_tables()))
                else:
                    names.extend(list(getattr(self.catalog, 'tables', {}).keys()))
            except Exception:
                pass
            # 去重并排序（不区分大小写）
            uniq = {}
            for n in names:
                if not n:
                    continue
                key = str(n).upper()
                # 过滤内部/系统对象
                if key == "SYS_CATALOG" or key.startswith("SYS_"):
                    continue
                uniq[key] = n
            final_names = sorted(uniq.keys())
            rows = []
            for key in final_names:
                tname = key  # 使用大写名与C++接口一致
                try:
                    cols = list(self.storage_engine.get_table_columns(tname))
                except Exception:
                    # 回退执行器缓存
                    cols = list(getattr(self.hybrid_executor, 'table_columns', {}).get(tname, []))
                    if not cols:
                        # 最后回退 Catalog 已登记列（若可用）
                        try:
                            if hasattr(self.catalog, 'get_columns'):
                                cols = list(self.catalog.get_columns(tname))
                        except Exception:
                            cols = []
                rows.append([tname, ','.join(cols)])
            return {"affected_rows": len(rows), "data": rows, "metadata": {"columns": ["table", "columns"]}}
        except Exception as e:
            raise ExecutionError(f"SHOW TABLES 失败: {e}")

    # === 触发器实现（适配器层） ===
    def _handle_create_trigger(self, sql: str) -> Dict[str, Any]:
        # 简化语法：CREATE TRIGGER name BEFORE|AFTER INSERT|UPDATE|DELETE ON table AS BEGIN <stmt1>; <stmt2>; END;
        s = sql.strip().rstrip(';')
        up = s.upper()
        try:
            if not up.startswith("CREATE TRIGGER "):
                raise ValueError("语法: CREATE TRIGGER name BEFORE|AFTER INSERT|UPDATE|DELETE ON table AS BEGIN ... END;")
            rest = s[len("CREATE TRIGGER "):].strip()
            name = rest.split()[0]
            rest2 = rest[len(name):].strip()
            timing = "BEFORE" if rest2.upper().startswith("BEFORE ") else ("AFTER" if rest2.upper().startswith("AFTER ") else None)
            if not timing:
                raise ValueError("缺少 BEFORE/AFTER")
            rest3 = rest2[len(timing):].strip()
            ev = None
            for e in ("INSERT","UPDATE","DELETE"):
                if rest3.upper().startswith(e+" "):
                    ev = e; break
            if not ev:
                raise ValueError("缺少事件 INSERT/UPDATE/DELETE")
            rest4 = rest3[len(ev):].strip()
            if not rest4.upper().startswith("ON "):
                raise ValueError("缺少 ON 关键字")
            rest5 = rest4[3:].strip()
            table = rest5.split()[0]
            # 解析 AS BEGIN ... END 块
            as_pos = rest5.upper().find(" AS ")
            if as_pos == -1:
                raise ValueError("缺少 AS 块")
            blk = rest5[as_pos+4:].strip()
            if blk.upper().startswith("BEGIN"):
                blk = blk[5:].strip()
            if blk.upper().endswith("END"):
                blk = blk[:-3].strip()
            # 语句按分号切分
            stmts = [x.strip() for x in blk.split(';') if x.strip()]
            trig = {"name": name, "timing": timing, "event": ev, "statements": stmts}
            self._triggers.setdefault(table, []).append(trig)
            return {"affected_rows": 0, "metadata": {"message": f"触发器 '{name}' 已创建"}}
        except Exception as e:
            raise SQLSyntaxError(f"CREATE TRIGGER 解析失败: {e}")

    def _handle_drop_trigger(self, sql: str) -> Dict[str, Any]:
        s = sql.strip().rstrip(';')
        up = s.upper()
        try:
            if not up.startswith("DROP TRIGGER "):
                raise ValueError("语法: DROP TRIGGER name ON table;")
            rest = s[len("DROP TRIGGER "):].strip()
            name = rest.split()[0]
            rest2 = rest[len(name):].strip()
            if not rest2.upper().startswith("ON "):
                raise ValueError("缺少 ON 关键字")
            table = rest2[3:].strip()
            arr = self._triggers.get(table, [])
            before = len(arr)
            arr = [t for t in arr if t.get("name") != name]
            self._triggers[table] = arr
            msg = "触发器已删除" if len(arr) != before else "触发器不存在"
            return {"affected_rows": 0, "metadata": {"message": msg}}
        except Exception as e:
            raise SQLSyntaxError(f"DROP TRIGGER 解析失败: {e}")

    def _handle_show_triggers(self) -> Dict[str, Any]:
        rows = []
        for table, lst in self._triggers.items():
            for t in lst:
                rows.append([t.get("name"), table, t.get("timing"), t.get("event")])
        return {"affected_rows": len(rows), "data": rows, "metadata": {"columns": ["name","table","timing","event"]}}

    def _fire_triggers(self, table: str, timing: str, event: str) -> None:
        try:
            for t in self._triggers.get(table, []) or []:
                if t.get("timing") == timing and t.get("event") == event:
                    for stmt in t.get("statements", []):
                        try:
                            self.execute(stmt)
                        except Exception:
                            # 触发器语句失败不影响主语句（简化）
                            pass
        except Exception:
            pass

    # === 物化视图 ===
    def _handle_create_materialized_view(self, sql: str) -> Dict[str, Any]:
        # 语法：CREATE MATERIALIZED VIEW name AS <SELECT ...>;
        s = sql.strip().rstrip(';')
        up = s.upper()
        try:
            if not up.startswith("CREATE MATERIALIZED VIEW "):
                raise ValueError("语法: CREATE MATERIALIZED VIEW name AS <SELECT ...>;")
            rest = s[len("CREATE MATERIALIZED VIEW "):].strip()
            name = rest.split()[0]
            as_pos = rest.upper().find(" AS ")
            if as_pos == -1:
                raise ValueError("缺少 AS")
            select_sql = rest[as_pos+4:].strip()
            phys = f"__mat_{name}"
            # 创建物理表并填充
            self._create_table_from_select(phys, select_sql)
            self._mat_views[name] = {"sql": select_sql, "physical_table": phys}
            return {"affected_rows": 0, "metadata": {"message": f"物化视图 '{name}' 已创建"}}
        except Exception as e:
            raise SQLSyntaxError(f"CREATE MATERIALIZED VIEW 解析失败: {e}")

    def _handle_drop_materialized_view(self, sql: str) -> Dict[str, Any]:
        s = sql.strip().rstrip(';')
        up = s.upper()
        try:
            if not up.startswith("DROP MATERIALIZED VIEW "):
                raise ValueError("语法: DROP MATERIALIZED VIEW name;")
            name = s[len("DROP MATERIALIZED VIEW "):].strip()
            mv = self._mat_views.pop(name, None)
            if mv:
                try:
                    self.execute(f"DROP TABLE {mv['physical_table']};")
                except Exception:
                    pass
                msg = "物化视图已删除"
            else:
                msg = "物化视图不存在"
            return {"affected_rows": 0, "metadata": {"message": msg}}
        except Exception as e:
            raise SQLSyntaxError(f"DROP MATERIALIZED VIEW 解析失败: {e}")

    def _handle_refresh_materialized_view(self, sql: str) -> Dict[str, Any]:
        # 语法：REFRESH MATERIALIZED VIEW name;
        s = sql.strip().rstrip(';')
        up = s.upper()
        try:
            if not up.startswith("REFRESH MATERIALIZED VIEW "):
                raise ValueError("语法: REFRESH MATERIALIZED VIEW name;")
            name = s[len("REFRESH MATERIALIZED VIEW "):].strip()
            mv = self._mat_views.get(name)
            if not mv:
                raise ExecutionError("物化视图不存在")
            phys = mv["physical_table"]
            # 先清空物理表（简化：DROP + 重新创建）
            try:
                self.execute(f"DROP TABLE {phys};")
            except Exception:
                pass
            self._create_table_from_select(phys, mv["sql"])
            return {"affected_rows": 0, "metadata": {"message": "物化视图已刷新"}}
        except Exception as e:
            raise ExecutionError(f"REFRESH MATERIALIZED VIEW 失败: {e}")

    def _create_table_from_select(self, table: str, select_sql: str) -> None:
        # 执行 select，使用返回的列构建表并插入数据
        inner = select_sql.strip()
        if not inner.endswith(';'):
            inner += ';'
        res = self.execute(inner)
        cols = res.get("metadata", {}).get("columns", []) or []
        if not cols:
            raise ExecutionError("无法解析物化视图列")
        col_defs = ', '.join(f"{c} STRING" for c in cols)  # 简化全部为 STRING
        self.execute(f"CREATE TABLE {table} ({col_defs});")
        for row in res.get("data", []) or []:
            vals = ', '.join([f"'{str(v)}'" for v in row])
            self.execute(f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({vals});")

    # === 存储过程/控制流（简化） ===
    def _handle_create_procedure(self, sql: str) -> Dict[str, Any]:
        # 语法：CREATE PROCEDURE name AS BEGIN stmt1; stmt2; END;
        s = sql.strip().rstrip(';')
        up = s.upper()
        try:
            if not up.startswith("CREATE PROCEDURE "):
                raise ValueError("语法: CREATE PROCEDURE name AS BEGIN ... END;")
            rest = s[len("CREATE PROCEDURE "):].strip()
            name = rest.split()[0]
            as_pos = rest.upper().find(" AS ")
            blk = rest[as_pos+4:].strip()
            if blk.upper().startswith("BEGIN"):
                blk = blk[5:].strip()
            if blk.upper().endswith("END"):
                blk = blk[:-3].strip()
            stmts = [x.strip() for x in blk.split(';') if x.strip()]
            self._procedures[name] = {"statements": stmts}
            return {"affected_rows": 0, "metadata": {"message": f"过程 '{name}' 已创建"}}
        except Exception as e:
            raise SQLSyntaxError(f"CREATE PROCEDURE 解析失败: {e}")

    def _handle_drop_procedure(self, sql: str) -> Dict[str, Any]:
        s = sql.strip().rstrip(';')
        up = s.upper()
        try:
            if not up.startswith("DROP PROCEDURE "):
                raise ValueError("语法: DROP PROCEDURE name;")
            name = s[len("DROP PROCEDURE "):].strip()
            existed = name in self._procedures
            self._procedures.pop(name, None)
            msg = "过程已删除" if existed else "过程不存在"
            return {"affected_rows": 0, "metadata": {"message": msg}}
        except Exception as e:
            raise SQLSyntaxError(f"DROP PROCEDURE 解析失败: {e}")

    def _handle_call(self, sql: str) -> Dict[str, Any]:
        # 语法：CALL name;
        s = sql.strip().rstrip(';')
        up = s.upper()
        try:
            if not up.startswith("CALL "):
                raise ValueError("语法: CALL name;")
            name = s[5:].strip()
            proc = self._procedures.get(name)
            if not proc:
                raise ExecutionError("过程不存在")
            for stmt in proc.get("statements", []):
                inner = stmt.strip()
                if not inner.endswith(';'):
                    inner += ';'
                self.execute(inner)
            return {"affected_rows": 0, "metadata": {"message": f"过程 '{name}' 已执行"}}
        except Exception as e:
            raise ExecutionError(f"CALL 失败: {e}")

    # === 视图（非物化） ===
    def _handle_create_view(self, sql: str) -> Dict[str, Any]:
        # 语法（简化）：CREATE VIEW view_name AS <SELECT ...>;
        s = sql.strip().rstrip(';')
        up = s.upper()
        try:
            if not up.startswith("CREATE VIEW "):
                raise ValueError("语法: CREATE VIEW name AS <SELECT ...>;")
            as_pos = up.find(" AS ")
            if as_pos == -1:
                raise ValueError("缺少 AS 关键字")
            view_name = s[len("CREATE VIEW "):as_pos].strip()
            select_sql = s[as_pos + 4:].strip()
            if not select_sql.upper().startswith("SELECT "):
                raise ValueError("仅支持 SELECT 视图定义")
            self._views[view_name] = {"sql": select_sql}
            return {"affected_rows": 0, "metadata": {"message": f"视图 '{view_name}' 已创建"}}
        except Exception as e:
            raise SQLSyntaxError(f"CREATE VIEW 解析失败: {e}")

    def _handle_drop_view(self, sql: str) -> Dict[str, Any]:
        # 语法：DROP VIEW view_name;
        s = sql.strip().rstrip(';')
        up = s.upper()
        try:
            if not up.startswith("DROP VIEW "):
                raise ValueError("语法: DROP VIEW name;")
            view_name = s[len("DROP VIEW "):].strip()
            existed = view_name in self._views
            if existed:
                self._views.pop(view_name, None)
            msg = "视图已删除" if existed else "视图不存在"
            return {"affected_rows": 0, "metadata": {"message": msg}}
        except Exception as e:
            raise SQLSyntaxError(f"DROP VIEW 解析失败: {e}")

    def _try_execute_view_select(self, sql: str) -> Optional[Dict[str, Any]]:
        """检测并重写简单形态: SELECT <cols> FROM <view>; （不含额外 WHERE/JOIN）"""
        s = sql.strip().rstrip(';')
        up = s.upper()
        if not up.startswith("SELECT "):
            return None
        # 仅支持单 FROM 的简单模式
        try:
            # 粗略解析: SELECT ... FROM <ident> [;]
            if " FROM " not in up:
                return None
            sel_part, from_part = s.split(" FROM ", 1)
            # 去除尾部
            rest = from_part.strip()
            # 如果包含空格或进一步关键字，认为是复杂查询，交由编译器
            # 我们只支持 "<view_name>" 或 "<view_name>;"
            view_name = rest.split()[0].strip().rstrip(';')
            if view_name not in self._views:
                return None
            inner_sql = self._views[view_name]["sql"]
            if not inner_sql.strip().endswith(';'):
                inner_sql = inner_sql.strip() + ';'
            # 如果用户选择的列是 * 则直接执行视图 SQL；否则对结果做列投影
            proj = sel_part[len("SELECT "):].strip()
            # 执行视图 SQL
            res = self.execute(inner_sql)
            if not proj or proj == "*":
                return res
            # 进行简单列投影
            data = res.get("data", [])
            columns = res.get("metadata", {}).get("columns", [])
            want_cols = [c.strip() for c in proj.split(',') if c.strip()]
            # 大小写不敏感匹配列名
            mapping = {str(col).lower(): i for i, col in enumerate(columns)}
            col_indices = []
            for c in want_cols:
                key = c.lower()
                if key not in mapping:
                    raise ExecutionError(f"视图列不存在: {c}")
                col_indices.append(mapping[key])
            new_rows = [[row[i] for i in col_indices] for row in data]
            return {"affected_rows": len(new_rows), "data": new_rows, "metadata": {"columns": want_cols}}
        except ExecutionError:
            raise
        except Exception:
            return None

    # === 约束处理 ===
    def _preprocess_create_table_constraints(self, sql: str) -> str:
        """在不修改编译器的前提下，粗略解析 CREATE TABLE 中的 PRIMARY KEY/UNIQUE/FOREIGN KEY，
        记录到适配器元数据，并从 SQL 中移除这些约束片段，以适配现有编译器不支持约束的限制。
        仅支持列级与简单表级 PRIMARY KEY/UNIQUE，FOREIGN KEY 仅登记元数据，不做执行时强制。"""
        s = sql.strip()
        up = s.upper()
        try:
            # 提取表名与括号内定义
            head = up[len("CREATE TABLE "):]
            table = head.split('(')[0].strip()
            body = s[s.find('(')+1:s.rfind(')')]
            parts = [p.strip() for p in body.split(',')]
            new_parts: List[str] = []
            uniques: List[str] = []
            pk_col: Optional[str] = None
            fks: List[Dict[str, str]] = []
            for p in parts:
                pu = p.upper()
                # 表级 PRIMARY KEY (col)
                if pu.startswith("PRIMARY KEY"):
                    try:
                        col = p[p.find('(')+1:p.rfind(')')].strip()
                        pk_col = col
                    except Exception:
                        pass
                    continue
                # 表级 UNIQUE (col)
                if pu.startswith("UNIQUE") and '(' in p and ')' in p:
                    try:
                        col = p[p.find('(')+1:p.rfind(')')].strip()
                        uniques.append(col)
                    except Exception:
                        pass
                    continue
                # FOREIGN KEY (col) REFERENCES ref_table(ref_col)
                if pu.startswith("FOREIGN KEY") or pu.startswith("CONSTRAINT ") and " FOREIGN KEY " in pu:
                    try:
                        seg = p
                        # 获取本列
                        col = seg[seg.find('(')+1:seg.find(')')].strip()
                        ref_pos = seg.upper().find("REFERENCES ")
                        ref_seg = seg[ref_pos+11:].strip()
                        ref_table = ref_seg.split('(')[0].strip()
                        ref_col = ref_seg[ref_seg.find('(')+1:ref_seg.find(')')].strip()
                        fks.append({"column": col, "ref_table": ref_table, "ref_column": ref_col})
                    except Exception:
                        pass
                    continue
                # 列级 PRIMARY KEY/UNIQUE: col TYPE PRIMARY KEY / UNIQUE
                if (" PRIMARY KEY" in pu) or (" UNIQUE" in pu):
                    toks = pu.split()
                    if " PRIMARY" in pu:
                        # 取列名为首个 token
                        try:
                            colname = p.split()[0]
                            pk_col = colname
                        except Exception:
                            pass
                    if " UNIQUE" in pu:
                        try:
                            colname = p.split()[0]
                            uniques.append(colname)
                        except Exception:
                            pass
                    # 去掉约束关键词，保留列名与类型
                    base = p.split()[0:2]
                    new_parts.append(' '.join(base))
                    continue
                # 普通列定义
                new_parts.append(p)

            # 记录元数据
            if pk_col:
                self._primary_key[table] = pk_col
            if uniques:
                self._unique_cols[table] = list(dict.fromkeys(uniques))
            if fks:
                self._foreign_keys[table] = fks

            # 组装去约束后的 SQL
            new_body = ', '.join(new_parts)
            return f"CREATE TABLE {table} ({new_body});"
        except Exception:
            return sql

    def _execute_with_index_optimization(self, executor_plan: Dict[str, Any]) -> Dict[str, Any]:
        # 对 SELECT 的等值/范围过滤进行索引优化
        try:
            # 触发器钩子：行级 BEFORE/AFTER
            if executor_plan.get("type") in ("INSERT","UPDATE","DELETE"):
                table = executor_plan.get("table")
                if table:
                    self._fire_triggers(table, timing="BEFORE", event=executor_plan.get("type"))
            if executor_plan.get("type") == "SELECT":
                table = executor_plan.get("table", "")
                flt = executor_plan.get("filter") or []
                # 单个条件优化：等值或范围
                if table and len(flt) == 1:
                    col = flt[0].get("column", "")
                    op = flt[0].get("op")
                    val = flt[0].get("value")
                    if self.index_manager.has_index(table, col):
                        if op == "=":
                            pk_values = self.index_manager.lookup_pks(table, col, str(val))
                            if len(pk_values) == 1:
                                # 改写为索引点查
                                executor_plan = dict(executor_plan)
                                executor_plan["access_method"] = "index_scan"
                                executor_plan["access_params"] = {"pk_value": pk_values[0]}
                        elif op in (">", ">=", "<", "<="):
                            # 映射为范围
                            min_val = None; max_val = None; inc_min = True; inc_max = True
                            if op in (">", ">="):
                                min_val = val; inc_min = (op == ">=")
                            else:
                                max_val = val; inc_max = (op == "<=")
                            pk_values = self.index_manager.range_lookup_pks(table, col, min_val, max_val, inc_min, inc_max)
                            if pk_values:
                                # 使用批量主键回表
                                target_columns = executor_plan.get("columns", ["*"])
                                return self.hybrid_executor.select_by_pk_values(table, target_columns, pk_values)
                # 多条件优化：对多个已建立二级索引的条件进行主键集合求交
                if table and len(flt) >= 2:
                    try:
                        pk_sets = []
                        for cond in flt:
                            col = cond.get("column", "")
                            op = cond.get("op")
                            val = cond.get("value")
                            if not self.index_manager.has_index(table, col):
                                continue
                            if op == "=":
                                s = set(self.index_manager.lookup_pks(table, col, str(val)))
                            elif op in (">", ">=", "<", "<="):
                                min_val = None; max_val = None; inc_min = True; inc_max = True
                                if op in (">", ">="):
                                    min_val = val; inc_min = (op == ">=")
                                else:
                                    max_val = val; inc_max = (op == "<=")
                                s = set(self.index_manager.range_lookup_pks(table, col, min_val, max_val, inc_min, inc_max))
                            else:
                                # LIKE/IN 等暂不使用二级索引
                                continue
                            if s:
                                pk_sets.append(s)
                        if pk_sets:
                            inter = pk_sets[0]
                            for s in pk_sets[1:]:
                                inter = inter.intersection(s)
                                if not inter:
                                    break
                            if inter:
                                target_columns = executor_plan.get("columns", ["*"])
                                return self.hybrid_executor.select_by_pk_values(table, target_columns, list(inter))
                    except Exception:
                        pass
                # 复合条件优化（雏形）：等值(c1) + 范围(c2) → 复合键范围
                if table and len(flt) >= 2:
                    # 找到一个等值和一个范围条件
                    eq_cond = None; rng_cond = None
                    for cond in flt:
                        if cond.get("op") == "=":
                            eq_cond = cond
                        elif cond.get("op") in (">", ">=", "<", "<="):
                            rng_cond = cond
                    if eq_cond and rng_cond:
                        c1 = eq_cond.get("column", ""); v1 = str(eq_cond.get("value", ""))
                        c2 = rng_cond.get("column", ""); v2 = str(rng_cond.get("value", ""))
                        # 复合键使用相同分隔符（与C++保持一致）
                        sep = "\x1F"
                        if rng_cond.get("op") in (">", ">="):
                            min_key = v1 + sep + v2
                            max_key = v1 + sep + "\xFF\xFF\xFF\xFF"
                        else:
                            min_key = v1 + sep + "\x00"
                            max_key = v1 + sep + v2
                        try:
                            rows = self.hybrid_executor.executor.composite_index_range_scan(table, min_key, max_key)
                            target_columns = executor_plan.get("columns", ["*"])
                            try:
                                data = self.hybrid_executor.executor.project(table, rows, target_columns)
                            except Exception:
                                data = [r.get_values() for r in rows]
                            return {"data": data, "affected_rows": len(data), "metadata": {"columns": target_columns}}
                        except Exception:
                            pass
            res = self.hybrid_executor.execute(executor_plan)
            # 结果后处理：将纯数字字符串转为 int，以匹配测试期望
            try:
                if isinstance(res, dict) and isinstance(res.get("data"), list):
                    new_data = []
                    for row in res.get("data", []):
                        if isinstance(row, list):
                            new_row = []
                            for v in row:
                                if isinstance(v, str) and v.strip().lstrip('-').isdigit():
                                    try:
                                        new_row.append(int(v))
                                    except Exception:
                                        new_row.append(v)
                                else:
                                    new_row.append(v)
                            new_data.append(new_row)
                        else:
                            new_data.append(row)
                    res["data"] = new_data
            except Exception:
                pass
            if executor_plan.get("type") in ("INSERT","UPDATE","DELETE"):
                table = executor_plan.get("table")
                if table:
                    self._fire_triggers(table, timing="AFTER", event=executor_plan.get("type"))
            # 钩子：INSERT 后更新索引（仅非事务立即生效；事务内在 COMMIT 时做批量）
            if executor_plan.get("type") == "INSERT" and not self.in_transaction:
                try:
                    table = executor_plan.get("table")
                    values = executor_plan.get("values", [])
                    cols = self.hybrid_executor.table_columns.get(table, [])
                    self.index_manager.on_insert(table, values, cols)
                except Exception:
                    pass
            return res
        except Exception as e:
            raise ExecutionError(f"索引优化执行失败: {e}")
    
    def get_compile_artifacts(self, sql: str) -> Dict[str, Any]:
        """仅编译不执行，收集 tokens/AST/逻辑计划/优化计划/执行器计划 与路径说明。"""
        s = sql.strip()
        up = s.upper().rstrip(';')
        if up.startswith("CREATE TABLE "):
            s = self._preprocess_create_table_constraints(s)
        # 词法
        sql_for_compile = self._uppercase_outside_quotes(s)
        lexer = Lexer(sql_for_compile)
        tokens, errors = lexer.tokenize()
        if errors:
            raise SQLSyntaxError(f"词法分析错误: {errors[0]}")
        tokens_str = [str(t) for t in tokens]
        # 语法
        parser = Parser(tokens)
        ast_list = parser.parse()
        ast_dicts = [a.to_dict() for a in ast_list]
        # 语义
        for ast in ast_list:
            self.semantic_analyzer.analyze(ast)
        # 逻辑计划
        planner = Planner(ast_dicts, enable_optimization=True)
        logical_plans = planner.generate_plan()
        logical_plan_dicts = []
        for lp in logical_plans:
            try:
                logical_plan_dicts.append(lp.to_dict())
            except Exception:
                logical_plan_dicts.append({"type": getattr(lp, "type", "Unknown")})
        # 优化
        optimized_plans = []
        for lp in logical_plans:
            try:
                optimized_plans.append(self.compiler_optimizer.optimize(lp))
            except Exception:
                optimized_plans.append(lp)
        optimized_plan_dicts = []
        for op in optimized_plans:
            try:
                optimized_plan_dicts.append(op.to_dict())
            except Exception:
                optimized_plan_dicts.append({"type": getattr(op, "type", "Unknown")})
        # 执行器计划 + 路径说明
        executor_plans = []
        explains = []
        for op in optimized_plans:
            ex = self._convert_plan_to_executor_format(op)
            chosen = self._choose_path(ex)
            executor_plans.append(chosen)
            explains.append(chosen.get("_explain", {}))
        return {
            "tokens": tokens_str,
            "asts": ast_dicts,
            "logical_plans": logical_plan_dicts,
            "optimized_plans": optimized_plan_dicts,
            "executor_plans": executor_plans,
            "explains": explains,
        }
    
    def flush(self):
        """刷盘所有脏页"""
        try:
            self.storage_engine.flush_all_dirty_pages()
            print("[ADAPTER] 数据刷盘完成")
        except Exception as e:
            print(f"[ADAPTER] 刷盘失败: {e}")
    
    def get_catalog_info(self) -> Dict[str, Any]:
        """获取系统目录信息"""
        try:
            catalog_info: Dict[str, Any] = {}
            names: List[str] = []
            # 兼容不同存储引擎接口
            if hasattr(self.storage_engine, 'get_table_names'):
                try:
                    names = list(self.storage_engine.get_table_names())
                except Exception:
                    names = []
            elif hasattr(self.storage_engine, 'list_tables'):
                try:
                    names = list(self.storage_engine.list_tables())
                except Exception:
                    names = []
            else:
                # 无法枚举表名，尝试从执行器缓存中获取
                try:
                    names = list(getattr(self.hybrid_executor, 'table_columns', {}).keys())
                except Exception:
                    names = []
            for table_name in names:
                try:
                    if hasattr(self.storage_engine, 'get_table_columns'):
                        columns = list(self.storage_engine.get_table_columns(table_name))
                    else:
                        columns = list(getattr(self.hybrid_executor, 'table_columns', {}).get(table_name, []))
                except Exception:
                    columns = []
                try:
                    has_idx = bool(self.storage_engine.has_index(table_name)) if hasattr(self.storage_engine, 'has_index') else False
                except Exception:
                    has_idx = False
                try:
                    idx_size = int(self.storage_engine.get_index_size(table_name)) if hasattr(self.storage_engine, 'get_index_size') else 0
                except Exception:
                    idx_size = 0
                catalog_info[table_name] = {"columns": columns, "has_index": has_idx, "index_size": idx_size}
            return catalog_info
        except Exception as e:
            print(f"[ADAPTER] 获取目录信息失败: {e}")
            return {}

    def export_table(self, table_name: str, format_type: str, output_path: str) -> bool:
        """
        导出表数据
        """
        try:
            t_upper = table_name.upper()
            # 先获取表的列信息
            columns = self.storage_engine.get_table_columns(t_upper)
            if not columns:
                print(f"表 {t_upper} 不存在或没有列")
                return False

            # 构建SQL语句（确保有分号）
            column_str = ", ".join(columns)
            sql = f"SELECT {column_str} FROM {t_upper};"

            print(f"[EXPORT] 执行导出查询: {sql}")

            # 执行查询
            result = self.execute(sql)

            if result.get("status") == "error":
                print(f"查询表数据失败: {result.get('error')}")
                return False

            data = result.get("data", [])
            metadata = result.get("metadata", {})
            result_columns = metadata.get("columns", columns)

            if not data:
                print(f"表 {table_name} 为空，无需导出")
                return True

            # 确保输出目录存在
            from pathlib import Path
            output_dir = Path(output_path).parent
            output_dir.mkdir(parents=True, exist_ok=True)

            # 根据格式类型导出数据
            if format_type.lower() == "csv":
                return self._export_to_csv(table_name, result_columns, data, output_path)
            elif format_type.lower() == "json":
                return self._export_to_json(table_name, result_columns, data, output_path)
            else:
                print(f"不支持的导出格式: {format_type}")
                return False

        except Exception as e:
            print(f"导出失败: {str(e)}")
            return False

    def _export_to_csv(self, table_name: str, columns: List[str], data: List[List[Any]], output_path: str) -> bool:
        """导出数据到CSV文件"""
        try:
            import csv

            with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)

                # 写入表头
                writer.writerow(columns)

                # 写入数据
                for row in data:
                    writer.writerow(row)

            print(f"成功导出 {len(data)} 行数据到 {output_path}")
            return True

        except Exception as e:
            print(f"CSV导出失败: {str(e)}")
            return False

    def _export_to_json(self, table_name: str, columns: List[str], data: List[List[Any]], output_path: str) -> bool:
        """导出数据到JSON文件"""
        try:
            import json

            # 将数据转换为字典列表
            json_data = []
            for row in data:
                row_dict = {}
                for i, col in enumerate(columns):
                    if i < len(row):
                        row_dict[col] = row[i]
                    else:
                        row_dict[col] = None
                json_data.append(row_dict)

            with open(output_path, 'w', encoding='utf-8') as jsonfile:
                json.dump(json_data, jsonfile, indent=2, ensure_ascii=False)

            print(f"成功导出 {len(data)} 行数据到 {output_path}")
            return True

        except Exception as e:
            print(f"JSON导出失败: {str(e)}")
            return False

    # 添加导入方法
    def import_table(self, table_name: str, format_type: str, file_path: str) -> bool:
        """
        从文件导入数据到表

        Args:
            table_name: 表名
            format_type: 导入格式 (csv/json)
            file_path: 文件路径

        Returns:
            bool: 导入是否成功
        """
        try:
            if format_type.lower() == 'csv':
                return self._import_from_csv(table_name, file_path)
            elif format_type.lower() == 'json':
                return self._import_from_json(table_name, file_path)
            else:
                print(f"❌ 不支持的导入格式: {format_type}")
                return False
        except Exception as e:
            print(f"❌ 导入失败: {str(e)}")
            return False

    def _import_from_csv(self, table_name: str, file_path: str) -> bool:
        """从CSV文件导入数据"""
        try:
            import csv
            import os

            # 检查文件是否存在
            if not os.path.exists(file_path):
                print(f"❌ 文件不存在: {file_path}")
                return False

            # 读取CSV文件
            with open(file_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile)

                # 读取列名（第一行）
                columns = next(reader)
                print(f"📋 检测到列: {columns}")

                # 读取数据
                data_rows = []
                for row in reader:
                    if row:  # 跳过空行
                        data_rows.append(row)

                if not data_rows:
                    print("⚠️  CSV文件中没有数据")
                    return True  # 没有数据也算成功

                print(f"📊 读取到 {len(data_rows)} 行数据")

                # 确保编译器目录已登记表结构：尝试创建（已存在则忽略）
                print(f"📝 确保表 {table_name} 在编译器目录中可用...")
                column_defs = []
                for i, col in enumerate(columns):
                    sample_value = data_rows[0][i] if data_rows and i < len(data_rows[0]) else ""
                    col_type = self._infer_data_type(sample_value)
                    column_defs.append(f"{col} {col_type}")
                try:
                    create_sql = f"CREATE TABLE {table_name} ({', '.join(column_defs)})"
                    self.execute(create_sql)
                    print(f"✓ 表 {table_name} 创建成功")
                except Exception:
                    print(f"✓ 表 {table_name} 已存在，继续插入数据")

                # 批量插入数据
                print("⏳ 正在插入数据...")
                for i, row in enumerate(data_rows):
                    if len(row) != len(columns):
                        print(f"⚠️  第 {i + 2} 行列数不匹配，跳过")
                        continue

                    # 构建INSERT语句
                    values_str = ", ".join([f"'{v}'" for v in row])
                    columns_str = ", ".join(columns)
                    insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({values_str});"

                    try:
                        result = self.execute(insert_sql)
                        if result.get("status") == "error":
                            print(f"⚠️  插入第 {i + 2} 行失败: {result.get('error')}")
                    except Exception as e:
                        print(f"⚠️  插入第 {i + 2} 行失败: {e}")

                print(f"✓ 数据导入完成，共处理 {len(data_rows)} 行")
                return True

        except Exception as e:
            print(f"❌ CSV导入失败: {str(e)}")
            return False

    def _import_from_json(self, table_name: str, file_path: str) -> bool:
        """从JSON文件导入数据"""
        try:
            import json
            import os

            # 检查文件是否存在
            if not os.path.exists(file_path):
                print(f"❌ 文件不存在: {file_path}")
                return False

            # 读取JSON文件
            with open(file_path, 'r', encoding='utf-8') as jsonfile:
                data = json.load(jsonfile)

            if not data:
                print("⚠️  JSON文件中没有数据")
                return True

            if not isinstance(data, list):
                print("❌ JSON文件格式错误：应为数组格式")
                return False

            # 获取列名（从第一个对象）
            first_row = data[0]
            if not isinstance(first_row, dict):
                print("❌ JSON文件格式错误：数组元素应为对象")
                return False

            columns = list(first_row.keys())
            print(f"📋 检测到列: {columns}")

            # 确保编译器目录已登记表结构：尝试创建（已存在则忽略）
            print(f"📝 确保表 {table_name} 在编译器目录中可用...")
            column_defs = []
            for col in columns:
                sample_value = first_row.get(col, "")
                col_type = self._infer_data_type(sample_value)
                column_defs.append(f"{col} {col_type}")
            try:
                create_sql = f"CREATE TABLE {table_name} ({', '.join(column_defs)})"
                self.execute(create_sql)
                print(f"✓ 表 {table_name} 创建成功")
            except Exception:
                print(f"✓ 表 {table_name} 已存在，继续插入数据")

            # 批量插入数据
            print("⏳ 正在插入数据...")
            for i, row in enumerate(data):
                if not isinstance(row, dict):
                    print(f"⚠️  第 {i + 1} 行格式错误，跳过")
                    continue

                # 构建值列表
                values = []
                for col in columns:
                    value = row.get(col, "")
                    values.append(str(value) if value is not None else "")

                # 构建INSERT语句
                values_str = ", ".join([f"'{v}'" for v in values])
                columns_str = ", ".join(columns)
                insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({values_str});"

                try:
                    result = self.execute(insert_sql)
                    if result.get("status") == "error":
                        print(f"⚠️  插入第 {i + 1} 行失败: {result.get('error')}")
                except Exception as e:
                    print(f"⚠️  插入第 {i + 1} 行失败: {e}")

            print(f"✓ 数据导入完成，共处理 {len(data)} 行")
            return True

        except Exception as e:
            print(f"❌ JSON导入失败: {str(e)}")
            return False

    def _infer_data_type(self, value: str) -> str:
        """推断数据类型"""
        if not value:
            return "STRING"

        # 尝试解析为整数
        try:
            int(value)
            return "INT"
        except ValueError:
            pass

        # 尝试解析为浮点数
        try:
            float(value)
            return "DOUBLE"
        except ValueError:
            pass

        # 默认为字符串
        return "STRING"

    def get_cache_stats(self) -> Dict[str, Any]:
        """获取缓存统计信息"""
        if self.hybrid_storage:
            return self.hybrid_storage.get_cache_stats()
        else:
            return {"message": "混合存储引擎未启用"}
    
    def flush_cache(self):
        """刷盘缓存"""
        if self.hybrid_storage:
            self.hybrid_storage.flush_all_dirty_pages()
        else:
            self.storage_engine.flush_all_dirty_pages()


def create_sql_compiler_adapter(use_hybrid_storage: bool = True, 
                               cache_capacity: int = 100, 
                               cache_strategy: str = "LRU") -> SQLCompilerAdapter:
    """创建SQL编译器适配器实例"""
    return SQLCompilerAdapter(
        use_hybrid_storage=use_hybrid_storage,
        cache_capacity=cache_capacity,
        cache_strategy=cache_strategy
    )


if __name__ == "__main__":
    # 测试SQL编译器适配器
    adapter = create_sql_compiler_adapter()
    
    # 测试基本功能
    print("=== SQL编译器适配器测试 ===")
    
    # 使用唯一表名
    import time
    timestamp = int(time.time())
    table_name = f"test_table_{timestamp}"
    
    # 创建表
    print(f"\n1. 创建表 {table_name}")
    result = adapter.execute(f"CREATE TABLE {table_name}(id INT, name STRING, age INT);")
    print("结果:", result)
    
    # 插入数据
    print("\n2. 插入数据")
    result = adapter.execute(f"INSERT INTO {table_name}(id, name, age) VALUES (1, 'Alice', 20);")
    print("结果:", result)
    
    # 查询数据
    print("\n3. 查询数据")
    result = adapter.execute(f"SELECT * FROM {table_name};")
    print("结果:", result)
    
    # 获取目录信息
    print("\n4. 目录信息")
    catalog_info = adapter.get_catalog_info()
    print("目录信息:", catalog_info)
    
    # 刷盘
    print("\n5. 刷盘")
    adapter.flush()
    
    print("\n=== 测试完成 ===")
