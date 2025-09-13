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

# 导入混合存储引擎
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from hybrid_storage_engine import HybridStorageEngine


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
        
        # 初始化存储引擎
        if use_hybrid_storage:
            try:
                # 使用混合存储引擎（集成OS存储缓存系统）
                self.hybrid_storage = HybridStorageEngine(
                    cache_capacity=cache_capacity,
                    cache_strategy=cache_strategy,
                    enable_cpp_acceleration=True
                )
                print("[ADAPTER] 混合存储引擎初始化成功")
                
                # 为了兼容现有接口，创建传统的执行引擎
                import db_core
                self.storage_engine = db_core.StorageEngine()
                self.execution_engine = db_core.ExecutionEngine(self.storage_engine)
                self.hybrid_executor = HybridExecutionEngine(self.storage_engine, self.execution_engine)
                print("[ADAPTER] C++执行引擎初始化成功")
                
            except Exception as e:
                print(f"[ADAPTER] 混合存储引擎初始化失败: {e}")
                # 回退到传统C++引擎
                try:
                    import db_core
                    self.storage_engine = db_core.StorageEngine()
                    self.execution_engine = db_core.ExecutionEngine(self.storage_engine)
                    self.hybrid_executor = HybridExecutionEngine(self.storage_engine, self.execution_engine)
                    self.hybrid_storage = None
                    print("[ADAPTER] 回退到传统C++执行引擎")
                except ImportError as e:
                    print(f"[ADAPTER] C++执行引擎初始化失败: {e}")
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
                "table": plan_dict["props"]["table"],
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
                "table": plan_dict["props"]["table"],
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
                "table": table_name,
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
            return {
                "type": "UPDATE",
                "table": plan_dict["props"]["table"],
                "set_clause": plan_dict["props"].get("set_clause", {}),
                "where_clause": plan_dict["props"].get("where_clause", {})
            }
        
        elif plan_type == "Delete":
            # 转换DELETE计划
            return {
                "type": "DELETE",
                "table": plan_dict["props"]["table"],
                "where_clause": plan_dict["props"].get("where_clause", {})
            }
        
        elif plan_type == "DropTable":
            # 转换DROP TABLE计划
            return {
                "type": "DROP_TABLE",
                "table": plan_dict["props"]["table"]
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
            return {
                "type": "SELECT",
                "table": plan_dict["props"].get("table", ""),
                "columns": plan_dict["props"].get("columns", []),
                "group_by": plan_dict["props"].get("group_columns", [])
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
        # 简易事务控制语句直通处理
        upper_sql = sql.upper().rstrip(';')
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
            # 1. 词法分析
            lexer = Lexer(sql)
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
            if sql.upper().startswith("EXPLAIN "):
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
                        self._txn_insert_buffer.setdefault(table, []).append(values)
                        print(f"[ADAPTER][TXN] 缓冲 INSERT -> {table}: {values}")
                        result = {"affected_rows": 1, "metadata": {"message": "已加入事务缓冲 (INSERT)"}}
                    else:
                        result = {"affected_rows": 0, "metadata": {"message": "INSERT 语句不完整，已忽略"}}
                else:
                    # 在非事务或不缓冲的语句直接执行，带路径选择与EXPLAIN
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

    def _handle_create_index(self, sql: str) -> Dict[str, Any]:
        # 语法（简化版）：CREATE INDEX idx ON table(col) PK pkcol;
        up = sql.strip().rstrip(';')
        try:
            # 粗略解析
            # 找到 ON 与 PK 关键字
            on_pos = up.upper().find(" ON ")
            pk_pos = up.upper().find(" PK ")
            if on_pos == -1 or pk_pos == -1 or pk_pos < on_pos:
                raise ValueError("语法: CREATE INDEX idx ON table(col) PK pkcol;")
            on_part = up[on_pos + 4: pk_pos].strip()
            pk_part = up[pk_pos + 4:].strip()
            # on_part 形如: table(col)
            table = on_part.split('(')[0].strip()
            col = on_part[on_part.find('(')+1:on_part.rfind(')')].strip()
            pkcol = pk_part
            ok = self.index_manager.create_index(self._parse_ident(table), self._parse_ident(col), self._parse_ident(pkcol))
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
        return {"affected_rows": len(items), "data": [[it["table"], it["column"], it["pk_column"]] for it in items], "metadata": {"columns": ["table", "column", "pk"]}}

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

    def _execute_with_index_optimization(self, executor_plan: Dict[str, Any]) -> Dict[str, Any]:
        # 对 SELECT 的等值/范围过滤进行索引优化
        try:
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
            table_names = self.storage_engine.get_table_names()
            catalog_info = {}
            for table_name in table_names:
                columns = self.storage_engine.get_table_columns(table_name)
                catalog_info[table_name] = {
                    "columns": columns,
                    "has_index": self.storage_engine.has_index(table_name),
                    "index_size": self.storage_engine.get_index_size(table_name)
                }
            return catalog_info
        except Exception as e:
            print(f"[ADAPTER] 获取目录信息失败: {e}")
            return {}

    def export_table(self, table_name: str, format_type: str, output_path: str) -> bool:
        """
        导出表数据
        """
        try:
            # 先获取表的列信息
            columns = self.storage_engine.get_table_columns(table_name)
            if not columns:
                print(f"表 {table_name} 不存在或没有列")
                return False

            # 构建SQL语句（确保有分号）
            column_str = ", ".join(columns)
            sql = f"SELECT {column_str} FROM {table_name};"

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
