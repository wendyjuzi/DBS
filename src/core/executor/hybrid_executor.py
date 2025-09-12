"""
混合架构执行引擎 - Python调度C++算子
"""

import time
from typing import Any, Dict, List, Optional, Callable, Tuple
from ...utils.transaction import TransactionManager
from ...utils.wal import WALManager, LogRecord
from ...utils.logging import get_logger
from ...index.index_manager import IndexManager
from ...utils.exceptions import ExecutionError, CatalogError


class HybridExecutionEngine:
    """混合架构执行引擎，Python调度C++算子"""

    def __init__(self, cpp_storage_engine, cpp_execution_engine):
        self.storage = cpp_storage_engine
        self.executor = cpp_execution_engine
        self.table_columns = {}
        # 简易 WAL + 事务
        self._wal = WALManager()
        self._txm = TransactionManager(self._wal)
        self._current_txid: Optional[str] = None
        # 事务覆盖层：table -> { 'insert': List[List[str]], 'delete': Set[str], 'mvcc_inserts': Set[str], 'mvcc_deletes': Set[str] }
        self._tx_overlay: Dict[str, Dict[str, Any]] = {}
        # 行版本链：key=(table, pk) -> {'head': Version}
        self._versions: Dict[tuple, Dict[str, Any]] = {}
        # 多列二级索引管理（内存）
        self.index_manager = IndexManager()
        self._logger = get_logger("executor")

    def insert_many(self, table: str, rows: List[List[str]]) -> int:
        if table not in self.table_columns:
            self._ensure_table_cached(table)
        if table not in self.table_columns:
            raise CatalogError(f"表 '{table}' 不存在")
        for r in rows:
            if len(r) != len(self.table_columns[table]):
                raise ExecutionError("批量插入某行列数不匹配")
        try:
            return int(self.executor.insert_many(table, rows))
        except Exception:
            # 回退逐行
            ok = 0
            for r in rows:
                if self.executor.insert(table, r):
                    ok += 1
            return ok

    def _ensure_table_cached(self, table_name: str) -> None:
        if table_name in self.table_columns:
            return
        try:
            cols = self.storage.get_table_columns(table_name)
            if cols:
                print(f"[EXEC] 缓存表结构: {table_name} -> {cols}")
                self.table_columns[table_name] = list(cols)
        except Exception as e:
            print(f"[EXEC] 加载表结构失败: {table_name}, err={e}")

    def execute(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        start_time = time.time()
        print(f"[EXEC] 收到计划: {plan}")
        try:
            t = plan.get("type")
            if t in ["CREATE_TABLE", "CreateTable"]:
                res = self._execute_create_table(plan)
            elif t in ["INSERT", "Insert"]:
                res = self._execute_insert(plan)
            elif t in ["SELECT", "Select"]:
                res = self._execute_select(plan)
            elif t in ["UPDATE", "Update"]:
                res = self._execute_update(plan)
            elif t in ["DELETE", "Delete"]:
                res = self._execute_delete(plan)
            elif t in ["DROP_TABLE", "DropTable"]:
                res = self._execute_drop_table(plan)
            else:
                raise ExecutionError(f"不支持的查询计划类型: {t}")
            res["execution_time"] = time.time() - start_time
            print(f"[EXEC] 执行完成, 用时 {res['execution_time']:.4f}s, 结果概要: affected_rows={res.get('affected_rows')}, data_len={len(res.get('data', []))}")
            return res
        except Exception as e:
            if isinstance(e, (ExecutionError, CatalogError)):
                print(f"[EXEC] 业务错误: {e}")
                raise
            print(f"[EXEC] 未预期错误: {e}")
            raise ExecutionError(f"执行查询时发生错误: {str(e)}")

    def _execute_create_table(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        # 支持两种计划格式：原始格式和SQL编译器格式
        if "props" in plan:
            # SQL编译器格式
            table_name = plan["props"]["table"]
            columns_def = plan["props"]["columns"]
        else:
            # 原始格式
            table_name = plan["table"]
            columns_def = plan["columns"]
        
        print(f"[EXEC] CREATE TABLE {table_name} cols={columns_def}")
        cpp_columns = []
        for col_def in columns_def:
            from db_core import Column, DataType
            if col_def["type"] == "INT":
                col_type = DataType.INT
            elif col_def["type"] == "STRING":
                col_type = DataType.STRING
            elif col_def["type"] == "DOUBLE":
                col_type = DataType.DOUBLE
            else:
                raise ExecutionError(f"不支持的数据类型: {col_def['type']}")
            cpp_columns.append(Column(col_def["name"], col_type, col_def.get("is_primary_key", False)))
        success = self.executor.create_table(table_name, cpp_columns)
        print(f"[EXEC] CREATE 调用C++返回: {success}")
        if success:
            self.table_columns[table_name] = [c["name"] for c in columns_def]
            return {"affected_rows": 0, "metadata": {"message": f"表 '{table_name}' 创建成功"}}
        raise ExecutionError(f"表 '{table_name}' 已存在")

    def _execute_insert(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        # 支持两种计划格式：原始格式和SQL编译器格式
        if "props" in plan:
            # SQL编译器格式
            table_name = plan["props"]["table"]
            # 从children中获取values
            values = []
            for child in plan.get("children", []):
                if child.get("type") == "Values":
                    rows = child.get("props", {}).get("rows", [])
                    if rows:
                        values = rows[0]  # 取第一行数据
                    break
        else:
            # 原始格式
            table_name = plan["table"]
            values = plan["values"]
        
        print(f"[EXEC] INSERT {table_name} values={values}")
        if table_name not in self.table_columns:
            self._ensure_table_cached(table_name)
        if table_name not in self.table_columns:
            raise CatalogError(f"表 '{table_name}' 不存在")
        expected_cols = len(self.table_columns[table_name])
        if len(values) != expected_cols:
            raise ExecutionError(f"列数不匹配，期望 {expected_cols} 列，实际 {len(values)} 列")
        # 事务与日志
        txid = self._current_txid or self._txm.begin().txid
        is_implicit_tx = self._current_txid is None
        self._wal.append(LogRecord(txid=txid, op="INSERT", table=table_name, payload={"values": values}))
        # 若在显式事务中，写入版本链（下沉到C++），不落到底层
        if not is_implicit_tx:
            pk_col = self._get_pk_column(table_name)
            if pk_col is None:
                ov = self._tx_overlay.setdefault(table_name, {"insert": [], "delete": set(), "mvcc_inserts": set()})
                ov["insert"].append(list(values))
            else:
                pk_idx = self.table_columns[table_name].index(pk_col)
                pk_value = str(values[pk_idx])
                try:
                    # 调用C++ MVCC插入未提交版本
                    self.storage.mvcc_insert_uncommitted(table_name, list(values), txid, pk_idx)
                except Exception:
                    pass
                ov = self._tx_overlay.setdefault(table_name, {"insert": [], "delete": set(), "mvcc_inserts": set()})
                ov["mvcc_inserts"].add(pk_value)
            # 覆盖层下立即维护二级索引（仅查询加速）
            try:
                self.index_manager.on_insert(table_name, values, self.table_columns[table_name])
            except Exception:
                pass
            return {"affected_rows": 1}

        success = self.executor.insert(table_name, values)
        print(f"[EXEC] INSERT 调用C++返回: {success}")
        if success:
            # 维护二级索引
            try:
                self.index_manager.on_insert(table_name, values, self.table_columns[table_name])
            except Exception:
                pass
            if is_implicit_tx:
                self._txm.commit(txid)
            return {"affected_rows": 1}
        raise ExecutionError("插入失败（行数据过大或存储空间不足）")

    def _execute_select(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        # 检查是否有JOIN
        tables = plan.get("tables", [plan.get("table")])
        joins = plan.get("joins", [])
        
        if len(tables) > 1 or joins:
            # 执行JOIN查询
            return self._execute_join(plan)
        
        # 检查是否有ORDER BY
        order_by = plan.get("order_by")
        if order_by:
            # 执行ORDER BY查询
            return self._execute_order_by(plan)
        
        # 检查是否有GROUP BY
        group_by = plan.get("group_by")
        if group_by:
            # 执行GROUP BY查询
            return self._execute_group_by(plan)
        
        # 常规单表查询
        table_name = plan["table"]
        target_columns = plan.get("columns", ["*"])
        where_clause = plan.get("where")
        filter_conditions = plan.get("filter")
        access_method = plan.get("access_method")
        access_params = plan.get("access_params", {})
        print(f"[EXEC] SELECT {table_name} cols={target_columns} where={where_clause} access={access_method} params={access_params}")
        if table_name not in self.table_columns:
            self._ensure_table_cached(table_name)
        if table_name not in self.table_columns:
            raise CatalogError(f"表 '{table_name}' 不存在")
        if target_columns == ["*"]:
            target_columns = self.table_columns[table_name]
        # 构造 C++ 下推条件 (colIdx, op, value)
        pushdown: List[Tuple[int, str, str]] = []
        if filter_conditions:
            for cond in filter_conditions:
                col = cond.get("column"); op = cond.get("op"); val = str(cond.get("value"))
                if col in self.table_columns[table_name]:
                    pushdown.append((self.table_columns[table_name].index(col), op, val))
        # 尝试使用内存二级索引加速：等值或范围
        if filter_conditions:
            try:
                # 优先使用等值条件
                for cond in filter_conditions:
                    col = cond.get("column"); op = (cond.get("op") or "").strip(); val = str(cond.get("value"))
                    if op == "=" and self.index_manager.has_index(table_name, col):
                        pks = self.index_manager.lookup_pks(table_name, col, val)
                        if pks:
                            return self.select_by_pk_values(table_name, target_columns, pks)
                # 尝试范围条件（需要相同列上的单个范围）
                rng_col = None; min_v = None; max_v = None; inc_min = True; inc_max = True
                for cond in filter_conditions:
                    col = cond.get("column"); op = (cond.get("op") or "").strip(); val = str(cond.get("value"))
                    if not self.index_manager.has_index(table_name, col):
                        continue
                    if rng_col and col != rng_col:
                        continue
                    rng_col = col
                    if op in (">", ">="):
                        min_v = val; inc_min = (op == ">=")
                    elif op in ("<", "<="):
                        max_v = val; inc_max = (op == "<=")
                if rng_col and (min_v is not None or max_v is not None):
                    pks = self.index_manager.range_lookup_pks(table_name, rng_col, min_v, max_v, inc_min, inc_max)
                    if pks:
                        return self.select_by_pk_values(table_name, target_columns, pks)
            except Exception:
                pass
        # 选择路径
        if access_method and getattr(access_method, "value", access_method) in ("index_scan", "index_range_scan"):
            rows = []
            try:
                m = access_method.value if hasattr(access_method, 'value') else access_method
                if m == "index_scan":
                    pk_value = str(access_params.get("pk_value", ""))
                    print(f"[EXEC] 索引点查 pk={pk_value}, index_size={getattr(self.storage,'get_index_size',lambda n: 'NA')(table_name)}")
                    row = self.executor.index_scan(table_name, pk_value)
                    if row:
                        rows = [row]
                else:
                    min_pk = str(access_params.get("min_pk", ""))
                    max_pk = str(access_params.get("max_pk", "\xFF\xFF\xFF\xFF"))
                    print(f"[EXEC] 索引范围 min={min_pk} max={max_pk}, index_size={getattr(self.storage,'get_index_size',lambda n: 'NA')(table_name)}")
                    rows = self.executor.index_range_scan(table_name, min_pk, max_pk)
            except Exception as e:
                print(f"[EXEC] 索引路径异常，回退顺扫: {e}")
                rows = self.executor.seq_scan(table_name)
            # 对索引结果应用过滤（若存在额外条件）
            filtered_rows = rows
            if pushdown:
                try:
                    filtered_rows = self.executor.filter_conditions(table_name, pushdown)
                except Exception as e:
                    print(f"[EXEC] 下推过滤不可用，回退: {e}")
                    filtered_rows = self._python_filter(rows, pushdown)
            if not filtered_rows:
                print("[EXEC] 索引未命中或过滤后为空，回退顺扫+过滤")
                scanned_rows = self.executor.seq_scan(table_name)
                filtered_rows = self._python_filter(scanned_rows, pushdown) if pushdown else scanned_rows
            # 应用 MVCC 可见性替换
            filtered_rows = self._apply_mvcc_to_rows(table_name, filtered_rows)
        else:
            scanned_rows = self.executor.seq_scan(table_name)
            print(f"[EXEC] SEQ 扫描返回 {len(scanned_rows)} 行")
            if pushdown:
                try:
                    filtered_rows = self.executor.filter_conditions(table_name, pushdown)
                except Exception as e:
                    print(f"[EXEC] 下推过滤不可用，改用 Python 过滤: {e}")
                    filtered_rows = self._python_filter(scanned_rows, pushdown)
            else:
                # 无条件
                filtered_rows = scanned_rows
            print(f"[EXEC] 过滤后 {len(filtered_rows)} 行")
        # MVCC：在投影前应用版本链可见性替换（调用C++可见性查询）
        filtered_rows = self._apply_mvcc_to_rows(table_name, filtered_rows)
        # 投影
        try:
            projected_data = self.executor.project(table_name, filtered_rows, target_columns)
        except Exception:
            projected_data = [r.get_values() for r in filtered_rows]
        # 事务覆盖层合并（仅当前表，无JOIN时）：
        if self._current_txid and table_name in self._tx_overlay:
            try:
                projected_data = self._merge_txn_overlay_select(table_name, target_columns, projected_data, pushdown)
            except Exception:
                pass
        print(f"[EXEC] 投影列 {target_columns} -> 返回 {len(projected_data)} 行")
        return {"data": projected_data, "affected_rows": len(projected_data), "metadata": {"columns": target_columns}}

    def _python_filter(self, rows, pushdown: List[Tuple[int, str, str]]):
        def eval_cond(lhs: str, op: str, rhs: str) -> bool:
            try:
                ln = float(lhs); rn = float(rhs)
                if op == "=": return ln == rn
                if op == ">": return ln > rn
                if op == "<": return ln < rn
                if op == ">=": return ln >= rn
                if op == "<=": return ln <= rn
                if op == "!=": return ln != rn
                return False
            except:
                if op == "=": return lhs == rhs
                if op == ">": return lhs > rhs
                if op == "<": return lhs < rhs
                if op == ">=": return lhs >= rhs
                if op == "<=": return lhs <= rhs
                if op == "!=": return lhs != rhs
                return False
        out = []
        for r in rows:
            vals = r.get_values()
            ok = True
            for idx,op,val in pushdown:
                if idx < 0 or idx >= len(vals) or not eval_cond(vals[idx], op, val):
                    ok = False; break
            if ok: out.append(r)
        return out

    def select_by_pk_values(self, table_name: str, target_columns: List[str], pk_values: List[str]) -> Dict[str, Any]:
        """基于主键集合的批量点查并投影，用于索引范围命中后的快速回表。"""
        if table_name not in self.table_columns:
            self._ensure_table_cached(table_name)
        if table_name not in self.table_columns:
            raise CatalogError(f"表 '{table_name}' 不存在")
        if target_columns == ["*"]:
            target_columns = self.table_columns[table_name]
        rows = []
        for pk in pk_values:
            try:
                r = self.executor.index_scan(table_name, str(pk))
                if r:
                    rows.append(r)
            except Exception:
                # 任意单个失败跳过，不影响其它主键
                continue
        try:
            projected_data = self.executor.project(table_name, rows, target_columns)
        except Exception:
            projected_data = [r.get_values() for r in rows]
        return {"data": projected_data, "affected_rows": len(projected_data), "metadata": {"columns": target_columns}}

    def _execute_delete(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        table_name = plan["table"]
        where_clause = plan.get("where")
        if table_name not in self.table_columns:
            self._ensure_table_cached(table_name)
        if table_name not in self.table_columns:
            raise CatalogError(f"表 '{table_name}' 不存在")
        predicate = self._build_predicate(table_name, where_clause)
        # 简化：逐行扫描找主键以维护索引与记录 WAL
        txid = self._current_txid or self._txm.begin().txid
        is_implicit_tx = self._current_txid is None
        deleted_count = 0
        # 需要列名与主键列
        col_names = self.table_columns.get(table_name, [])
        pk_col = self._get_pk_column(table_name)
        # 无法确认主键时直接调用 C++ 删除
        if pk_col is None:
            self._wal.append(LogRecord(txid=txid, op="DELETE", table=table_name, payload={"where": where_clause}))
            # 覆盖层场景：若在事务中，退回到底层删除将破坏隔离，这里在显式事务内改为不物化
            if is_implicit_tx:
                deleted_count = self.executor.delete_rows(table_name, predicate)
            else:
                # 事务中：扫描PK并标记删除
                ov = self._tx_overlay.setdefault(table_name, {"insert": [], "delete": set()})
                for r in self.executor.seq_scan(table_name):
                    vals = r.get_values()
                    try:
                        if predicate(vals):
                            # 无法获知PK，跳过
                            pass
                    except Exception:
                        continue
        else:
            # 扫描并按谓词删除，维护索引
            for r in self.executor.seq_scan(table_name):
                vals = r.get_values()
                try:
                    if predicate(vals):
                        pk_value = vals[col_names.index(pk_col)]
                        self._wal.append(LogRecord(txid=txid, op="DELETE", table=table_name, payload={"pk": pk_value}))
                        if is_implicit_tx:
                            # 直接物化删除
                            try:
                                # 优先用谓词删除
                                deleted_count += int(self.executor.delete_rows(table_name, lambda x, pv=str(pk_value), idx=col_names.index(pk_col): x[idx] == pv))
                            except Exception:
                                pass
                            self.index_manager.on_delete(table_name, str(pk_value))
                        else:
                            # 事务中：标记删除（MVCC：提交时设置 xmax）
                            ov = self._tx_overlay.setdefault(table_name, {"insert": [], "delete": set(), "mvcc_inserts": set(), "mvcc_deletes": set()})
                            ov["delete"].add(str(pk_value))
                            ov["mvcc_deletes"].add(str(pk_value))
                            deleted_count += 1
                except Exception:
                    continue
        if is_implicit_tx:
            self._txm.commit(txid)
        return {"affected_rows": deleted_count, "metadata": {"message": f"删除了 {deleted_count} 行"}}

    def _build_predicate(self, table_name: str, where_clause: Optional[str]) -> Callable[[List[str]], bool]:
        if not where_clause:
            return lambda x: True
        col_names = self.table_columns.get(table_name, [])
        processed_clause = where_clause
        for col_idx, col_name in enumerate(col_names):
            if col_name in processed_clause:
                processed_clause = processed_clause.replace(f"{col_name} ", f"x[{col_idx}] ")
                processed_clause = processed_clause.replace(f" {col_name}", f" x[{col_idx}]")
                processed_clause = processed_clause.replace(f"'{col_name}'", f"x[{col_idx}]")
        # 归一化逻辑与比较运算符
        import re as _re
        processed_clause = _re.sub(r"\\bAND\\b", "and", processed_clause, flags=_re.IGNORECASE)
        processed_clause = _re.sub(r"\\bOR\\b", "or", processed_clause, flags=_re.IGNORECASE)
        processed_clause = _re.sub(r"\\bNOT\\b", "not", processed_clause, flags=_re.IGNORECASE)
        processed_clause = _re.sub(r"<>", "!=", processed_clause)
        processed_clause = _re.sub(r"(?<![<>!=])=(?![=])", "==", processed_clause)
        try:
            return eval(f"lambda x: {processed_clause}")
        except Exception as e:
            raise ExecutionError(f"WHERE条件解析错误: {str(e)}")

    def _execute_update(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """执行UPDATE语句"""
        table_name = plan["table"]
        set_clauses = plan.get("set_clauses", [])
        where_clause = plan.get("where")
        
        print(f"[EXEC] UPDATE {table_name} SET {set_clauses} WHERE {where_clause}")
        
        if table_name not in self.table_columns:
            self._ensure_table_cached(table_name)
        if table_name not in self.table_columns:
            raise CatalogError(f"表 '{table_name}' 不存在")
        
        # 构建WHERE谓词
        predicate = self._build_predicate(table_name, where_clause)
        
        # 调用C++ UPDATE算子
        # 简化：记录 WAL，调用底层更新，无法轻易提取旧值时跳过索引维护
        txid = self._current_txid or self._txm.begin().txid
        is_implicit_tx = self._current_txid is None
        self._wal.append(LogRecord(txid=txid, op="UPDATE", table=table_name, payload={"set": set_clauses, "where": where_clause}))
        updated_count = self.executor.update_rows(table_name, set_clauses, predicate)
        if is_implicit_tx:
            self._txm.commit(txid)
        
        return {
            "affected_rows": updated_count,
            "metadata": {"message": f"更新了 {updated_count} 行"}
        }

    # --- 事务 API ---
    def begin(self) -> str:
        if self._current_txid:
            return self._current_txid
        txn = self._txm.begin()
        self._current_txid = txn.txid
        self._tx_overlay.clear()
        # 快照 = 当前 txid 与活跃 tx 集
        self._current_snapshot = {
            "txid": txn.txid,
            "active": self._txm.get_active_txids()
        }
        return txn.txid

    def commit(self) -> None:
        if not self._current_txid:
            return
        # 物化覆盖层 + 提交版本链
        for table_name, ov in list(self._tx_overlay.items()):
            inserts: List[List[str]] = ov.get("insert", []) or []
            deletes: set = ov.get("delete", set()) or set()
            mvcc_inserts: set = ov.get("mvcc_inserts", set()) or set()
            mvcc_deletes: set = ov.get("mvcc_deletes", set()) or set()
            # 应用删除
            if deletes:
                pk_col = self._get_pk_column(table_name)
                if pk_col is not None and table_name in self.table_columns:
                    idx = self.table_columns[table_name].index(pk_col)
                    for pk in list(deletes):
                        try:
                            self.executor.delete_rows(table_name, lambda x, pv=str(pk), i=idx: x[i] == pv)
                        except Exception:
                            pass
                        self.index_manager.on_delete(table_name, str(pk))
            # 应用插入
            for row in inserts:
                try:
                    if self.executor.insert(table_name, row):
                        self.index_manager.on_insert(table_name, row, self.table_columns.get(table_name, []))
                except Exception:
                    pass
            # 提交版本链插入（将本事务创建的 head 置为已提交）
            for pk in list(mvcc_inserts):
                try:
                    self.storage.mvcc_commit_insert(table_name, pk, self._current_txid)
                except Exception:
                    pass
            # 提交版本链删除（设置可见版本的 xmax）
            if mvcc_deletes:
                for pk in list(mvcc_deletes):
                    try:
                        self.storage.mvcc_mark_delete_commit(table_name, pk, self._current_txid)
                    except Exception:
                        pass
        self._tx_overlay.clear()
        self._txm.commit(self._current_txid)
        self._current_txid = None
        self._current_snapshot = None

    def rollback(self) -> None:
        if not self._current_txid:
            return
        # 丢弃覆盖层 + 弹出未提交版本 + 撤销本事务设置的 xmax
        for table_name, ov in list(self._tx_overlay.items()):
            mvcc_inserts: set = ov.get("mvcc_inserts", set()) or set()
            mvcc_deletes: set = ov.get("mvcc_deletes", set()) or set()
            for pk in list(mvcc_inserts):
                try:
                    self.storage.mvcc_rollback_insert(table_name, pk, self._current_txid)
                except Exception:
                    pass
            for pk in list(mvcc_deletes):
                head = self._get_version_head(table_name, pk)
                cur = head
                while cur:
                    if cur.xmax == self._current_txid:
                        cur.xmax = None
                        break
                    cur = cur.next
        self._tx_overlay.clear()
        self._txm.rollback(self._current_txid)
        self._current_txid = None
        self._current_snapshot = None

    # --- 刷盘 ---
    def flush_all_dirty_pages(self) -> None:
        try:
            if hasattr(self.storage, "flush_all_dirty_pages"):
                self.storage.flush_all_dirty_pages()
            elif hasattr(self.storage, "flush_all"):
                self.storage.flush_all()
        except Exception:
            pass
    
    def get_tx_overlay_snapshot(self) -> Dict[str, Any]:
        """返回事务覆盖层的快照信息，供 CLI/Adapter 观测。
        格式: { in_tx: bool, tables: { table: { inserts: int, deletes: int } } }
        """
        info: Dict[str, Any] = {"in_tx": bool(self._current_txid), "tables": {}}
        if not self._current_txid:
            return info
        for table_name, ov in self._tx_overlay.items():
            inserts = len(ov.get("insert", []) or [])
            deletes = len(ov.get("delete", set()) or set())
            info["tables"][table_name] = {"inserts": inserts, "deletes": deletes}
        return info

    # --- MVCC version chain (skeleton) ---
    class _Version:
        def __init__(self, values: List[str], xmin: str, committed: bool):
            self.values = list(values)
            self.xmin = xmin
            self.xmax: Optional[str] = None
            self.committed = committed
            self.next: Optional['HybridExecutionEngine._Version'] = None

    def _get_version_head(self, table: str, pk: str) -> Optional['HybridExecutionEngine._Version']:
        node = self._versions.get((table, pk))
        return node.get('head') if node else None

    def _set_version_head(self, table: str, pk: str, head: 'HybridExecutionEngine._Version') -> None:
        self._versions[(table, pk)] = {'head': head}

    def _is_visible(self, ver: 'HybridExecutionEngine._Version', snapshot: Dict[str, Any]) -> bool:
        if not ver.committed:
            # 仅对本事务可见
            return self._current_txid is not None and ver.xmin == self._current_txid
        txid = snapshot.get('txid') if snapshot else None
        active = snapshot.get('active') if snapshot else set()
        if txid is None:
            return ver.committed and ver.xmax is None
        # 可见性规则：xmin 提交且不在活跃集中，xmin <= txid 且 (xmax is None or xmax > txid)
        if ver.xmax is not None and ver.xmax <= txid:
            return False
        if ver.xmin in active:
            return False
        return True

    def _lookup_visible_version(self, table: str, pk: str) -> Optional[List[str]]:
        head = self._get_version_head(table, pk)
        cur = head
        snap = getattr(self, '_current_snapshot', None)
        while cur:
            if self._is_visible(cur, snap):
                return list(cur.values)
            cur = cur.next
        return None

    # --- helpers ---
    def _get_pk_column(self, table_name: str) -> Optional[str]:
        try:
            cols = list(self.storage.get_table_columns(table_name))
            # 简化：推断常见主键列名
            for name in cols:
                if str(name).lower() in ("id", "pk", "primary", "primary_key"):
                    return name
        except Exception:
            pass
        # 回退：若列缓存可用，尝试同样规则
        for name in self.table_columns.get(table_name, []):
            if str(name).lower() in ("id", "pk", "primary", "primary_key"):
                return name
        return None

    def _merge_txn_overlay_select(self, table_name: str, target_columns: List[str], projected_rows: List[List[str]], pushdown: List[Tuple[int, str, str]]):
        """在结果集层面合并事务覆盖层：追加新增、剔除删除（当 PK 列被投影时）。"""
        ov = self._tx_overlay.get(table_name) or {}
        inserts: List[List[str]] = ov.get("insert", []) or []
        deletes: set = ov.get("delete", set()) or set()
        base = list(projected_rows)
        # 删除：仅当 PK 出现在投影列中
        pk_col = self._get_pk_column(table_name)
        if pk_col and pk_col in target_columns and deletes:
            pk_idx_in_proj = target_columns.index(pk_col)
            base = [r for r in base if str(r[pk_idx_in_proj]) not in deletes]

        # 追加插入：按投影列构造并应用过滤（基于 pushdown 条件）
        if inserts:
            # 建立列名->下标
            table_cols = self.table_columns.get(table_name, [])
            col_to_idx = {c: i for i, c in enumerate(table_cols)}
            # 评估函数（与 _python_filter 一致，但基于值序列）
            def eval_cond(lhs: str, op: str, rhs: str) -> bool:
                try:
                    ln = float(lhs); rn = float(rhs)
                    if op == "=": return ln == rn
                    if op == ">": return ln > rn
                    if op == "<": return ln < rn
                    if op == ">=": return ln >= rn
                    if op == "<=": return ln <= rn
                    if op == "!=": return ln != rn
                    return False
                except:
                    if op == "=": return lhs == rhs
                    if op == ">": return lhs > rhs
                    if op == "<": return lhs < rhs
                    if op == ">=": return lhs >= rhs
                    if op == "<=": return lhs <= rhs
                    if op == "!=": return lhs != rhs
                    return False
            for row in inserts:
                # 过滤（如果有）
                ok = True
                for idx, op, val in pushdown:
                    if idx < 0 or idx >= len(table_cols):
                        continue
                    src_idx = idx
                    if src_idx >= len(row) or not eval_cond(str(row[src_idx]), op, val):
                        ok = False; break
                if not ok:
                    continue
                # 投影
                proj = []
                for c in target_columns:
                    if c in col_to_idx and col_to_idx[c] < len(row):
                        proj.append(row[col_to_idx[c]])
                if proj:
                    base.append(proj)
        return base

    def _apply_mvcc_to_rows(self, table_name: str, rows):
        """使用 C++ 版本链按当前快照替换可见行；对未命中版本链的行原样返回。"""
        if not self._current_txid:
            return rows
        table_cols = self.table_columns.get(table_name, [])
        pk_col = self._get_pk_column(table_name)
        if not pk_col or pk_col not in table_cols:
            return rows
        pk_idx = table_cols.index(pk_col)
        try:
            from db_core import Row as CppRow
        except Exception:
            CppRow = None
        snap = getattr(self, '_current_snapshot', None) or {}
        reader = str(snap.get('txid', ''))
        active = list(snap.get('active', []))
        out = []
        for r in rows:
            vals = r.get_values()
            if pk_idx >= len(vals):
                continue
            pk = str(vals[pk_idx])
            visible = None
            try:
                vis = self.storage.mvcc_lookup_visible(table_name, pk, reader, active)
                if vis:
                    visible = list(vis)
            except Exception:
                pass
            if visible is None:
                # 可能版本链无记录，保留底层
                out.append(r)
            else:
                if CppRow:
                    out.append(CppRow(visible))
                else:
                    out.append(r)
        # 事务内新插入但底层无记录的 PK，追加为可见
        ov = self._tx_overlay.get(table_name) or {}
        for pk in list(ov.get("mvcc_inserts", set()) or set()):
            try:
                vis = self.storage.mvcc_lookup_visible(table_name, pk, reader, active)
                if vis and CppRow:
                    out.append(CppRow(list(vis)))
            except Exception:
                continue
        return out

    def _execute_join(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """执行JOIN操作"""
        tables = plan.get("tables", [])
        joins = plan.get("joins", [])
        target_columns = plan.get("columns", ["*"])
        where_clause = plan.get("where")
        join_algo = (plan.get("join_algo") or "").lower()  # "merge" | "hash" | ""
        
        print(f"[EXEC] JOIN tables={tables} joins={joins} cols={target_columns}")
        
        if len(tables) < 2:
            raise ExecutionError("JOIN需要至少两个表")
        
        # 简化实现：只支持两个表的内连接
        if len(tables) == 2 and len(joins) == 1:
            left_table = tables[0]
            right_table = tables[1]
            join_info = joins[0]
            
            # 选择JOIN算法
            if join_algo == "merge":
                joined_rows = self.executor.merge_join(
                    left_table, right_table,
                    join_info["left_column"], join_info["right_column"]
                )
            else:
                joined_rows = self.executor.inner_join(
                    left_table, right_table,
                    join_info["left_column"], join_info["right_column"]
                )
            
            # 应用WHERE条件（如果有）
            if where_clause:
                # 简化WHERE处理：假设是单表条件
                filtered_rows = []
                for row in joined_rows:
                    # 这里需要更复杂的WHERE条件处理
                    # 简化实现：跳过WHERE过滤
                    filtered_rows.append(row)
                joined_rows = filtered_rows
            
            # 应用投影（如果有）
            if target_columns != ["*"]:
                # 简化投影处理
                projected_rows = []
                for row in joined_rows:
                    # 这里需要根据列名映射进行投影
                    projected_rows.append(row)
                joined_rows = projected_rows
            
            return {
                "affected_rows": len(joined_rows),
                "data": joined_rows,
                "metadata": {"message": f"JOIN返回 {len(joined_rows)} 行", "join_algo": (join_algo or "hash")}
            }
        else:
            raise ExecutionError("暂不支持复杂的多表JOIN")

    def _execute_order_by(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """执行ORDER BY操作"""
        table_name = plan.get("table")
        order_by = plan.get("order_by", [])
        
        print(f"[EXEC] ORDER BY {table_name} {order_by}")
        
        if table_name not in self.table_columns:
            self._ensure_table_cached(table_name)
        if table_name not in self.table_columns:
            raise CatalogError(f"表 '{table_name}' 不存在")
        
        # 调用C++ ORDER BY算子
        sorted_rows = self.executor.order_by(table_name, order_by)
        
        # 转换为数据格式
        data = []
        for row in sorted_rows:
            data.append(row.get_values())
        
        return {
            "affected_rows": len(data),
            "data": data,
            "metadata": {"message": f"排序返回 {len(data)} 行"}
        }

    def _execute_group_by(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """执行GROUP BY操作"""
        table_name = plan.get("table")
        group_by = plan.get("group_by", {})
        
        print(f"[EXEC] GROUP BY {table_name} {group_by}")
        
        if table_name not in self.table_columns:
            self._ensure_table_cached(table_name)
        if table_name not in self.table_columns:
            raise CatalogError(f"表 '{table_name}' 不存在")
        
        group_columns = group_by.get("group_columns", [])
        aggregates = group_by.get("aggregates", [])
        
        # 调用C++ GROUP BY算子
        group_results = self.executor.group_by(table_name, group_columns, aggregates)
        
        # 转换为数据格式
        data = []
        for result in group_results:
            row = []
            # 添加分组键
            row.extend(result.group_keys)
            # 添加聚合值
            for agg_name, agg_value in result.aggregates.items():
                row.append(str(agg_value))
            data.append(row)
        
        return {
            "affected_rows": len(data),
            "data": data,
            "metadata": {"message": f"分组聚合返回 {len(data)} 组"}
        }

    def _execute_drop_table(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """执行DROP TABLE操作"""
        # 支持两种计划格式：原始格式和SQL编译器格式
        if "props" in plan:
            # SQL编译器格式
            table_name = plan["props"]["table"]
        else:
            # 原始格式
            table_name = plan["table"]
        
        print(f"[EXEC] DROP TABLE {table_name}")
        
        # 检查表是否存在
        if table_name not in self.table_columns:
            self._ensure_table_cached(table_name)
        
        # 调用C++ DROP TABLE算子
        success = self.executor.drop_table(table_name)
        
        if success:
            # 清理本地缓存
            if table_name in self.table_columns:
                del self.table_columns[table_name]
            
            return {
                "affected_rows": 0,
                "metadata": {"message": f"表 '{table_name}' 删除成功"}
            }
        else:
            raise ExecutionError(f"表 '{table_name}' 不存在或删除失败")
