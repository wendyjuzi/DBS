"""
混合架构执行引擎 - Python调度C++算子
"""

import time
from typing import Any, Dict, List, Optional, Callable, Tuple
from ...utils.exceptions import ExecutionError, CatalogError


class HybridExecutionEngine:
    """混合架构执行引擎，Python调度C++算子"""

    def __init__(self, cpp_storage_engine, cpp_execution_engine):
        self.storage = cpp_storage_engine
        self.executor = cpp_execution_engine
        self.table_columns = {}

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
        success = self.executor.insert(table_name, values)
        print(f"[EXEC] INSERT 调用C++返回: {success}")
        if success:
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
        # 投影
        try:
            projected_data = self.executor.project(table_name, filtered_rows, target_columns)
        except Exception:
            projected_data = [r.get_values() for r in filtered_rows]
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

    def _execute_delete(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        table_name = plan["table"]
        where_clause = plan.get("where")
        if table_name not in self.table_columns:
            self._ensure_table_cached(table_name)
        if table_name not in self.table_columns:
            raise CatalogError(f"表 '{table_name}' 不存在")
        predicate = self._build_predicate(table_name, where_clause)
        deleted_count = self.executor.delete_rows(table_name, predicate)
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
        updated_count = self.executor.update_rows(table_name, set_clauses, predicate)
        
        return {
            "affected_rows": updated_count,
            "metadata": {"message": f"更新了 {updated_count} 行"}
        }

    def _execute_join(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """执行JOIN操作"""
        tables = plan.get("tables", [])
        joins = plan.get("joins", [])
        target_columns = plan.get("columns", ["*"])
        where_clause = plan.get("where")
        
        print(f"[EXEC] JOIN tables={tables} joins={joins} cols={target_columns}")
        
        if len(tables) < 2:
            raise ExecutionError("JOIN需要至少两个表")
        
        # 简化实现：只支持两个表的内连接
        if len(tables) == 2 and len(joins) == 1:
            left_table = tables[0]
            right_table = tables[1]
            join_info = joins[0]
            
            # 调用C++ JOIN算子
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
                "metadata": {"message": f"JOIN返回 {len(joined_rows)} 行"}
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
