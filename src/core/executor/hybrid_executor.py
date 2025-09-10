"""
混合架构执行引擎 - Python调度C++算子
"""

import time
from typing import Any, Dict, List, Optional, Callable
from ...utils.exceptions import ExecutionError, CatalogError


class HybridExecutionEngine:
    """混合架构执行引擎，Python调度C++算子"""

    def __init__(self, cpp_storage_engine, cpp_execution_engine):
        """
        初始化混合执行引擎
        
        Args:
            cpp_storage_engine: C++存储引擎实例
            cpp_execution_engine: C++执行引擎实例
        """
        self.storage = cpp_storage_engine
        self.executor = cpp_execution_engine
        # 元数据缓存（表名→列名列表，避免重复调用C++）
        self.table_columns = {}

    def _ensure_table_cached(self, table_name: str) -> None:
        """当缓存中缺少表结构时，从存储引擎加载一次。"""
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
        """
        执行查询计划
        
        Args:
            plan: 查询计划字典
            
        Returns:
            执行结果字典
        """
        start_time = time.time()
        print(f"[EXEC] 收到计划: {plan}")
        
        try:
            plan_type = plan.get("type")
            
            if plan_type == "CREATE_TABLE":
                result = self._execute_create_table(plan)
            elif plan_type == "INSERT":
                result = self._execute_insert(plan)
            elif plan_type == "SELECT":
                result = self._execute_select(plan)
            elif plan_type == "DELETE":
                result = self._execute_delete(plan)
            else:
                raise ExecutionError(f"不支持的查询计划类型: {plan_type}")
            
            execution_time = time.time() - start_time
            result["execution_time"] = execution_time
            print(f"[EXEC] 执行完成, 用时 {execution_time:.4f}s, 结果概要: affected_rows={result.get('affected_rows')}, data_len={len(result.get('data', []))}")
            
            return result
            
        except Exception as e:
            if isinstance(e, (ExecutionError, CatalogError)):
                print(f"[EXEC] 业务错误: {e}")
                raise
            else:
                print(f"[EXEC] 未预期错误: {e}")
                raise ExecutionError(f"执行查询时发生错误: {str(e)}")

    def _execute_create_table(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """执行CREATE TABLE计划"""
        table_name = plan["table"]
        columns_def = plan["columns"]
        print(f"[EXEC] CREATE TABLE {table_name} cols={columns_def}")
        
        # 转换列定义为C++格式
        cpp_columns = []
        for col_def in columns_def:
            from db_core import Column, DataType
            
            # 映射数据类型
            if col_def["type"] == "INT":
                col_type = DataType.INT
            elif col_def["type"] == "STRING":
                col_type = DataType.STRING
            elif col_def["type"] == "DOUBLE":
                col_type = DataType.DOUBLE
            else:
                raise ExecutionError(f"不支持的数据类型: {col_def['type']}")
            
            cpp_columns.append(Column(
                col_def["name"],
                col_type,
                col_def.get("is_primary_key", False)
            ))
        
        # 调用C++执行引擎
        success = self.executor.create_table(table_name, cpp_columns)
        print(f"[EXEC] CREATE 调用C++返回: {success}")
        
        if success:
            # 更新元数据缓存
            names = [col["name"] for col in columns_def]
            self.table_columns[table_name] = names
            return {
                "affected_rows": 0,
                "metadata": {"message": f"表 '{table_name}' 创建成功"}
            }
        else:
            raise ExecutionError(f"表 '{table_name}' 已存在")

    def _execute_insert(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """执行INSERT计划"""
        table_name = plan["table"]
        values = plan["values"]
        print(f"[EXEC] INSERT {table_name} values={values}")
        
        # 校验表是否存在
        if table_name not in self.table_columns:
            self._ensure_table_cached(table_name)
        if table_name not in self.table_columns:
            raise CatalogError(f"表 '{table_name}' 不存在")
        
        # 校验列数匹配
        expected_cols = len(self.table_columns[table_name])
        if len(values) != expected_cols:
            raise ExecutionError(f"列数不匹配，期望 {expected_cols} 列，实际 {len(values)} 列")
        
        # 调用C++执行引擎
        success = self.executor.insert(table_name, values)
        print(f"[EXEC] INSERT 调用C++返回: {success}")
        
        if success:
            return {"affected_rows": 1}
        else:
            raise ExecutionError("插入失败（行数据过大或存储空间不足）")

    def _execute_select(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """执行SELECT计划（支持优化器的 access_method/filter/project）"""
        table_name = plan["table"]
        target_columns = plan.get("columns", ["*"])
        where_clause = plan.get("where")
        filter_conditions = plan.get("filter")  # 优化器提供的条件列表：[{column, op, value}, ...]
        access_method = plan.get("access_method")
        access_params = plan.get("access_params", {})
        print(f"[EXEC] SELECT {table_name} cols={target_columns} where={where_clause} access={access_method} params={access_params}")
        
        # 校验表是否存在
        if table_name not in self.table_columns:
            self._ensure_table_cached(table_name)
        if table_name not in self.table_columns:
            raise CatalogError(f"表 '{table_name}' 不存在")
        
        # 处理目标列（* → 所有列）
        if target_columns == ["*"]:
            target_columns = self.table_columns[table_name]
        
        # 构建过滤函数
        if filter_conditions:
            predicate = self._build_predicate_from_conditions(table_name, filter_conditions)
        else:
            predicate = self._build_predicate(table_name, where_clause)
        
        # 调用C++算子：根据access_method选择路径
        if access_method and getattr(access_method, "value", access_method) in ("index_scan", "index_range_scan"):
            # 索引路径
            rows = []
            try:
                method_str = access_method.value if hasattr(access_method, 'value') else access_method
                if method_str == "index_scan":
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
            # 对索引结果应用过滤（非覆盖索引、或存在额外条件）
            filtered_rows = []
            for r in rows:
                if predicate(r.get_values()):
                    filtered_rows.append(r)
            if not filtered_rows:
                print("[EXEC] 索引未命中或过滤后为空，回退顺扫+过滤")
                scanned_rows = self.executor.seq_scan(table_name)
                filtered_rows = []
                for r in scanned_rows:
                    if predicate(r.get_values()):
                        filtered_rows.append(r)
        else:
            # 全表扫描
            scanned_rows = self.executor.seq_scan(table_name)
            print(f"[EXEC] SEQ 扫描返回 {len(scanned_rows)} 行")
            try:
                filtered_rows = self.executor.filter(table_name, predicate)
            except Exception as e:
                print(f"[EXEC] C++ filter 不可用，改用 Python 过滤: {e}")
                filtered_rows = []
                for r in scanned_rows:
                    if predicate(r.get_values()):
                        filtered_rows.append(r)
            print(f"[EXEC] 过滤后 {len(filtered_rows)} 行")
        
        # 投影
        try:
            projected_data = self.executor.project(table_name, filtered_rows, target_columns)
        except Exception:
            projected_data = [r.get_values() for r in filtered_rows]
        print(f"[EXEC] 投影列 {target_columns} -> 返回 {len(projected_data)} 行")
        
        return {
            "data": projected_data,
            "affected_rows": len(projected_data),
            "metadata": {"columns": target_columns}
        }

    def _execute_delete(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """执行DELETE计划"""
        table_name = plan["table"]
        where_clause = plan.get("where")
        
        # 校验表是否存在
        if table_name not in self.table_columns:
            self._ensure_table_cached(table_name)
        if table_name not in self.table_columns:
            raise CatalogError(f"表 '{table_name}' 不存在")
        
        # 构建过滤函数
        predicate = self._build_predicate(table_name, where_clause)
        
        # 调用C++执行引擎
        deleted_count = self.executor.delete_rows(table_name, predicate)
        
        return {
            "affected_rows": deleted_count,
            "metadata": {"message": f"删除了 {deleted_count} 行"}
        }

    def _build_predicate(self, table_name: str, where_clause: Optional[str]) -> Callable[[List[str]], bool]:
        """
        将WHERE条件字符串转为C++可调用的过滤函数
        """
        if not where_clause:
            return lambda x: True  # 无WHERE条件，返回所有行

        # 获取表的列信息
        col_names = self.table_columns.get(table_name, [])
        
        # 替换列名为Row.values的下标（如"age > 18" → "x[1] > 18"）
        processed_clause = where_clause
        for col_idx, col_name in enumerate(col_names):
            if col_name in processed_clause:
                processed_clause = processed_clause.replace(f"{col_name} ", f"x[{col_idx}] ")
                processed_clause = processed_clause.replace(f" {col_name}", f" x[{col_idx}]")
                processed_clause = processed_clause.replace(f"'{col_name}'", f"x[{col_idx}]")
        
        try:
            predicate = eval(f"lambda x: {processed_clause}")
            return predicate
        except Exception as e:
            raise ExecutionError(f"WHERE条件解析错误: {str(e)}")

    def _build_predicate_from_conditions(self, table_name: str, conditions: List[Dict[str, Any]]):
        """将优化器给出的条件列表构造成可被 C++ filter 调用的 Python 谓词。"""
        col_names = self.table_columns.get(table_name, [])

        def predicate(values: List[str]) -> bool:
            for cond in conditions:
                col = cond.get("column")
                op = cond.get("op")
                val = cond.get("value")
                try:
                    idx = col_names.index(col) if col in col_names else -1
                except Exception:
                    idx = -1
                if idx < 0 or idx >= len(values):
                    return False
                left = values[idx]
                # 尝试数值比较
                def to_num(s: str):
                    try:
                        if "." in s:
                            return float(s)
                        return int(s)
                    except Exception:
                        return None
                lnum = to_num(left)
                rnum = to_num(str(val))
                if lnum is not None and rnum is not None:
                    if op == "=":
                        ok = (lnum == rnum)
                    elif op == ">":
                        ok = (lnum > rnum)
                    elif op == "<":
                        ok = (lnum < rnum)
                    elif op == ">=":
                        ok = (lnum >= rnum)
                    elif op == "<=":
                        ok = (lnum <= rnum)
                    elif op == "!=":
                        ok = (lnum != rnum)
                    else:
                        ok = False
                else:
                    # 字符串比较
                    if op == "=":
                        ok = (left == str(val))
                    elif op == ">":
                        ok = (left > str(val))
                    elif op == "<":
                        ok = (left < str(val))
                    elif op == ">=":
                        ok = (left >= str(val))
                    elif op == "<=":
                        ok = (left <= str(val))
                    elif op == "!=":
                        ok = (left != str(val))
                    else:
                        ok = False
                if not ok:
                    return False
            return True

        return predicate
