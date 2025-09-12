from typing import Dict, List, DefaultDict
from collections import defaultdict
import bisect


class IndexManager:
    """
    简易二级索引管理器（内存级）
    - 支持为 (table, column) 建立到主键列 pk_column 的倒排索引
    - 仅支持等值查询加速：value -> [pk_values]
    - 持久化暂无，进程内有效
    """

    def __init__(self):
        # key: (table, column) -> value_to_pks 映射
        self._indexes: Dict[tuple, DefaultDict[str, List[str]]] = {}
        # 记录每个索引绑定的主键列
        self._pk_column_of_index: Dict[tuple, str] = {}
        # 维护每个索引的已排序键列表（用于范围查询，简易实现）
        self._sorted_keys: Dict[tuple, List[str]] = {}

    def create_index(self, table: str, column: str, pk_column: str) -> bool:
        key = (table, column)
        if key in self._indexes:
            return False
        self._indexes[key] = defaultdict(list)
        self._pk_column_of_index[key] = pk_column
        self._sorted_keys[key] = []
        return True

    def drop_index(self, table: str, column: str) -> bool:
        key = (table, column)
        existed = key in self._indexes
        self._indexes.pop(key, None)
        self._pk_column_of_index.pop(key, None)
        self._sorted_keys.pop(key, None)
        return existed

    def has_index(self, table: str, column: str) -> bool:
        return (table, column) in self._indexes

    def get_indexes(self) -> List[dict]:
        out = []
        for (t, c), pkc in self._pk_column_of_index.items():
            out.append({"table": t, "column": c, "pk_column": pkc})
        return out

    def on_insert(self, table: str, row_values: List[str], column_names: List[str]):
        # 将该行反映射到所有与该表相关的索引
        if not column_names:
            return
        try:
            # 找到主键值
            for (t, col), pk_col in list(self._pk_column_of_index.items()):
                if t != table:
                    continue
                if col not in column_names or pk_col not in column_names:
                    continue
                col_idx = column_names.index(col)
                pk_idx = column_names.index(pk_col)
                if col_idx < len(row_values) and pk_idx < len(row_values):
                    val = str(row_values[col_idx])
                    pk = str(row_values[pk_idx])
                    idx = self._indexes.get((t, col))
                    if idx is not None:
                        # 避免重复 pk
                        if pk not in idx[val]:
                            idx[val].append(pk)
                            # 维护排序键（仅在新值首次出现时插入）
                            if (t, col) in self._sorted_keys and len(idx[val]) == 1:
                                keys = self._sorted_keys[(t, col)]
                                # 简易地按字符串顺序插入
                                pos = bisect.bisect_left(keys, val)
                                if pos >= len(keys) or keys[pos] != val:
                                    keys.insert(pos, val)
        except Exception:
            pass

    def on_delete(self, table: str, pk_value: str) -> None:
        """根据主键值从该表所有索引移除映射。"""
        try:
            for (t, col), idx in list(self._indexes.items()):
                if t != table:
                    continue
                lst = idx.get("*scan*", None)  # 占位以保证类型
                # 遍历所有 value -> [pks]
                for v, pks in list(idx.items()):
                    if pk_value in pks:
                        pks.remove(pk_value)
                        if not pks:
                            # 从排序键中移除
                            keys = self._sorted_keys.get((t, col))
                            if keys and v in keys:
                                try:
                                    keys.remove(v)
                                except ValueError:
                                    pass
                            del idx[v]
        except Exception:
            pass

    def on_update(self, table: str, old_row: List[str], new_row: List[str], column_names: List[str]):
        """更新索引：删除旧值映射，插入新值映射。"""
        try:
            if not column_names:
                return
            # 找到所有绑定该表的索引
            for (t, col), pk_col in list(self._pk_column_of_index.items()):
                if t != table:
                    continue
                if col not in column_names or pk_col not in column_names:
                    continue
                col_idx = column_names.index(col)
                pk_idx = column_names.index(pk_col)
                if col_idx < len(old_row) and pk_idx < len(old_row):
                    old_val = str(old_row[col_idx])
                    pk = str(old_row[pk_idx])
                    idx = self._indexes.get((t, col))
                    if idx is not None:
                        # 从旧值列表移除 pk
                        if pk in idx.get(old_val, []):
                            idx[old_val].remove(pk)
                            if not idx[old_val]:
                                # 清理排序键
                                keys = self._sorted_keys.get((t, col))
                                if keys and old_val in keys:
                                    try:
                                        keys.remove(old_val)
                                    except ValueError:
                                        pass
                                del idx[old_val]
                # 插入新值
                if col_idx < len(new_row) and pk_idx < len(new_row):
                    new_val = str(new_row[col_idx])
                    pk = str(new_row[pk_idx])
                    idx = self._indexes.get((t, col))
                    if idx is not None:
                        if pk not in idx[new_val]:
                            idx[new_val].append(pk)
                            if (t, col) in self._sorted_keys and len(idx[new_val]) == 1:
                                keys = self._sorted_keys[(t, col)]
                                pos = bisect.bisect_left(keys, new_val)
                                if pos >= len(keys) or keys[pos] != new_val:
                                    keys.insert(pos, new_val)
        except Exception:
            pass

    def lookup_pks(self, table: str, column: str, value: str) -> List[str]:
        idx = self._indexes.get((table, column))
        if not idx:
            return []
        return list(idx.get(str(value), []))

    def range_lookup_pks(self, table: str, column: str, min_value: str = None, max_value: str = None,
                         include_min: bool = True, include_max: bool = True) -> List[str]:
        key = (table, column)
        idx = self._indexes.get(key)
        keys = self._sorted_keys.get(key)
        if not idx or keys is None:
            return []
        # 计算边界
        left = 0
        right = len(keys)
        if min_value is not None:
            pos = bisect.bisect_left(keys, str(min_value)) if include_min else bisect.bisect_right(keys, str(min_value))
            left = max(left, pos)
        if max_value is not None:
            pos = bisect.bisect_right(keys, str(max_value)) if include_max else bisect.bisect_left(keys, str(max_value))
            right = min(right, pos)
        if left >= right:
            return []
        out: List[str] = []
        for k in keys[left:right]:
            out.extend(idx.get(k, []))
        return out


