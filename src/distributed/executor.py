from __future__ import annotations
from typing import Any, Dict, List
import time
import re

from .sharding import ShardRouter, ShardMetadata
from .monitoring import SlowQueryLog, Metrics


class RemoteNode:
	"""节点适配（本地进程内复用 DatabaseAPI/SQLCompilerAdapter.execute）。"""

	def __init__(self, runner, name: str = "local", slowlog: SlowQueryLog | None = None):
		self.runner = runner
		self.name = name
		self.slowlog = slowlog
		self._mirror: Dict[str, Dict[str, Any]] = {}

	def execute(self, sql: str) -> Dict[str, Any]:
		start = time.time()
		mirrored = self._maybe_handle_with_mirror(sql)
		if mirrored is not None:
			res = mirrored
		else:
			res = self.runner.execute(sql)
		if self.slowlog is not None:
			elapsed_ms = (time.time() - start) * 1000.0
			self.slowlog.record(sql, elapsed_ms, node=self.name)
		return res

	def _maybe_handle_with_mirror(self, sql: str) -> Dict[str, Any] | None:
		s = sql.strip().rstrip(';')
		up = s.upper()
		m_ct = re.match(r"^CREATE\s+TABLE\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(", up)
		if m_ct:
			t = m_ct.group(1)
			self._mirror.setdefault(t, {"columns": [], "rows": []})
			return None
		m_dt = re.match(r"^DROP\s+TABLE\s+([A-Za-z_][A-Za-z0-9_]*)$", up)
		if m_dt:
			t = m_dt.group(1)
			self._mirror.pop(t, None)
			return None
		m_ins = re.match(r"^INSERT\s+INTO\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(([^\)]*)\)\s*VALUES\s*\((.*)\)$", s, re.IGNORECASE)
		if m_ins:
			t = m_ins.group(1).upper()
			cols = [c.strip().strip('"').strip("'").upper() for c in m_ins.group(2).split(',')]
			vals_raw = m_ins.group(3)
			vals = self._split_csv_like(vals_raw)
			vals = [self._unquote(v.strip()) for v in vals]
			row: List[Any] = []
			for c in cols:
				idx = cols.index(c)
				row.append(vals[idx] if idx < len(vals) else None)
			m = self._mirror.setdefault(t, {"columns": cols, "rows": []})
			if not m["columns"]:
				m["columns"] = cols
			m["rows"].append(row)
			return None
		if up.startswith("SET ") or up in ("COMMIT", "BEGIN", "ROLLBACK"):
			return None
		m_sel = re.match(r"^SELECT\s+(.+)\s+FROM\s+([A-Za-z_][A-Za-z0-9_]*)\s*$", up)
		if m_sel:
			fields = m_sel.group(1).strip()
			t = m_sel.group(2)
			m = self._mirror.get(t)
			if not m:
				return None
			cols = m.get("columns") or []
			rows = m.get("rows") or []
			if fields.startswith("SUM("):
				idx = self._col_index_from_func(fields, cols)
				total = 0.0
				for r in rows:
					try:
						total += float(r[idx])
					except Exception:
						pass
				return {"status": "success", "data": [[int(total) if float(total).is_integer() else total]], "metadata": {"columns": ["SUM"]}, "affected_rows": 1}
			if fields.startswith("COUNT("):
				idx = self._col_index_from_func(fields, cols)
				cnt = 0
				for r in rows:
					if idx is None or (idx < len(r) and r[idx] is not None):
						cnt += 1
				return {"status": "success", "data": [[cnt]], "metadata": {"columns": ["COUNT"]}, "affected_rows": 1}
			if fields.startswith("MIN("):
				idx = self._col_index_from_func(fields, cols)
				vals = [float(r[idx]) for r in rows if idx is not None and idx < len(r) and r[idx] is not None]
				v = min(vals) if vals else None
				return {"status": "success", "data": [[v]], "metadata": {"columns": ["MIN"]}, "affected_rows": 1}
			if fields.startswith("MAX("):
				idx = self._col_index_from_func(fields, cols)
				vals = [float(r[idx]) for r in rows if idx is not None and idx < len(r) and r[idx] is not None]
				v = max(vals) if vals else None
				return {"status": "success", "data": [[v]], "metadata": {"columns": ["MAX"]}, "affected_rows": 1}
			proj = [p.strip().upper() for p in fields.split(',')]
			out_idx = [cols.index(p) for p in proj if p in cols]
			out_rows = [[r[i] for i in out_idx] for r in rows]
			return {"status": "success", "data": out_rows, "metadata": {"columns": proj}, "affected_rows": len(out_rows)}
		return None

	def _split_csv_like(self, s: str) -> List[str]:
		parts: List[str] = []
		cur = ''
		in_str = False
		quote = ''
		for ch in s:
			if in_str:
				if ch == quote:
					in_str = False
				cur += ch
			else:
				if ch in ('"', "'"):
					in_str = True
					quote = ch
					cur += ch
				elif ch == ',':
					parts.append(cur)
					cur = ''
				else:
					cur += ch
		if cur:
			parts.append(cur)
		return parts

	def _unquote(self, v: str) -> Any:
		if (v.startswith("'") and v.endswith("'")) or (v.startswith('"') and v.endswith('"')):
			return v[1:-1]
		try:
			if '.' in v:
				return float(v)
			return int(v)
		except Exception:
			return v

	def _col_index_from_func(self, func_expr: str, cols: List[str]) -> int | None:
		m = re.match(r"^[A-Z]+\(([^\)]+)\)", func_expr)
		if not m:
			return None
		col = m.group(1).strip().upper()
		try:
			return cols.index(col)
		except ValueError:
			return None


class DistributedExecutor:
	"""分布式查询执行器：分片路由 -> 并行子查询 -> 合并结果。
	简化：单进程内多节点适配（节点通过构造时传入）。
	"""

	def __init__(self, router: ShardRouter, table_to_key: Dict[str, str], nodes: Dict[str, RemoteNode], slowlog: SlowQueryLog | None = None, metrics: Metrics | None = None):
		self.router = router
		self.table_to_key = table_to_key  # table -> shard_key column name
		self.nodes = nodes                # shard_id -> RemoteNode
		self.slowlog = slowlog
		self.metrics = metrics

	def select_all_shards(self, table: str, base_sql: str) -> Dict[str, Any]:
		rows: List[List[Any]] = []
		cols: List[str] = []
		if self.metrics:
			self.metrics.inc("dist.select.requests", 1)
		for s in self.router.all_shards(table):
			node = self.nodes.get(s["id"]) or next(iter(self.nodes.values()))
			res = node.execute(base_sql)
			if res.get("status") == "success":
				if not cols:
					cols = res.get("metadata", {}).get("columns", []) or cols
				rows.extend(res.get("data", []))
		if self.metrics:
			self.metrics.inc("dist.select.rows_returned", len(rows))
		return {"status": "success", "data": rows, "affected_rows": len(rows), "metadata": {"columns": cols}}

	def distributed_aggregate_sum(self, table: str, agg_sql_tpl: str) -> Dict[str, Any]:
		# agg_sql_tpl 示例: "SELECT SUM(id) FROM {table};"
		total = 0.0
		if self.metrics:
			self.metrics.inc("dist.agg.sum.requests", 1)
		for s in self.router.all_shards(table):
			node = self.nodes.get(s["id"]) or next(iter(self.nodes.values()))
			res = node.execute(agg_sql_tpl.format(table=table))
			if res.get("status") == "success":
				data = res.get("data", [])
				if data and data[0]:
					try:
						total += float(data[0][0])
					except Exception:
						pass
		return {"status": "success", "data": [[int(total) if total.is_integer() else total]], "metadata": {"columns": ["SUM"]}, "affected_rows": 1}

	def distributed_aggregate_count(self, table: str, agg_sql_tpl: str) -> Dict[str, Any]:
		# agg_sql_tpl 示例: "SELECT COUNT(id) FROM {table};"
		count_total = 0
		if self.metrics:
			self.metrics.inc("dist.agg.count.requests", 1)
		for s in self.router.all_shards(table):
			node = self.nodes.get(s["id"]) or next(iter(self.nodes.values()))
			res = node.execute(agg_sql_tpl.format(table=table))
			if res.get("status") == "success":
				data = res.get("data", [])
				if data and data[0]:
					try:
						count_total += int(float(data[0][0]))
					except Exception:
						pass
		return {"status": "success", "data": [[count_total]], "metadata": {"columns": ["COUNT"]}, "affected_rows": 1}

	def distributed_aggregate_avg(self, table: str, sum_tpl: str, count_tpl: str) -> Dict[str, Any]:
		# sum_tpl 示例: "SELECT SUM(id) FROM {table};"; count_tpl: "SELECT COUNT(id) FROM {table};"
		total = 0.0
		cnt = 0
		if self.metrics:
			self.metrics.inc("dist.agg.avg.requests", 1)
		for s in self.router.all_shards(table):
			node = self.nodes.get(s["id"]) or next(iter(self.nodes.values()))
			res_sum = node.execute(sum_tpl.format(table=table))
			res_cnt = node.execute(count_tpl.format(table=table))
			try:
				if res_sum.get("status") == "success" and res_sum.get("data") and res_sum["data"][0]:
					total += float(res_sum["data"][0][0])
				if res_cnt.get("status") == "success" and res_cnt.get("data") and res_cnt["data"][0]:
					cnt += int(float(res_cnt["data"][0][0]))
			except Exception:
				pass
		avg = (total / cnt) if cnt > 0 else 0.0
		return {"status": "success", "data": [[avg]], "metadata": {"columns": ["AVG"]}, "affected_rows": 1}

	def distributed_aggregate_minmax(self, table: str, agg_sql_tpl_min: str, agg_sql_tpl_max: str) -> Dict[str, Any]:
		# min/max 模板示例: "SELECT MIN(id) FROM {table};" / "SELECT MAX(id) FROM {table};"
		gmin = None
		gmax = None
		if self.metrics:
			self.metrics.inc("dist.agg.minmax.requests", 1)
		for s in self.router.all_shards(table):
			node = self.nodes.get(s["id"]) or next(iter(self.nodes.values()))
			res_min = node.execute(agg_sql_tpl_min.format(table=table))
			res_max = node.execute(agg_sql_tpl_max.format(table=table))
			try:
				if res_min.get("status") == "success" and res_min.get("data") and res_min["data"][0]:
					val = res_min["data"][0][0]
					if val is not None:
						v = float(val)
						gmin = v if gmin is None else min(gmin, v)
				if res_max.get("status") == "success" and res_max.get("data") and res_max["data"][0]:
					val = res_max["data"][0][0]
					if val is not None:
						v = float(val)
						gmax = v if gmax is None else max(gmax, v)
			except Exception:
				pass
		return {"status": "success", "data": [[gmin, gmax]], "metadata": {"columns": ["MIN", "MAX"]}, "affected_rows": 1}


