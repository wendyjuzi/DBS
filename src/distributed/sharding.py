from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple


class ShardMetadata:
	"""分片元数据：记录每个逻辑表的分片策略与片段。
	结构: table -> {
	  'strategy': 'RANGE'|'HASH',
	  'shards': [ {'id': 's1', 'range': [min,max]}, ... ]  # RANGE
	  或 'shards': [ {'id': 's0'}, {'id': 's1'}, ... ]    # HASH (取模到索引)
	}
	"""

	def __init__(self) -> None:
		self._meta: Dict[str, Dict[str, Any]] = {}

	def create_range_shards(self, table: str, ranges: List[Tuple[str, str]]) -> None:
		self._meta[table] = {
			"strategy": "RANGE",
			"shards": [ {"id": f"{table}_r{i}", "range": [r[0], r[1]]} for i, r in enumerate(ranges) ]
		}

	def create_hash_shards(self, table: str, num: int) -> None:
		self._meta[table] = {
			"strategy": "HASH",
			"shards": [ {"id": f"{table}_h{i}"} for i in range(max(1, int(num)))]
		}

	def get(self, table: str) -> Optional[Dict[str, Any]]:
		return self._meta.get(table)

	def list_shards(self, table: str) -> List[Dict[str, Any]]:
		m = self.get(table) or {}
		return list(m.get("shards", []))


class ShardRouter:
	"""分片路由：根据策略与键值定位目标分片。"""

	def __init__(self, meta: ShardMetadata) -> None:
		self.meta = meta

	def locate_by_value(self, table: str, shard_key_value: str) -> List[Dict[str, Any]]:
		m = self.meta.get(table)
		if not m:
			return []
		if m.get("strategy") == "RANGE":
			out = []
			for s in m.get("shards", []):
				mn, mx = s.get("range", [None, None])
				if (mn is None or shard_key_value >= mn) and (mx is None or shard_key_value < mx):
					out.append(s)
					break
				# 未命中也支持返回空
			return out
		if m.get("strategy") == "HASH":
			shards = m.get("shards", [])
			if not shards:
				return []
			idx = (hash(str(shard_key_value)) & 0x7FFFFFFF) % len(shards)
			return [shards[idx]]
		return []

	def all_shards(self, table: str) -> List[Dict[str, Any]]:
		return self.meta.list_shards(table)


