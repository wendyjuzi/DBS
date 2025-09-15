from __future__ import annotations
from typing import Any, Dict, List, Callable


class MiniZK:
	"""极简协调服务(内存)：
	- 成员注册/心跳
	- 配置键值存储与watch
	"""

	def __init__(self) -> None:
		self.members: Dict[str, Dict[str, Any]] = {}
		self.kv: Dict[str, Any] = {}
		self.watchers: Dict[str, List[Callable[[str, Any], None]]] = {}

	def register(self, node_id: str, meta: Dict[str, Any]) -> None:
		self.members[node_id] = dict(meta or {})
		self._notify_members_watchers()

	def heartbeat(self, node_id: str) -> None:
		m = self.members.get(node_id)
		if m is not None:
			m['last_beat'] = m.get('last_beat', 0) + 1
			self._notify_members_watchers()

	def set_config(self, key: str, value: Any) -> None:
		self.kv[key] = value
		for cb in self.watchers.get(key, []) or []:
			try:
				cb(key, value)
			except Exception:
				pass

	def get_config(self, key: str) -> Any:
		return self.kv.get(key)

	def watch(self, key: str, cb: Callable[[str, Any], None]) -> None:
		self.watchers.setdefault(key, []).append(cb)

	# 便捷API，兼容示例脚本
	def add_member(self, node_id: str) -> None:
		self.register(node_id, {})

	def list_members(self) -> List[str]:
		return list(self.members.keys())

	def watch_members(self, cb: Callable[[], None]) -> None:
		# 使用特殊键 '__members__' 作为成员变更watch
		self.watchers.setdefault('__members__', []).append(lambda _k, _v: cb())

	def get_all_config(self) -> Dict[str, Any]:
		return dict(self.kv)

	def _notify_members_watchers(self) -> None:
		for cb in self.watchers.get('__members__', []) or []:
			try:
				cb('__members__', self.list_members())
			except Exception:
				pass


