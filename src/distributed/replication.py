from __future__ import annotations
from typing import Any, Dict, List
import time
import itertools


class ReplicatedCluster:
	"""主从复制（最终一致性）与读写分离的最小实现。
	- 写入：先写主库，异步推送到从库
	- 读取：可选走从库（可能稍有延迟）或强一致读主库
	"""

	def __init__(self, primary, replicas: List):
		self.primary = primary
		self.replicas = list(replicas or [])
		self._lag_ms = 50  # 模拟网络/复制延迟
		self._rr_iter = itertools.cycle(range(len(self.replicas))) if self.replicas else None
		self._last_heartbeat_ok = True
		self._hb_interval_ms = 500
		self._last_hb_ts = 0.0

	def execute_write(self, sql: str) -> Dict[str, Any]:
		res = self.primary.execute(sql)
		if res.get("status") == "success":
			# 异步复制（简化：同步sleep模拟延迟后推送）
			for r in self.replicas:
				try:
					time.sleep(self._lag_ms/1000.0)
					r.execute(sql)
				except Exception:
					pass
		return res

	def execute_read(self, sql: str, read_from_replicas: bool = True) -> Dict[str, Any]:
		# 心跳探活与主库健康检查
		now = time.time()
		if (now - self._last_hb_ts) * 1000.0 >= self._hb_interval_ms:
			self._last_hb_ts = now
			self._last_heartbeat_ok = self._heartbeat()
		if not self._last_heartbeat_ok:
			# 主库不可用，尝试故障切换（选择第一个健康副本为主）
			for i, r in enumerate(self.replicas):
				if self._probe(r):
					self.primary, self.replicas[i] = r, self.primary
					# 重新构建轮询迭代器
					self._rr_iter = itertools.cycle(range(len(self.replicas))) if self.replicas else None
					break
			self._last_heartbeat_ok = True

		if read_from_replicas and self.replicas:
			# 轮询负载均衡
			idx = next(self._rr_iter) if self._rr_iter else 0
			idx = idx % len(self.replicas)
			try:
				return self.replicas[idx].execute(sql)
			except Exception:
				# 回退到主库
				pass
		return self.primary.execute(sql)

	def _heartbeat(self) -> bool:
		return self._probe(self.primary)

	def _probe(self, node) -> bool:
		try:
			res = node.execute("SELECT 1;")
			return bool(res and res.get("status") == "success")
		except Exception:
			return False


