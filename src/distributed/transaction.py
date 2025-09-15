from __future__ import annotations
from typing import Any, Dict, List


class TwoPhaseCoordinator:
	"""两阶段提交(2PC)协调器——最小骨架实现。
	- prepare: 向各参与者发起预提交，全部ok则commit，否则rollback
	- 简化：参与者接口需要实现 prepare(txid) / commit(txid) / rollback(txid)
	"""

	def __init__(self, participants: List[Any]):
		self.participants = list(participants or [])

	def begin(self) -> str:
		# 简化txid
		import uuid
		return str(uuid.uuid4())

	def commit(self, txid: str) -> bool:
		# 1. Prepare 阶段
		for p in self.participants:
			try:
				ok = getattr(p, "prepare")(txid)
				if not ok:
					raise RuntimeError("prepare failed")
			except Exception:
				self._rollback_all(txid)
				return False
		# 2. Commit 阶段
		all_ok = True
		for p in self.participants:
			try:
				ok = getattr(p, "commit")(txid)
				all_ok = all_ok and bool(ok)
			except Exception:
				all_ok = False
		return all_ok

	def rollback(self, txid: str) -> None:
		self._rollback_all(txid)

	def _rollback_all(self, txid: str) -> None:
		for p in self.participants:
			try:
				getattr(p, "rollback")(txid)
			except Exception:
				pass


