from __future__ import annotations
from typing import Any, Dict, List
import time


class SlowQueryLog:
	def __init__(self, threshold_ms: int = 100):
		self.threshold = threshold_ms
		self.logs: List[Dict[str, Any]] = []

	def record(self, sql: str, elapsed_ms: float, node: str = "local") -> None:
		if elapsed_ms >= self.threshold:
			self.logs.append({"sql": sql, "elapsed_ms": elapsed_ms, "node": node, "ts": time.time()})

	def list(self) -> List[Dict[str, Any]]:
		return list(self.logs)


class Metrics:
	def __init__(self) -> None:
		self.counters: Dict[str, int] = {}
		self.gauges: Dict[str, float] = {}

	def inc(self, name: str, by: int = 1) -> None:
		self.counters[name] = self.counters.get(name, 0) + by

	def set(self, name: str, value: float) -> None:
		self.gauges[name] = float(value)

	def snapshot(self) -> Dict[str, Any]:
		return {"counters": dict(self.counters), "gauges": dict(self.gauges)}


