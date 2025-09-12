"""
极简顺序写前日志（WAL）实现：仅用于插入/更新/删除的重做日志。

设计：
- 仅提供 redo 日志（不实现 undo/物化撤销）。
- 供单进程教学用途：每条日志一行 JSON；追加写入；周期 fsync。
- 支持事务边界：BEGIN/COMMIT/ABORT；崩溃恢复按事务提交状态重做。
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Optional


@dataclass(frozen=True)
class LogRecord:
    txid: str
    op: str  # BEGIN|COMMIT|ABORT|INSERT|UPDATE|DELETE
    table: Optional[str] = None
    payload: Optional[Dict[str, Any]] = None

    def to_json(self) -> str:
        return json.dumps({
            "txid": self.txid,
            "op": self.op,
            "table": self.table,
            "payload": self.payload or {},
        }, ensure_ascii=False)


class WALManager:
    """极简 WAL 管理器：顺序日志文件，提供追加、刷盘与遍历。"""

    def __init__(self, log_dir: str | Path = "logs", file_name: str = "wal.log") -> None:
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.path = self.log_dir / file_name
        # 打开文件句柄（追加模式，二进制）
        self.fd: Optional[int] = None
        self._open_append()

    def _open_append(self) -> None:
        flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND
        # 使用系统默认权限
        self.fd = os.open(self.path, flags)

    def append(self, record: LogRecord, sync: bool = True) -> None:
        if self.fd is None:
            self._open_append()
        data = (record.to_json() + "\n").encode("utf-8")
        assert self.fd is not None
        os.write(self.fd, data)
        if sync:
            os.fsync(self.fd)

    def flush(self) -> None:
        if self.fd is not None:
            os.fsync(self.fd)

    def close(self) -> None:
        if self.fd is not None:
            try:
                os.close(self.fd)
            finally:
                self.fd = None

    def iterate(self) -> Iterable[LogRecord]:
        if not self.path.exists():
            return []
        with self.path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    yield LogRecord(
                        txid=obj.get("txid", ""),
                        op=obj.get("op", ""),
                        table=obj.get("table"),
                        payload=obj.get("payload") or {},
                    )
                except Exception:
                    # 跳过损坏行
                    continue


class Recovery:
    """基于 WAL 的简化恢复：重做已提交事务，忽略未提交。"""

    def __init__(self, wal: WALManager):
        self.wal = wal

    def analyze_committed(self) -> set[str]:
        active: set[str] = set()
        committed: set[str] = set()
        for rec in self.wal.iterate():
            if rec.op == "BEGIN":
                active.add(rec.txid)
            elif rec.op == "COMMIT":
                committed.add(rec.txid)
                active.discard(rec.txid)
            elif rec.op == "ABORT":
                active.discard(rec.txid)
        return committed


