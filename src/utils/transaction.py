"""
极简事务管理器：提供 begin/commit/rollback，与 WAL 协同。
限制：单线程/单进程教学用途；不实现并发控制与锁。
"""

from __future__ import annotations

import time
import uuid
from typing import Dict, Optional

from .wal import WALManager, LogRecord


class TransactionState:
    ACTIVE = "ACTIVE"
    COMMITTED = "COMMITTED"
    ABORTED = "ABORTED"


class Transaction:
    def __init__(self, txid: Optional[str] = None) -> None:
        self.txid = txid or str(uuid.uuid4())
        self.state = TransactionState.ACTIVE
        self.start_ts = time.time()


class TransactionManager:
    def __init__(self, wal: WALManager) -> None:
        self.wal = wal
        self._txns: Dict[str, Transaction] = {}
        self._active: set[str] = set()

    def begin(self) -> Transaction:
        txn = Transaction()
        self._txns[txn.txid] = txn
        self.wal.append(LogRecord(txid=txn.txid, op="BEGIN"))
        self._active.add(txn.txid)
        return txn

    def commit(self, txid: str) -> None:
        txn = self._ensure(txid)
        if txn.state != TransactionState.ACTIVE:
            return
        self.wal.append(LogRecord(txid=txid, op="COMMIT"))
        txn.state = TransactionState.COMMITTED
        self._active.discard(txid)

    def rollback(self, txid: str) -> None:
        txn = self._ensure(txid)
        if txn.state != TransactionState.ACTIVE:
            return
        # 简化：仅写 ABORT 标记，不做物化回滚
        self.wal.append(LogRecord(txid=txid, op="ABORT"))
        txn.state = TransactionState.ABORTED
        self._active.discard(txid)

    # --- helpers ---
    def _ensure(self, txid: str) -> Transaction:
        if txid not in self._txns:
            self._txns[txid] = Transaction(txid)
        return self._txns[txid]

    # --- observability ---
    def get_active_txids(self) -> set[str]:
        return set(self._active)


