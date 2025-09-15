#!/usr/bin/env python3
import sys
from pathlib import Path

root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(root))

from src.api.sql_compiler_adapter import SQLCompilerAdapter


def expect(ok: bool, msg: str):
    print(("[OK] " if ok else "[FAIL] ") + msg)
    if not ok:
        sys.exit(1)


def main():
    db = SQLCompilerAdapter()
    import uuid
    t = f"T_AB_{uuid.uuid4().hex[:8]}"
    # 唯一表名避免残留冲突
    try:
        db.execute(f"DROP TABLE {t};")
    except Exception:
        pass

    db.execute(f"CREATE TABLE {t} (id INT, name STRING);")
    db.execute(f"INSERT INTO {t} (id, name) VALUES (1, 'A');")
    db.execute(f"INSERT INTO {t} (id, name) VALUES (2, 'B');")
    res = db.execute(f"SELECT id, name FROM {t};")
    rows = res.get("data", [])
    expect(any(r[0]==1 and r[1]=='A' for r in rows), "adapter basic insert/select A")
    expect(any(r[0]==2 and r[1]=='B' for r in rows), "adapter basic insert/select B")
    db.execute(f"DROP TABLE {t};")
    print("OK: adapter basic")

if __name__ == "__main__":
    main()


