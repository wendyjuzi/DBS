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
    t = f"T_IDX_{uuid.uuid4().hex[:8]}"
    # 唯一表名，无需预清理
    db.execute(f"CREATE TABLE {t} (id INT, v STRING);")
    db.execute(f"CREATE INDEX idx ON {t}(id) USING HASH PK id;")
    res = db.execute("SHOW INDEXES;")
    rows = res.get("data", [])
    expect(any(r[0]==t and r[1]=='id' for r in rows), "index created and listed")
    db.execute(f"DROP INDEX {t}(id);")
    db.execute(f"DROP TABLE {t};")
    print("OK: indexes")

if __name__ == "__main__":
    main()


