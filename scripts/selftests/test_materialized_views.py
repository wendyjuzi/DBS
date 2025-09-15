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
    t = f"T_MV_{uuid.uuid4().hex[:8]}"; mv = f"MV_MV_{uuid.uuid4().hex[:8]}"
    db.execute(f"CREATE TABLE {t} (id INT, name STRING);")
    db.execute(f"INSERT INTO {t} (id, name) VALUES (1, 'A');")
    db.execute(f"CREATE MATERIALIZED VIEW {mv} AS SELECT id, name FROM {t};")
    # 刷新
    db.execute(f"REFRESH MATERIALIZED VIEW {mv};")
    # 物化视图底表是 __mat_<name>，读取检查
    phys = f"__mat_{mv}"
    r = db.execute(f"SELECT name FROM {phys};").get("data", [])
    expect(any(x[0]=='A' for x in r), "mat view data present")
    db.execute(f"DROP MATERIALIZED VIEW {mv};")
    db.execute(f"DROP TABLE {t};")
    print("OK: materialized views")

if __name__ == "__main__":
    main()


