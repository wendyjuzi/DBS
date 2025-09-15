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
    t = f"T_VB_{uuid.uuid4().hex[:8]}"; v = f"V_VB_{uuid.uuid4().hex[:8]}"
    db.execute(f"CREATE TABLE {t} (id INT, name STRING);")
    db.execute(f"INSERT INTO {t} (id, name) VALUES (1, 'A');")
    db.execute(f"INSERT INTO {t} (id, name) VALUES (2, 'B');")
    db.execute(f"CREATE VIEW {v} AS SELECT id, name FROM {t};")
    r1 = db.execute(f"SELECT * FROM {v};").get("data", [])
    expect(len(r1)>=2, "view select *")
    r2 = db.execute(f"SELECT name FROM {v};").get("data", [])
    expect(set(x[0] for x in r2)=={'A','B'}, "view projection")
    db.execute(f"DROP VIEW {v};")
    db.execute(f"DROP TABLE {t};")
    print("OK: views")

if __name__ == "__main__":
    main()


