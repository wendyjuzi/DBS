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
    t = f"T_PR_{uuid.uuid4().hex[:8]}"; p = f"P_TEST_{uuid.uuid4().hex[:8]}"
    db.execute(f"CREATE TABLE {t} (id INT, name STRING);")
    db.execute(f"CREATE PROCEDURE {p} AS BEGIN INSERT INTO {t}(id,name) VALUES (1,'A'); END;")
    db.execute(f"CALL {p};")
    r = db.execute(f"SELECT name FROM {t};").get("data", [])
    expect(any(x[0]=='A' for x in r), "procedure executed")
    db.execute(f"DROP PROCEDURE {p};")
    db.execute(f"DROP TABLE {t};")
    print("OK: procedures")

if __name__ == "__main__":
    main()


