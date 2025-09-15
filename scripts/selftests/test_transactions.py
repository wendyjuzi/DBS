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
    t = f"T_TX_{uuid.uuid4().hex[:8]}"
    # 使用唯一表名，无需预先清理
    db.execute(f"CREATE TABLE {t} (id INT, v STRING);")

    db.execute("BEGIN;")
    db.execute(f"INSERT INTO {t} (id, v) VALUES (1, 'X');")
    db.execute("ROLLBACK;")
    res = db.execute(f"SELECT id, v FROM {t};")
    expect(not any(r[0]==1 for r in res.get("data", [])), "rollback visible check")

    db.execute("BEGIN;")
    db.execute(f"INSERT INTO {t} (id, v) VALUES (2, 'Y');")
    db.execute("COMMIT;")
    res = db.execute(f"SELECT id, v FROM {t};")
    expect(any(r[0]==2 and r[1]=='Y' for r in res.get("data", [])), "commit visible check")

    db.execute(f"DROP TABLE {t};")
    print("OK: transactions")

if __name__ == "__main__":
    main()


