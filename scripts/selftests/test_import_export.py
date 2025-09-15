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
    t = f"T_IO_{uuid.uuid4().hex[:8]}"; out = root / f"_tmp_export_{uuid.uuid4().hex[:8]}.csv"
    db.execute(f"CREATE TABLE {t} (id INT, name STRING);")
    db.execute(f"INSERT INTO {t} (id, name) VALUES (1, 'A');")
    ok = db.export_table(t, "csv", str(out))
    expect(ok and out.exists(), "export csv")
    # 导入到新表
    t2 = f"T_IO2_{uuid.uuid4().hex[:8]}"
    # 走适配器导入接口
    db.import_table(t2, "csv", str(out))
    r = db.execute(f"SELECT name FROM {t2};").get("data", [])
    expect(any(x[0]=='A' for x in r), "import csv")
    out.unlink(missing_ok=True)
    db.execute(f"DROP TABLE {t};")
    db.execute(f"DROP TABLE {t2};")
    print("OK: import/export")

if __name__ == "__main__":
    main()


