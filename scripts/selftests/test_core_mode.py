#!/usr/bin/env python3
import sys
from pathlib import Path

root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(root))

from src.core.hybrid_engine import HybridDatabaseEngine


def expect(ok: bool, msg: str):
    print(("[OK] " if ok else "[FAIL] ") + msg)
    if not ok:
        sys.exit(1)


def main():
    core = HybridDatabaseEngine()
    import uuid
    t = f"t_core_{uuid.uuid4().hex[:8]}"
    try:
        core.execute(f"DROP TABLE {t};")
    except Exception:
        pass
    core.execute(f"CREATE TABLE {t} (id INT, name STRING);")
    core.execute(f"INSERT INTO {t} VALUES (1, 'A');")
    # core 支持 * 与较宽松 WHERE
    r = core.execute(f"SELECT * FROM {t} WHERE id = 1;")
    rows = r.get("data", [])
    expect(len(rows)>=1, "core select * where")
    # core 模式不支持 DROP，直接结束即可
    print("OK: core mode")

if __name__ == "__main__":
    main()


