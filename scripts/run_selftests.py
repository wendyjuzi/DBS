#!/usr/bin/env python3
import sys
import subprocess
from pathlib import Path

root = Path(__file__).resolve().parents[1]

tests = [
    "scripts/selftests/test_adapter_basic.py",
    "scripts/selftests/test_transactions.py",
    "scripts/selftests/test_indexes.py",
    "scripts/selftests/test_views.py",
    "scripts/selftests/test_materialized_views.py",
    "scripts/selftests/test_triggers.py",
    "scripts/selftests/test_procedures.py",
    "scripts/selftests/test_import_export.py",
    "scripts/selftests/test_core_mode.py",
]

def run(cmd):
    print(f"\n=== Running: {cmd} ===")
    p = subprocess.run([sys.executable, str(root / cmd)], cwd=str(root))
    if p.returncode != 0:
        print(f"[FAIL] {cmd}")
        return False
    print(f"[OK] {cmd}")
    return True

def main():
    ok = True
    for t in tests:
        ok = run(t) and ok
    print("\nRESULT:", "ALL PASSED" if ok else "SOME FAILED")
    sys.exit(0 if ok else 1)

if __name__ == "__main__":
    main()


