param(
    [switch]$Clean
)

$ErrorActionPreference = 'Stop'

function Write-Step($msg) { Write-Host "[STEP] $msg" -ForegroundColor Cyan }
function Write-Ok($msg)   { Write-Host "[OK]   $msg" -ForegroundColor Green }
function Write-Warn($msg) { Write-Host "[WARN] $msg" -ForegroundColor Yellow }

Push-Location $PSScriptRoot\..
try {
    Write-Step "Build C++ extension"
    Push-Location cpp_core
    $buildFailed = $false
    try {
        python setup.py build_ext --inplace | Out-Null
    } catch {
        $buildFailed = $true
        Write-Warn "Build failed, will try using existing pyd if present. Details: $_"
    } finally {
        Pop-Location
    }
    $pyd = "cpp_core\build\lib.win-amd64-3.10\db_core.cp310-win_amd64.pyd"
    if (-not (Test-Path $pyd)) {
        if ($buildFailed) { throw "No built pyd found: $pyd" }
    }
    Copy-Item $pyd . -Force
    Write-Ok "Extension ready"

    if ($Clean) {
        Write-Step "Clean catalog and page files"
        Remove-Item -ErrorAction SilentlyContinue -Force sys_catalog_page_0.bin
        Get-ChildItem -Filter '*_page_*.bin' | Remove-Item -Force -ErrorAction SilentlyContinue
        Write-Ok "Clean done"
    }

    Write-Step "Run minimal pushdown demo"
    $proj = (Get-Location).Path
    $tmpPy = Join-Path $PSScriptRoot 'pushdown_demo_tmp.py'
    @"
import sys
sys.path.insert(0, r'$proj')
from src.api.db_api import DatabaseAPI

db = DatabaseAPI()
print('CREATE', db.execute("CREATE TABLE t2(id INT PRIMARY KEY, name STRING, age INT, score DOUBLE)"))
eng = db._runner
rows = [[str(i), f"Name{i}", str(18+i), str(80.0+i)] for i in range(1,6)]
print('INSERT_MANY', eng.insert_many('t2', rows))
print('SELECT_PUSH', db.execute("SELECT name,score FROM t2 WHERE id >= 2 AND id <= 4"))
print('SELECT_PK3', db.execute("SELECT id,name FROM t2 WHERE id = 3"))
"@ | Set-Content -Encoding UTF8 $tmpPy
    python $tmpPy
    Remove-Item -ErrorAction SilentlyContinue $tmpPy
    Write-Ok "Demo finished"
}
catch {
    Write-Warn $_
    exit 1
}
finally {
    Pop-Location
}


