@echo off
chcp 65001 >nul
echo ===================================
echo    混合架构数据库系统 GUI
echo ===================================
echo.

cd /d "%~dp0"

echo 正在启动数据库GUI...
python run_gui.py

if %errorlevel% neq 0 (
    echo.
    echo GUI启动失败！
    echo 请检查：
    echo 1. Python是否正确安装
    echo 2. 是否运行了 python build_hybrid_db.py
    echo 3. 依赖包是否安装完整
    echo.
    pause
) else (
    echo GUI已关闭
)
