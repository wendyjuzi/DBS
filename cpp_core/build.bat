@echo off
echo Building C++ database core module...

REM 检查Python环境
python --version
if %errorlevel% neq 0 (
    echo Error: Python not found in PATH
    exit /b 1
)

REM 检查pybind11是否安装
python -c "import pybind11; print('pybind11 found')"
if %errorlevel% neq 0 (
    echo Installing pybind11...
    pip install pybind11
)

REM 编译C++模块
echo Compiling C++ module...
python setup.py build_ext --inplace

if %errorlevel% equ 0 (
    echo Build successful! Generated db_core.pyd
) else (
    echo Build failed!
    exit /b 1
)

echo Done!
