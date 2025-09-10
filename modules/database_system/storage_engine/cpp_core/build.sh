#!/bin/bash

echo "Building C++ database core module..."

# 检查Python环境
python3 --version
if [ $? -ne 0 ]; then
    echo "Error: Python3 not found in PATH"
    exit 1
fi

# 检查pybind11是否安装
python3 -c "import pybind11; print('pybind11 found')"
if [ $? -ne 0 ]; then
    echo "Installing pybind11..."
    pip3 install pybind11
fi

# 编译C++模块
echo "Compiling C++ module..."
python3 setup.py build_ext --inplace

if [ $? -eq 0 ]; then
    echo "Build successful! Generated db_core.so"
else
    echo "Build failed!"
    exit 1
fi

echo "Done!"
