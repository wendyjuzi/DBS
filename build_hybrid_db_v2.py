#!/usr/bin/env python3
"""
混合架构数据库系统构建脚本 v2
重新组织后的模块化结构
"""

import os
import sys
import subprocess
import platform
import shutil

def check_python_version():
    """检查Python版本"""
    if sys.version_info < (3, 6):
        print("错误: 需要Python 3.6或更高版本")
        sys.exit(1)
    print(f"✓ Python版本: {sys.version}")

def install_dependencies():
    """安装Python依赖"""
    print("安装Python依赖...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        print("✓ Python依赖安装完成")
    except subprocess.CalledProcessError as e:
        print(f"错误: 安装Python依赖失败: {e}")
        sys.exit(1)

def build_cpp_module():
    """编译C++模块"""
    print("编译C++核心模块...")
    
    cpp_dir = "modules/database_system/storage_engine/cpp_core"
    if not os.path.exists(cpp_dir):
        print(f"错误: C++源码目录不存在: {cpp_dir}")
        sys.exit(1)
    
    # 切换到C++目录
    original_dir = os.getcwd()
    os.chdir(cpp_dir)
    
    try:
        # 检查pybind11是否安装
        try:
            import pybind11
            print(f"✓ pybind11已安装: {pybind11.__version__}")
        except ImportError:
            print("安装pybind11...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", "pybind11"])
        
        # 编译C++模块
        if platform.system() == "Windows":
            subprocess.check_call([sys.executable, "setup.py", "build_ext", "--inplace"])
        else:
            subprocess.check_call([sys.executable, "setup.py", "build_ext", "--inplace"])
        
        # 检查编译结果
        if platform.system() == "Windows":
            module_file = "db_core.pyd"
        else:
            module_file = "db_core.so"
            
        if os.path.exists(module_file):
            print(f"✓ C++模块编译成功: {module_file}")
            # 复制到项目根目录
            shutil.copy(module_file, "../../../../")
            print("✓ C++模块已复制到项目根目录")
        else:
            print(f"错误: 编译失败，未找到模块文件: {module_file}")
            sys.exit(1)
            
    except subprocess.CalledProcessError as e:
        print(f"错误: 编译C++模块失败: {e}")
        sys.exit(1)
    finally:
        os.chdir(original_dir)

def test_import():
    """测试模块导入"""
    print("测试模块导入...")
    try:
        # 测试导入新模块
        from modules.database_system.hybrid_engine import HybridDatabaseEngine
        print("✓ 混合架构引擎导入成功")
        
        from modules.database_system.parser.simple_sql_parser import SimpleSQLParser
        print("✓ SQL解析器导入成功")
        
        from modules.database_system.executor.hybrid_executor import HybridExecutionEngine
        print("✓ 执行引擎导入成功")
        
        from modules.database_system.frontend.hybrid_cli import HybridCLI
        print("✓ CLI界面导入成功")
        
    except ImportError as e:
        print(f"错误: 模块导入失败: {e}")
        sys.exit(1)

def create_directory_structure():
    """创建目录结构说明"""
    print("\n=== 重新组织后的目录结构 ===")
    print("""
modules/
├── database_system/              # 混合架构数据库系统模块
│   ├── __init__.py              # 模块初始化
│   ├── hybrid_engine.py         # 混合架构引擎
│   ├── parser/                  # SQL解析器模块
│   │   ├── __init__.py
│   │   └── simple_sql_parser.py # 简化SQL解析器
│   ├── executor/                # 执行引擎模块
│   │   ├── __init__.py
│   │   └── hybrid_executor.py   # 混合执行引擎
│   ├── frontend/                # 前端界面模块
│   │   ├── __init__.py
│   │   └── hybrid_cli.py        # 命令行界面
│   └── storage_engine/          # 存储引擎模块
│       └── cpp_core/            # C++核心实现
│           ├── db_core.h        # C++头文件
│           ├── db_engine.cpp    # C++实现文件
│           ├── setup.py         # 编译配置
│           └── db_core.pyd      # 编译后的模块
├── sql_compiler/                # SQL编译器模块
├── os_storage/                  # 操作系统存储模块
└── __init__.py                  # 根模块初始化
    """)

def main():
    """主函数"""
    print("=== 混合架构数据库系统构建脚本 v2 ===")
    print("重新组织后的模块化结构")
    print(f"操作系统: {platform.system()}")
    print(f"架构: {platform.machine()}")
    print()
    
    # 检查Python版本
    check_python_version()
    
    # 安装依赖
    install_dependencies()
    
    # 编译C++模块
    build_cpp_module()
    
    # 测试导入
    test_import()
    
    # 显示目录结构
    create_directory_structure()
    
    print()
    print("=== 构建完成 ===")
    print("运行数据库系统: python hybrid_db_v2.py")
    print("快速演示: python quick_demo_v2.py")
    print("或直接运行: python modules/database_system/frontend/hybrid_cli.py")

if __name__ == "__main__":
    main()
