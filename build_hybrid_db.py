#!/usr/bin/env python3
"""
混合架构数据库系统构建脚本
自动编译C++模块并安装依赖
"""

import os
import sys
import subprocess
import platform

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
    
    cpp_dir = "cpp_core"
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
        # 添加C++目录到Python路径
        cpp_dir = os.path.abspath("cpp_core")
        if cpp_dir not in sys.path:
            sys.path.insert(0, cpp_dir)
        
        # 测试导入C++模块
        import db_core
        print("✓ C++模块导入成功")
        
        # 测试导入Python模块
        from src.core.hybrid_engine import HybridDatabaseEngine
        print("✓ Python模块导入成功")
        
    except ImportError as e:
        print(f"错误: 模块导入失败: {e}")
        sys.exit(1)

def main():
    """主函数"""
    print("=== 混合架构数据库系统构建脚本 ===")
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
    
    print()
    print("=== 构建完成 ===")
    print("运行数据库系统: python hybrid_db.py")
    print("或直接运行: python src/frontend/hybrid_cli.py")

if __name__ == "__main__":
    main()
