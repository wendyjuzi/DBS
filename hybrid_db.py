#!/usr/bin/env python3
"""
混合架构数据库系统主入口
Python集成C++的简化数据库系统实现
"""

import sys
import os

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.frontend.hybrid_cli import main

if __name__ == "__main__":
    main()
