"""
操作系统存储模块常量定义
"""

import os
from pathlib import Path

# --- 存储常量 ---
PAGE_SIZE = 4096  # 4KB 页大小
ROW_ACTIVE = 1     # 行状态：活跃
ROW_DELETED = 0    # 行状态：已删除

# --- 缓存常量 ---
DEFAULT_CACHE_CAPACITY = 100     # 默认缓存容量（页数）
DEFAULT_CACHE_STRATEGY = "LRU"   # 默认缓存替换策略
CACHE_STRATEGIES = ["LRU", "FIFO"]  # 支持的缓存策略

# --- 文件路径常量 ---
# 数据存储根目录（相对于项目根目录）
BASE_DIR = Path(__file__).parent.parent.parent
DATA_DIR = BASE_DIR / "data"
# 每个数据库的存储目录
CATALOG_DB_DIR = DATA_DIR / "catalog"
USER_DB_DIR = DATA_DIR / "user_data"

# --- 页结构常量 ---
PAGE_HEADER_SIZE = 16  # 页头大小 [free_start(4)][slot_count(4)][free_space(4)][reserved(4)]
SLOT_SIZE = 9          # 槽目录项大小 [offset(4)][length(4)][flag(1)]

# --- 日志配置 ---
LOG_LEVEL = "INFO"     # 日志级别：DEBUG, INFO, WARNING, ERROR
ENABLE_CACHE_LOG = True  # 是否启用缓存日志
ENABLE_PAGE_LOG = False  # 是否启用页操作日志

def ensure_directories():
    """确保所有必要的目录都存在"""
    directories = [DATA_DIR, CATALOG_DB_DIR, USER_DB_DIR]
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)
    return directories

# 初始化时创建目录
ensure_directories()