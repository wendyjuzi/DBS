"""
API层模块
提供统一的数据库访问接口
"""

from .db_api import DatabaseAPI
from .rest_api import create_rest_app

__all__ = ['DatabaseAPI', 'create_rest_app']