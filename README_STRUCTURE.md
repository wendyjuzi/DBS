# 混合架构数据库系统 - 重新组织后的结构

## 项目概述

本项目是一个Python集成C++的简化数据库系统，采用混合架构设计。经过重新组织，代码结构更加清晰和模块化。

## 目录结构

```
数据库系统/
├── modules/                          # 核心模块目录
│   ├── __init__.py                  # 根模块初始化
│   ├── database_system/             # 混合架构数据库系统模块
│   │   ├── __init__.py             # 模块初始化
│   │   ├── hybrid_engine.py        # 混合架构引擎
│   │   ├── parser/                 # SQL解析器模块
│   │   │   ├── __init__.py
│   │   │   └── simple_sql_parser.py # 简化SQL解析器
│   │   ├── executor/               # 执行引擎模块
│   │   │   ├── __init__.py
│   │   │   └── hybrid_executor.py  # 混合执行引擎
│   │   ├── frontend/               # 前端界面模块
│   │   │   ├── __init__.py
│   │   │   └── hybrid_cli.py       # 命令行界面
│   │   └── storage_engine/         # 存储引擎模块
│   │       └── cpp_core/           # C++核心实现
│   │           ├── db_core.h       # C++头文件
│   │           ├── db_engine.cpp   # C++实现文件
│   │           ├── setup.py        # 编译配置
│   │           └── db_core.pyd     # 编译后的模块
│   ├── sql_compiler/               # SQL编译器模块
│   │   ├── lexical/                # 词法分析
│   │   ├── syntax/                 # 语法分析
│   │   ├── semantic/               # 语义分析
│   │   └── planner/                # 查询规划
│   └── os_storage/                 # 操作系统存储模块
│       ├── buffer_pool/            # 缓冲池
│       ├── cache/                  # 缓存管理
│       ├── file_system/            # 文件系统
│       └── page_management/        # 页管理
├── src/                            # 原有源码目录（保留兼容性）
├── tests/                          # 测试目录
├── docs/                           # 文档目录
├── data/                           # 数据目录
├── logs/                           # 日志目录
├── hybrid_db_v2.py                 # 主入口文件 v2
├── quick_demo_v2.py                # 快速演示 v2
├── build_hybrid_db_v2.py           # 构建脚本 v2
├── requirements.txt                # Python依赖
└── README_STRUCTURE.md             # 本文档
```

## 模块说明

### 1. database_system 模块

混合架构数据库系统的核心模块，包含所有主要功能：

#### hybrid_engine.py
- **功能**: 混合架构数据库引擎
- **职责**: 协调Python上层和C++核心，提供统一的数据库接口
- **依赖**: parser, executor, planner, optimizer

#### parser/ 目录
- **功能**: SQL解析器模块
- **文件**: simple_sql_parser.py
- **职责**: 解析SQL语句，生成抽象语法树(AST)

#### executor/ 目录
- **功能**: 执行引擎模块
- **文件**: hybrid_executor.py
- **职责**: Python调度C++算子，执行查询计划

#### frontend/ 目录
- **功能**: 前端界面模块
- **文件**: hybrid_cli.py
- **职责**: 提供命令行交互界面

#### storage_engine/cpp_core/ 目录
- **功能**: C++核心实现
- **文件**: db_core.h, db_engine.cpp, setup.py
- **职责**: 高性能存储引擎、算子执行、数据IO

### 2. sql_compiler 模块

SQL编译器模块，提供查询优化和规划功能：

- **lexical/**: 词法分析
- **syntax/**: 语法分析
- **semantic/**: 语义分析
- **planner/**: 查询规划

### 3. os_storage 模块

操作系统存储模块，提供底层存储抽象：

- **buffer_pool/**: 缓冲池管理
- **cache/**: 缓存管理
- **file_system/**: 文件系统接口
- **page_management/**: 页管理

## 使用方式

### 1. 构建系统

```bash
# 使用新的构建脚本
python build_hybrid_db_v2.py
```

### 2. 运行系统

```bash
# 使用新的主入口
python hybrid_db_v2.py

# 或直接运行模块
python modules/database_system/frontend/hybrid_cli.py
```

### 3. 快速演示

```bash
# 使用新的演示脚本
python quick_demo_v2.py
```

### 4. 编程接口

```python
# 导入混合架构引擎
from modules.database_system import HybridDatabaseEngine

# 创建数据库实例
engine = HybridDatabaseEngine()

# 执行SQL
result = engine.execute("CREATE TABLE test (id INT, name STRING)")
print(result)

# 关闭数据库
engine.close()
```

## 架构优势

### 1. 模块化设计
- **清晰分离**: 每个模块职责明确
- **易于维护**: 代码组织有序，便于修改和扩展
- **可重用性**: 模块间松耦合，便于复用

### 2. 混合架构
- **Python灵活性**: 上层逻辑易于开发和调试
- **C++高性能**: 底层核心保证执行效率
- **无缝集成**: pybind11提供透明调用

### 3. 扩展性
- **插件化**: 新功能可以作为独立模块添加
- **配置化**: 各模块可独立配置和优化
- **测试友好**: 模块化便于单元测试

## 开发指南

### 1. 添加新功能
1. 在对应模块目录下创建新文件
2. 更新模块的`__init__.py`
3. 添加相应的测试
4. 更新文档

### 2. 修改现有功能
1. 定位到对应的模块文件
2. 进行修改
3. 运行测试确保功能正常
4. 更新相关文档

### 3. 调试技巧
- 使用`python -m`运行模块
- 利用模块的`__main__`进行独立测试
- 使用日志系统记录调试信息

## 迁移指南

### 从旧版本迁移
1. 使用新的构建脚本: `python build_hybrid_db_v2.py`
2. 更新导入路径: 从`src.core`改为`modules.database_system`
3. 使用新的入口文件: `hybrid_db_v2.py`

### 兼容性
- 保留了原有的`src/`目录结构
- 旧版本脚本仍然可以运行
- 新版本提供了更好的模块化体验

## 性能特点

- **页式存储**: 4KB页大小，减少磁盘IO
- **内存缓存**: 页缓存机制，提升访问速度
- **C++核心**: 关键路径使用C++实现
- **逻辑删除**: 避免存储碎片

## 许可证

本项目采用MIT许可证。
