# 数据库系统（DBS）

一个用 Python 实现的教学用关系型数据库系统脚手架，采用分层与模块化设计。远端仓库标题为“DBS”。

## 项目结构

```
database_system/
├── src/                    # 传统分层代码（保留）
│   ├── frontend/          # 前端界面（CLI/Web）
│   ├── api/               # API 层
│   ├── core/              # 核心引擎（解析/计划/执行/优化/目录）
│   └── storage/           # 存储后端（页、缓冲池、索引）
├── modules/               # 三大教学板块的模块化实现
│   ├── sql_compiler/      # SQL 编译器（词法/语法/语义/计划/优化）
│   ├── os_storage/        # 操作系统存储实践（文件/页/缓冲/缓存）
│   └── database_system/   # 数据库系统综合（执行器/存储引擎/查询引擎/目录）
├── tests/                 # 单元与集成测试
├── docs/                  # 文档
├── data/                  # 数据目录（已在 .gitignore）
├── logs/                  # 日志目录（已在 .gitignore）
├── config.json            # 配置
├── requirements.txt       # 依赖
└── main.py                # 入口
```

## 功能特性

- SQL 解析与执行（编译前端 + 执行计划）
- 表结构与元数据管理
- 文件/页式存储与缓冲池
- 索引与基础优化
- 命令行与可选 Web 界面

## 安装与运行

```bash
pip install -r requirements.txt

# 运行 CLI
python main.py shell

# 运行 Web（可选）
python main.py web
```

## 开发阶段

1) SQL 编译器：词法/语法/语义/计划与优化
2) OS 存储：文件系统、页管理、缓冲池与缓存
3) 系统整合：执行器、事务与目录

## 远端仓库

- GitHub: https://github.com/wendyjuzi/DBS.git

## 许可证

MIT License
