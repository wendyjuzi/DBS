# 数据库系统

一个用Python实现的简单关系型数据库系统，采用分层架构设计。

## 项目结构

```
database_system/
├── src/                    # 源代码目录
│   ├── frontend/          # 前端界面
│   │   ├── cli.py         # 命令行界面
│   │   └── web/           # Web界面
│   ├── api/               # API层
│   │   ├── db_api.py      # 数据库API接口
│   │   └── rest_api.py    # RESTful API
│   ├── core/              # 核心引擎
│   │   ├── parser/        # SQL解析器
│   │   ├── catalog/       # 系统目录
│   │   ├── planner/       # 执行计划生成
│   │   ├── executor/      # 执行引擎
│   │   └── optimizer/     # 查询优化器
│   ├── storage/           # 存储后端
│   │   ├── engine.py      # 存储引擎接口
│   │   ├── file_storage.py # 文件存储实现
│   │   ├── memory_storage.py # 内存存储实现
│   │   ├── buffer_pool.py # 缓存管理
│   │   ├── page.py        # 页式存储模型
│   │   └── index.py       # 索引管理
│   └── utils/             # 工具函数
│       ├── exceptions.py  # 自定义异常
│       ├── logging.py     # 日志配置
│       ├── constants.py   # 系统常量
│       └── helpers.py     # 辅助函数
├── tests/                 # 测试代码
├── data/                  # 数据存储目录
├── logs/                  # 日志目录
├── docs/                  # 项目文档
├── config.json            # 配置文件
├── requirements.txt       # 依赖列表
└── main.py               # 主程序入口
```

## 功能特性

- SQL解析和执行
- 表结构管理
- 数据存储和检索
- 索引支持
- 命令行界面
- Web界面（可选）
- 事务支持（计划中）

## 安装和运行

1. 安装依赖：
```bash
pip install -r requirements.txt
```

2. 运行命令行界面：
```bash
python main.py cli
```

3. 运行Web界面：
```bash
python main.py web
```

## 开发计划

1. 实现SQL解析器
2. 实现基础存储引擎
3. 实现查询执行器
4. 添加索引支持
5. 实现事务管理
6. 性能优化

## 许可证

MIT License
