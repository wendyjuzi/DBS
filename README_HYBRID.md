# 混合架构数据库系统 (Python-C++ Hybrid)

本系统采用"Python做上层框架+C++做底层核心"的混合架构，Python负责SQL解析、交互接口与执行计划调度，C++负责高性能的存储引擎、算子执行与数据IO，通过`pybind11`实现跨语言无缝调用。

## 系统特性

- **混合架构**: Python灵活性与C++高性能结合
- **核心功能**: 支持CREATE TABLE、INSERT、SELECT、DELETE操作
- **页式存储**: 4KB页大小，符合数据库存储规范
- **执行引擎**: 实现CreateTable、Insert、SeqScan、Filter、Project等算子
- **系统目录**: 元数据管理，支持表结构注册与查询
- **命令行界面**: 友好的CLI交互体验

## 技术栈

| 模块 | 技术选型 | 说明 |
|------|----------|------|
| 跨语言集成 | pybind11 | 轻量级C++/Python绑定 |
| C++核心 | C++17 | 高性能存储与执行引擎 |
| Python上层 | Python 3.6+ | SQL解析与交互接口 |
| SQL解析 | sqlparse | 标准SQL语句解析 |
| 存储格式 | 自定义二进制页 | 4KB页式存储 |

## 快速开始

### 1. 环境要求

- Python 3.6+
- C++17编译器 (Windows: Visual Studio 2019+, Linux: g++ 7+, macOS: Xcode 10+)
- pip包管理器

### 2. 自动构建

```bash
# 克隆项目
git clone <repository-url>
cd 数据库系统

# 运行自动构建脚本
python build_hybrid_db.py
```

构建脚本会自动：
- 检查Python版本
- 安装Python依赖
- 编译C++核心模块
- 测试模块导入

### 3. 手动构建

如果自动构建失败，可以手动执行：

```bash
# 安装Python依赖
pip install -r requirements.txt

# 编译C++模块
cd cpp_core
python setup.py build_ext --inplace
cd ..

# 测试系统
python test_hybrid_db.py
```

### 4. 运行数据库

```bash
# 启动命令行界面
python hybrid_db.py
```

## 使用示例

```
=== 混合架构数据库系统 (Python-C++ Hybrid) ===
支持的命令: CREATE TABLE, INSERT, SELECT, DELETE
输入 'exit' 退出, 'help' 查看帮助

db> CREATE TABLE student (id INT PRIMARY KEY, name STRING, age INT, score DOUBLE)
✓ 表 'student' 创建成功

db> INSERT INTO student VALUES (1, 'Alice', 20, 95.5)
✓ 影响 1 行

db> INSERT INTO student VALUES (2, 'Bob', 19, 88.0)
✓ 影响 1 行

db> SELECT * FROM student
-----------------------------------------------------
| id | name  | age | score  |
-----------------------------------------------------
| 1  | Alice | 20  | 95.5   |
| 2  | Bob   | 19  | 88.0   |
-----------------------------------------------------
共 2 行

db> SELECT name, score FROM student WHERE age > 19
------------------------------------------------
| name  | score  |
------------------------------------------------
| Alice | 95.5   |
------------------------------------------------
共 1 行

db> DELETE FROM student WHERE id = 2
✓ 删除了 1 行

db> SELECT * FROM student
---------------------------------------------
| id | name  | age | score  |
---------------------------------------------
| 1  | Alice | 20  | 95.5   |
---------------------------------------------
共 1 行

db> exit
再见!
```

## 系统架构

```
[Python层：灵活调度与交互]
├─ SQL解析模块（sqlparse）：拆分SQL类型（CREATE/INSERT/SELECT/DELETE）
├─ 执行计划生成：将SQL转换为C++算子调用序列
├─ 系统目录交互：封装C++元数据接口
└─ CLI交互界面：接收用户输入，格式化输出执行结果

[pybind11：跨语言桥梁]
├─ 数据类型绑定：C++的Row/TableSchema→Python可调用对象
├─ 算子接口暴露：C++的CreateTable/Insert/SeqScan等算子→Python函数
└─ 异常处理：C++错误→Python异常

[C++层：高性能核心]
├─ 存储引擎：页管理、Row-Page映射、磁盘持久化
├─ 执行引擎：CreateTable/Insert/SeqScan/Filter/Project算子实现
└─ 系统目录：元数据表、表结构缓存
```

## 支持的操作

### 数据定义语言 (DDL)

```sql
-- 创建表
CREATE TABLE table_name (
    column1 type1 [PRIMARY KEY],
    column2 type2,
    ...
);
```

支持的数据类型：
- `INT`: 整数
- `STRING`: 字符串
- `DOUBLE`: 浮点数

### 数据操作语言 (DML)

```sql
-- 插入数据
INSERT INTO table_name VALUES (value1, value2, ...);

-- 查询数据
SELECT column1, column2 FROM table_name [WHERE condition];

-- 删除数据
DELETE FROM table_name [WHERE condition];
```

### WHERE条件

支持简单的比较操作：
- `=`: 等于
- `>`: 大于
- `<`: 小于
- `>=`: 大于等于
- `<=`: 小于等于

## 文件结构

```
数据库系统/
├── cpp_core/                 # C++核心模块
│   ├── db_core.h            # C++头文件
│   ├── db_engine.cpp        # C++实现文件
│   ├── setup.py             # 编译配置
│   └── build.sh/build.bat   # 构建脚本
├── src/
│   ├── core/
│   │   ├── hybrid_engine.py      # 混合架构引擎
│   │   ├── parser/
│   │   │   └── hybrid_sql_parser.py  # SQL解析器
│   │   └── executor/
│   │       └── hybrid_executor.py    # 执行引擎
│   └── frontend/
│       └── hybrid_cli.py           # 命令行界面
├── hybrid_db.py             # 主入口文件
├── build_hybrid_db.py       # 构建脚本
├── test_hybrid_db.py        # 测试脚本
└── requirements.txt         # Python依赖
```

## 性能优化

1. **页式存储**: 4KB页大小，减少磁盘IO次数
2. **内存缓存**: 页缓存机制，避免重复磁盘读取
3. **C++核心**: 关键路径使用C++实现，提升性能
4. **逻辑删除**: 避免物理删除产生的存储碎片

## 扩展建议

1. **索引优化**: 添加B+树索引，减少全表扫描
2. **批量操作**: 支持批量INSERT，提升插入性能
3. **事务支持**: 添加WAL日志，实现ACID特性
4. **类型安全**: 加强数据类型校验
5. **并发控制**: 添加锁机制，支持多用户访问

## 故障排除

### 编译错误

1. **pybind11未安装**:
   ```bash
   pip install pybind11
   ```

2. **C++编译器未找到**:
   - Windows: 安装Visual Studio 2019+
   - Linux: 安装g++ 7+
   - macOS: 安装Xcode 10+

3. **Python版本过低**:
   - 需要Python 3.6或更高版本

### 运行时错误

1. **模块导入失败**:
   - 确保C++模块已正确编译
   - 检查Python路径设置

2. **SQL语法错误**:
   - 参考支持的操作列表
   - 检查SQL语句格式

## 许可证

本项目采用MIT许可证，详见LICENSE文件。
