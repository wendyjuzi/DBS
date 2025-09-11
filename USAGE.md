# 快速使用（C++ 执行引擎 + Python 上层）

本节给出最小可复现步骤，覆盖构建、清理、批量插入与下推过滤/索引点查。

## 1) 构建 C++ 扩展

在 PowerShell 中执行：

```
cd cpp_core
python setup.py build_ext --inplace
cd ..
Copy-Item cpp_core\build\lib.win-amd64-3.10\db_core.cp310-win_amd64.pyd . -Force
```

如使用虚拟环境，请先激活 venv 再执行。

## 2) 可选：清理旧数据（重置环境）

```
Remove-Item -ErrorAction SilentlyContinue -Force sys_catalog_page_0.bin
Get-ChildItem -Filter '*_page_*.bin' | Remove-Item -Force -ErrorAction SilentlyContinue
```

## 3) 最小验证（SQL → 解析 → 优化 → 执行）

以下命令均在项目根目录执行（注意把项目根加入 sys.path）：

```
# 创建表（4 列，主键 id）
python -c "import sys; sys.path.insert(0, r'D:\大三上\数据库系统'); from src.api.db_api import DatabaseAPI; db=DatabaseAPI(); print('CREATE', db.execute('CREATE TABLE t2(id INT PRIMARY KEY, name STRING, age INT, score DOUBLE)'))"

# 批量插入（5 行）
python -c "import sys; sys.path.insert(0, r'D:\大三上\数据库系统'); from src.api.db_api import DatabaseAPI; db=DatabaseAPI(); eng=db._runner; rows=[[str(i), f'Name{i}', str(18+i), str(80.0+i)] for i in range(1,6)]; print('INSERT_MANY', eng.insert_many('t2', rows))"

# 下推过滤（WHERE id >= 2 AND id <= 4），仅返回 name,score 两列
python -c "import sys; sys.path.insert(0, r'D:\大三上\数据库系统'); from src.api.db_api import DatabaseAPI; db=DatabaseAPI(); print('SELECT_PUSH', db.execute('SELECT name,score FROM t2 WHERE id >= 2 AND id <= 4'))"

# 主键点查（命中索引或自动回退顺扫）
python -c "import sys; sys.path.insert(0, r'D:\大三上\数据库系统'); from src.api.db_api import DatabaseAPI; db=DatabaseAPI(); print('SELECT_PK3', db.execute('SELECT id,name FROM t2 WHERE id = 3'))"
```

预期：
- INSERT_MANY 输出 5；
- SELECT_PUSH 返回 3 行；
- SELECT_PK3 返回 [["3","Name3"]]。

## 4) 说明与排错

- 若报导入 Flask/Werkzeug 相关错误，是因为纯本地模式无需 REST，请仅从 `src.api.db_api` 导入 `DatabaseAPI`；`src/api/__init__.py` 已调整默认不导入 REST。
- 若 `filter()` 回调报类型不匹配，执行器会自动回退为 Python 侧过滤；也可通过 `filter_conditions` 下推条件避免回调开销（已在执行器中自动尝试）。
- 若点查未命中但返回结果不为空，说明已自动回退顺扫+过滤；这不影响功能正确性。

## 5) 常见接口

- CreateTable：`db.execute('CREATE TABLE t(id INT PRIMARY KEY, name STRING, ...)')`
- Insert：`db.execute("INSERT INTO t VALUES (1,'Alice',20,95.5)")` 或批量 `db._runner.insert_many('t', rows)`
- Select（顺扫/索引 + 过滤/投影）：`db.execute('SELECT id,name FROM t WHERE id = 1')`
- Flush：`db.flush()`（主动刷脏页）

# 混合架构数据库系统使用说明

## 系统概述

本系统是一个Python集成C++的简化数据库系统，采用混合架构设计：
- **Python层**：负责SQL解析、交互接口与执行计划调度
- **C++层**：负责高性能的存储引擎、算子执行与数据IO
- **pybind11**：实现跨语言无缝调用

## 快速开始

### 1. 构建系统

```bash
# 自动构建（推荐）
python build_hybrid_db.py

# 或手动构建
pip install -r requirements.txt
cd cpp_core
python setup.py build_ext --inplace
cd ..
copy cpp_core\db_core.cp310-win_amd64.pyd .
```

### 2. 运行系统

```bash
# 启动交互式命令行界面
python hybrid_db.py

# 或运行快速演示
python quick_demo.py
```

## 支持的功能

### 数据定义语言 (DDL)

```sql
-- 创建表
CREATE TABLE table_name (
    column1 type1 [PRIMARY KEY],
    column2 type2,
    ...
);
```

**支持的数据类型：**
- `INT`: 整数
- `STRING`: 字符串  
- `DOUBLE`: 浮点数

**示例：**
```sql
CREATE TABLE student (id INT PRIMARY KEY, name STRING, age INT, score DOUBLE);
```

### 数据操作语言 (DML)

#### 插入数据
```sql
INSERT INTO table_name VALUES (value1, value2, ...);
```

**示例：**
```sql
INSERT INTO student VALUES (1, 'Alice', 20, 95.5);
INSERT INTO student VALUES (2, 'Bob', 19, 88.0);
```

#### 查询数据
```sql
SELECT column1, column2 FROM table_name [WHERE condition];
SELECT * FROM table_name [WHERE condition];
```

**示例：**
```sql
-- 查询所有学生
SELECT * FROM student;

-- 查询特定列
SELECT name, score FROM student;

-- 条件查询
SELECT * FROM student WHERE age > 19;
SELECT name, score FROM student WHERE score >= 90;
```

#### 删除数据
```sql
DELETE FROM table_name [WHERE condition];
```

**示例：**
```sql
-- 删除特定记录
DELETE FROM student WHERE id = 2;

-- 删除所有记录
DELETE FROM student;
```

### WHERE条件

支持简单的比较操作：
- `=`: 等于
- `>`: 大于
- `<`: 小于
- `>=`: 大于等于
- `<=`: 小于等于

**示例：**
```sql
WHERE age > 18
WHERE name = 'Alice'
WHERE score >= 90.0
```

## 命令行界面

启动系统后，可以使用以下命令：

```
db> CREATE TABLE student (id INT PRIMARY KEY, name STRING, age INT, score DOUBLE)
✓ 表 'student' 创建成功

db> INSERT INTO student VALUES (1, 'Alice', 20, 95.5)
✓ 影响 1 行

db> SELECT * FROM student
-----------------------------------------------------
| id | name  | age | score  |
-----------------------------------------------------
| 1  | Alice | 20  | 95.5   |
-----------------------------------------------------
共 1 行

db> help
可用命令:
  CREATE TABLE table_name (col1 type1, col2 type2, ...)  - 创建表
  INSERT INTO table_name VALUES (val1, val2, ...)        - 插入数据
  SELECT col1, col2 FROM table_name [WHERE condition]    - 查询数据
  DELETE FROM table_name [WHERE condition]               - 删除数据
  tables                                                 - 显示所有表
  help                                                   - 显示此帮助
  exit                                                   - 退出程序

db> exit
再见!
```

## 系统特性

### 1. 混合架构优势
- **Python灵活性**：易于扩展和维护
- **C++高性能**：关键路径使用C++实现
- **无缝集成**：pybind11提供透明调用

### 2. 存储引擎
- **页式存储**：4KB页大小，符合数据库规范
- **内存缓存**：页缓存机制，减少磁盘IO
- **逻辑删除**：避免物理删除产生的碎片

### 3. 执行引擎
- **算子实现**：CreateTable、Insert、SeqScan、Filter、Project、Delete
- **执行计划**：SQL → AST → 执行计划 → 优化 → 执行
- **错误处理**：完善的异常处理机制

## 文件结构

```
数据库系统/
├── cpp_core/                    # C++核心模块
│   ├── db_core.h               # C++头文件
│   ├── db_engine.cpp           # C++实现文件
│   ├── setup.py                # 编译配置
│   └── db_core.cp310-win_amd64.pyd  # 编译后的模块
├── src/
│   ├── core/
│   │   ├── hybrid_engine.py         # 混合架构引擎
│   │   ├── parser/
│   │   │   └── simple_sql_parser.py # SQL解析器
│   │   └── executor/
│   │       └── hybrid_executor.py   # 执行引擎
│   └── frontend/
│       └── hybrid_cli.py            # 命令行界面
├── hybrid_db.py                # 主入口文件
├── quick_demo.py               # 快速演示
├── build_hybrid_db.py          # 构建脚本
└── requirements.txt            # Python依赖
```

## 故障排除

### 编译问题

1. **pybind11未安装**
   ```bash
   pip install pybind11
   ```

2. **C++编译器未找到**
   - Windows: 安装Visual Studio 2019+
   - Linux: 安装g++ 7+
   - macOS: 安装Xcode 10+

3. **Python版本过低**
   - 需要Python 3.6或更高版本

### 运行时问题

1. **模块导入失败**
   ```bash
   # 确保C++模块在正确位置
   copy cpp_core\db_core.cp310-win_amd64.pyd .
   ```

2. **SQL语法错误**
   - 检查SQL语句格式
   - 参考支持的操作列表

## 性能特点

- **页式存储**：4KB页大小，减少磁盘IO
- **内存缓存**：页缓存机制，提升访问速度
- **C++核心**：关键路径使用C++实现
- **逻辑删除**：避免存储碎片

## 扩展建议

1. **索引优化**：添加B+树索引
2. **批量操作**：支持批量INSERT
3. **事务支持**：添加WAL日志
4. **并发控制**：添加锁机制
5. **类型安全**：加强数据类型校验

## 许可证

本项目采用MIT许可证。
