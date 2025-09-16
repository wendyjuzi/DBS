## 编译器（modules/sql_compiler）

- lexical/: 词法分析器 Lexer
- syntax/: 语法分析器 Parser（生成 AST）
- semantic/: 语义分析（Catalog、视图、列/表校验、聚合校验）
- planner/: 逻辑计划生成（视图查询展开时复用）
- diagnostics/: 错误诊断与友好提示

关键增强：
- 语义层识别 `FROM` 视图名，放行查询并在适配器层做视图展开
- 聚合语义检查兼容 `AGGREGATE: AVG\n ARG: COL` 文本形式

