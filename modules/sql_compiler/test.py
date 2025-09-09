import sys
from modules.sql_compiler.lexical.lexer import Lexer
from modules.sql_compiler.syntax.parser import Parser, ParseError
from modules.sql_compiler.semantic.semantic import SemanticAnalyzer, Catalog
from modules.sql_compiler.planner.planner import Planner

def run_sql(sql_text):
    print("=== SQL Input ===")
    print(sql_text.strip())

    # 1. 词法分析
    print("\n=== 词法分析阶段 ===")
    lexer = Lexer(sql_text)
    tokens, errors = lexer.tokenize()
    
    print("Token 流:")
    for t in tokens:
        print(f"  {t}")
        
    if errors:
        print("\n❌ 词法分析失败!")
        print("--- Lexical Errors ---")
        for e in errors:
            print(f"  {e}")
        print("程序终止：词法错误导致无法继续分析")
        return

    print("✅ 词法分析成功!")

    # 2. 语法分析
    print("\n=== 语法分析阶段 ===")
    parser = Parser(tokens)
    try:
        ast_list = parser.parse()
        print("✅ 语法分析成功!")
        print("抽象语法树 (AST):")
        for ast in ast_list:
            print(ast)
    except ParseError as e:
        print("❌ 语法分析失败!")
        print(f"[ParseError] {e}")
        print("程序终止：语法错误导致无法生成 AST，后续步骤无法继续")
        return

    # 3. 语义分析
    print("\n=== 语义分析阶段 ===")
    catalog = Catalog()  # 创建模式目录
    analyzer = SemanticAnalyzer(catalog)
    semantic_errors = 0
    
    for ast in ast_list:
        try:
            analyzer.analyze(ast)
            ast_type = ast.node_type if hasattr(ast, "node_type") else ast["type"]
            print(f"✅ [OK] 语义检查通过: {ast_type}")
        except Exception as e:
            print(f"❌ [SemanticError] {e}")
            semantic_errors += 1

    if semantic_errors > 0:
        print(f"\n❌ 语义分析失败! 检测到 {semantic_errors} 个语义错误")
        print("程序终止：语义错误导致无法生成可靠的执行计划")
        return
    
    print("✅ 语义分析成功!")

    # 4. 执行计划生成
    print("\n=== 执行计划生成阶段 ===")
    try:
        ast_list_dict = [ast.to_dict() for ast in ast_list]  # ASTNode -> dict
        planner = Planner(ast_list_dict)
        plans = planner.generate_plan()
        print("✅ 执行计划生成成功!")
        print("执行计划:")
        for plan in plans:
            print(plan)
    except Exception as e:
        print("❌ 执行计划生成失败!")
        print(f"[PlanError] {e}")
        return


if __name__ == "__main__":
    if len(sys.argv) > 1:
        # 从文件读取
        with open(sys.argv[1], "r", encoding="utf-8") as f:
            sql_text = f.read()
    else:
        # 标准输入
        print("请输入 SQL 语句 (多条语句用分号分隔，以空行结束):")
        lines = []
        while True:
            line = input()
            if line.strip() == "":
                break
            lines.append(line)
        sql_text = "\n".join(lines)

    run_sql(sql_text)
