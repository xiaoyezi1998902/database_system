"""
项目入口：加载 SQL → 编译器 → 执行引擎 → 存储引擎
"""

from compiler.lexer import Lexer
from compiler.parser import Parser
from compiler.semantic_analyzer import SemanticAnalyzer
from compiler.planner import Planner

from execution.executor import Executor
from storage.disk_manager import DiskManager
from storage.buffer_manager import BufferManager
from execution.system_catalog import SystemCatalog


def run_sql(sql: str, catalog: SystemCatalog, executor: Executor):
    """
    执行单条 SQL 语句的流程：
    SQL → Token 流 → AST → 语义检查 → 执行计划 → 执行结果
    """
    print("\n=== 输入 SQL ===")
    print(sql)

    # 1. 词法分析
    lexer = Lexer(sql)
    tokens = lexer.tokenize()
    print("\n[Token 流]")
    for t in tokens:
        print(t)

    # 2. 语法分析
    parser = Parser(tokens)
    ast = parser.parse()
    print("\n[AST]")
    print(ast)

    # 3. 语义分析
    analyzer = SemanticAnalyzer(catalog)
    analyzer.check(ast)
    print("\n[语义检查] 通过 ✅")

    # 4. 执行计划生成
    planner = Planner()
    plan = planner.create_plan(ast)
    print("\n[执行计划]")
    print(plan)

    # 5. 执行引擎执行
    print("\n[执行结果]")
    result = executor.execute(plan)
    if isinstance(result, int):
        print(f"{result} 行受影响")
    elif isinstance(result, list):
        for row in result:
            print(row)
    elif result is not None:
        print(result)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="简化数据库系统 CLI")
    parser.add_argument("--file", dest="file", help="SQL 文件路径（可选）", default=None)
    args = parser.parse_args()

    # === 初始化系统组件 ===
    disk_manager = DiskManager()
    buffer_manager = BufferManager(disk_manager)
    catalog = SystemCatalog()
    executor = Executor(catalog, buffer_manager, disk_manager)

    def split_sql(sql_text: str):
        stmt = []
        in_string = False
        escape = False
        for ch in sql_text:
            stmt.append(ch)
            if ch == "'" and not escape:
                in_string = not in_string
            escape = (ch == '\\' and not escape)
            if ch == ';' and not in_string:
                yield ''.join(stmt).strip()
                stmt = []
        tail = ''.join(stmt).strip()
        if tail:
            yield tail

    if args.file:
        with open(args.file, 'r', encoding='utf-8') as f:
            sql_text = f.read()
        statements = list(split_sql(sql_text))
    else:
        statements = [
            "CREATE TABLE student(id INT, name TEXT);",
            "INSERT INTO student VALUES (1, 'Alice');",
            "INSERT INTO student VALUES (2, 'Bob');",
            "SELECT id, name FROM student;",
            "DELETE FROM student WHERE id = 1;",
            "SELECT id, name FROM student;",
        ]

    for sql in statements:
        try:
            run_sql(sql, catalog, executor)
        except Exception as e:
            print(f"[错误] {e}")


if __name__ == "__main__":
    main()
