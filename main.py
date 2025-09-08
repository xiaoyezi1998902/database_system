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
    if result is not None:
        for row in result:
            print(row)


def main():
    # === 初始化系统组件 ===
    # 存储层
    disk_manager = DiskManager("data.db")
    buffer_manager = BufferManager(disk_manager)

    # 系统目录（元信息表）
    catalog = SystemCatalog(buffer_manager)

    # 执行引擎
    executor = Executor(catalog, buffer_manager)

    # === 测试 SQL 脚本 ===
    sql_statements = [
        "CREATE TABLE student(id INT, name TEXT);",
        "INSERT INTO student VALUES (1, 'Alice');",
        "INSERT INTO student VALUES (2, 'Bob');",
        "SELECT id, name FROM student;",
        "DELETE FROM student WHERE id = 1;",
        "SELECT id, name FROM student;",
    ]

    for sql in sql_statements:
        try:
            run_sql(sql, catalog, executor)
        except Exception as e:
            print(f"[错误] {e}")


if __name__ == "__main__":
    main()
