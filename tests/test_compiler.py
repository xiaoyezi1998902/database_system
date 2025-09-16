from compiler.lexer import Lexer
from compiler.parser import Parser
from compiler.semantic_analyzer import SemanticAnalyzer
from compiler.planner import Planner
from execution.system_catalog import SystemCatalog


# pytest -v ./tests/test_compiler.py
def compile_one(sql: str):
	lexer = Lexer(sql)
	tokens = lexer.tokenize()
	parser = Parser(tokens)
	ast = parser.parse()
	catalog = SystemCatalog()
	analyzer = SemanticAnalyzer(catalog)
	if ast.__class__.__name__ == 'CreateTable':
		# 先不报存在性
		pass
	else:
		# 为非建表语句准备一个测试表
		catalog.create_table('student', [{"name": "id", "type": "INT"}, {"name": "name", "type": "TEXT"}])
	print("所有表：", catalog.tables)

	analyzer.check(ast)
	planner = Planner()
	plan = planner.create_plan(ast)
	return tokens, ast, plan


def test_create_table_compile():
	_, ast, plan = compile_one("CREATE TABLE student(id INT, name TEXT);")
	assert ast.table_name == 'student'
	assert plan.name == 'CreateTable'


def test_insert_compile():
	_, ast, plan = compile_one("INSERT INTO student VALUES (1, 'Alice');")
	assert plan.name == 'Insert'
	assert plan.args['values'] == [1, 'Alice']


def test_select_compile():
	_, ast, plan = compile_one("SELECT id,name FROM student WHERE id >= 1;")
	assert 'SeqScan' in repr(plan)


if __name__ == "__main__":
	test_insert_compile()
