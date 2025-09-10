import os

from execution.system_catalog import SystemCatalog
from execution.executor import Executor
from storage.disk_manager import DiskManager
from storage.buffer_manager import BufferManager
from compiler.lexer import Lexer
from compiler.parser import Parser
from compiler.semantic_analyzer import SemanticAnalyzer
from compiler.planner import Planner


def run(sql: str, catalog, executor):
	lexer = Lexer(sql)
	parser = Parser(lexer.tokenize())
	ast = parser.parse()
	SemanticAnalyzer(catalog).check(ast)
	plan = Planner().create_plan(ast)
	return executor.execute(plan)


def test_end_to_end(tmp_path):
	os.chdir(tmp_path)
	# 首次启动
	disk = DiskManager()
	buf = BufferManager(disk)
	catalog = SystemCatalog()
	exec = Executor(catalog, buf, disk)

	run("CREATE TABLE student(id INT, name TEXT);", catalog, exec)
	run("INSERT INTO student VALUES (1,'Alice');", catalog, exec)
	run("INSERT INTO student VALUES (2,'Bob');", catalog, exec)
	rows = run("SELECT id,name FROM student;", catalog, exec)
	assert len(rows) == 2

	# 删除一行
	run("DELETE FROM student WHERE id = 1;", catalog, exec)
	rows2 = run("SELECT id,name FROM student;", catalog, exec)
	assert len(rows2) == 1
