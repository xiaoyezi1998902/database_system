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


def test_insert_select_delete(tmp_path):
	os.chdir(tmp_path)
	disk = DiskManager()
	buf = BufferManager(disk)
	catalog = SystemCatalog()
	exec = Executor(catalog, buf, disk)

	run("CREATE TABLE student(id INT, name TEXT);", catalog, exec)
	assert run("INSERT INTO student VALUES (1,'A');", catalog, exec) == 1
	assert run("INSERT INTO student VALUES (2,'B');", catalog, exec) == 1
	rows = run("SELECT id,name FROM student;", catalog, exec)
	assert {"id": 1, "name": "A"} in rows and {"id": 2, "name": "B"} in rows
	assert run("DELETE FROM student WHERE id = 1;", catalog, exec) == 1
	rows2 = run("SELECT id,name FROM student;", catalog, exec)
	assert {"id": 1, "name": "A"} not in rows2
