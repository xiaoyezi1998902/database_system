from dataclasses import dataclass
from typing import Optional, List, Union

from .parser import CreateTable, Insert, Select, Delete, Comparison


# 逻辑计划节点定义（简化）
@dataclass
class PlanNode:
	name: str
	args: dict
	children: List['PlanNode']

	def __repr__(self) -> str:
		def render(node: 'PlanNode', indent: int = 0) -> str:
			pad = '  ' * indent
			line = f"{pad}{node.name} {node.args}"
			for ch in node.children:
				line += "\n" + render(ch, indent + 1)
			return line
		return render(self)


class Planner:
	def create_plan(self, stmt: Union[CreateTable, Insert, Select, Delete]) -> PlanNode:
		if isinstance(stmt, CreateTable):
			return self._plan_create_table(stmt)
		if isinstance(stmt, Insert):
			return self._plan_insert(stmt)
		if isinstance(stmt, Select):
			return self._plan_select(stmt)
		if isinstance(stmt, Delete):
			return self._plan_delete(stmt)
		raise ValueError("不支持的语句用于计划生成")

	def _plan_create_table(self, stmt: CreateTable) -> PlanNode:
		cols = [{"name": c.name, "type": c.type_name.upper()} for c in stmt.columns]
		return PlanNode("CreateTable", {"table": stmt.table_name, "columns": cols}, [])

	def _plan_insert(self, stmt: Insert) -> PlanNode:
		return PlanNode("Insert", {
			"table": stmt.table_name,
			"columns": stmt.columns,
			"values": stmt.values,
		}, [])

	def _plan_select(self, stmt: Select) -> PlanNode:
		scan = PlanNode("SeqScan", {"table": stmt.table_name}, [])
		node = scan
		if stmt.where is not None:
			node = PlanNode("Filter", {"predicate": {
				"left": stmt.where.left,
				"op": stmt.where.op,
				"right": stmt.where.right,
			}}, [node])
		if stmt.columns != ['*']:
			node = PlanNode("Project", {"columns": stmt.columns}, [node])
		return node

	def _plan_delete(self, stmt: Delete) -> PlanNode:
		scan = PlanNode("SeqScan", {"table": stmt.table_name}, [])
		node = scan
		if stmt.where is not None:
			node = PlanNode("Filter", {"predicate": {
				"left": stmt.where.left,
				"op": stmt.where.op,
				"right": stmt.where.right,
			}}, [node])
		return PlanNode("Delete", {}, [node])
