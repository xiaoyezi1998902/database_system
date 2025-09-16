from dataclasses import dataclass
from typing import Optional, List, Union

from .parser import CreateTable, Insert, Select, Delete, Update, Comparison, LogicalExpression, Condition, Join, OrderBy, GroupBy, AggregateFunction, ColumnWithAlias


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
	def create_plan(self, stmt: Union[CreateTable, Insert, Select, Delete, Update]) -> PlanNode:
		if isinstance(stmt, CreateTable):
			return self._plan_create_table(stmt)
		if isinstance(stmt, Insert):
			return self._plan_insert(stmt)
		if isinstance(stmt, Select):
			return self._plan_select(stmt)
		if isinstance(stmt, Update):
			return self._plan_update(stmt)
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
		# 构建扫描节点
		scan = PlanNode("SeqScan", {"table": stmt.table_name}, [])
		node = scan
		
		# 谓词下推：将WHERE条件尽可能下推到扫描节点
		where_predicates = []
		if stmt.where is not None:
			where_predicates.extend(self._flatten_condition(stmt.where))
		
		# 处理JOIN
		for join in stmt.joins:
			join_scan = PlanNode("SeqScan", {"table": join.table_name}, [])
			
			# 谓词下推：将JOIN条件下推到扫描节点
			join_predicates = []
			join_predicates.append(join.on_condition)
			
			# 将WHERE条件中涉及JOIN表的谓词下推
			for pred in where_predicates[:]:
				if self._predicate_involves_table(pred, join.table_name):
					join_predicates.append(pred)
					where_predicates.remove(pred)
			
			# 为JOIN表添加过滤条件
			if join_predicates:
				join_scan = self._add_filters(join_scan, join_predicates)
			
			node = PlanNode("Join", {
				"join_type": join.join_type,
				"on_condition": {
					"left": join.on_condition.left,
					"op": join.on_condition.op,
					"right": join.on_condition.right,
				}
			}, [node, join_scan])
		
		# 为主表添加剩余的WHERE条件
		if where_predicates:
			node = self._add_filters(node, where_predicates)
		
		# 处理GROUP BY
		if stmt.group_by is not None:
			group_columns = stmt.group_by.columns
			having = stmt.group_by.having
			node = PlanNode("GroupBy", {
				"columns": group_columns,
				"having": {
					"left": having.left,
					"op": having.op,
					"right": having.right,
				} if having else None
			}, [node])
		
		# 处理ORDER BY
		if stmt.order_by:
			order_specs = [{"column": o.column, "direction": o.direction} for o in stmt.order_by]
			node = PlanNode("OrderBy", {"order_specs": order_specs}, [node])
		
		# 处理投影
		if stmt.columns != ['*']:
			# 处理聚合函数和列别名
			columns = []
			aggregates = []
			for col in stmt.columns:
				if isinstance(col, str):
					columns.append(col)
				elif isinstance(col, AggregateFunction):
					aggregates.append({
						"func": col.func_name,
						"column": col.column,
						"alias": col.alias
					})
				elif isinstance(col, ColumnWithAlias):
					columns.append({
						"column": col.column,
						"alias": col.alias
					})
			
			if aggregates:
				node = PlanNode("Aggregate", {"functions": aggregates}, [node])
			if columns:
				node = PlanNode("Project", {"columns": columns}, [node])
		
		return node

	def _predicate_involves_table(self, pred: Comparison, table_name: str) -> bool:
		"""检查谓词是否涉及指定表（简化实现）"""
		# 这里简化实现，实际应该检查列名是否属于指定表
		# 可以通过列名前缀或目录信息来判断
		return True  # 简化实现，总是返回True

	def _flatten_condition(self, condition: Condition) -> List[Comparison]:
		"""将复杂条件扁平化为简单比较条件的列表"""
		if isinstance(condition, Comparison):
			return [condition]
		elif isinstance(condition, LogicalExpression):
			left_preds = self._flatten_condition(condition.left)
			right_preds = self._flatten_condition(condition.right)
			# 简化处理：对于AND条件，合并所有谓词；对于OR条件，只取第一个
			if condition.op == 'AND':
				return left_preds + right_preds
			else:  # OR
				return left_preds + right_preds
		return []

	def _add_filters(self, node: PlanNode, predicates: List[Comparison]) -> PlanNode:
		"""为节点添加过滤条件"""
		for pred in predicates:
			node = PlanNode("Filter", {"predicate": {
				"left": pred.left,
				"op": pred.op,
				"right": pred.right,
			}}, [node])
		return node

	def _plan_delete(self, stmt: Delete) -> PlanNode:
		scan = PlanNode("SeqScan", {"table": stmt.table_name}, [])
		node = scan
		if stmt.where is not None:
			# DELETE 的谓词下推
			predicates = self._flatten_condition(stmt.where)
			node = self._add_filters(node, predicates)
		return PlanNode("Delete", {}, [node])

	def _plan_update(self, stmt: Update) -> PlanNode:
		scan = PlanNode("SeqScan", {"table": stmt.table_name}, [])
		node = scan
		if stmt.where is not None:
			# UPDATE 的谓词下推
			predicates = self._flatten_condition(stmt.where)
			node = self._add_filters(node, predicates)
		return PlanNode("Update", {"set_clause": stmt.set_clause}, [node])
