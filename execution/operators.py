from typing import Dict, Any, Iterable, List, Optional, Union

from .system_catalog import SystemCatalog
from storage.table import TableStorage


def eval_predicate(row: Dict[str, Any], left: str, op: str, right: Any) -> bool:
	val = row.get(left)
	
	# 如果右操作数是字符串且看起来像列名，从row中获取值
	right_val = right
	if isinstance(right, str) and '.' in right:
		# 处理table.column格式的列引用
		right_val = row.get(right)
	elif isinstance(right, str) and right in row:
		# 处理简单列名
		right_val = row.get(right)
	
	if op == '=':
		return val == right_val
	if op in ('<>', '!='):
		return val != right_val
	if op == '<':
		return val < right_val
	if op == '>':
		return val > right_val
	if op == '<=':
		return val <= right_val
	if op == '>=':
		return val >= right_val
	raise ValueError(f"不支持的比较运算符: {op}")


class SeqScan:
	def __init__(self, table: TableStorage):
		self.table = table

	def execute(self) -> Iterable[Dict[str, Any]]:
		return self.table.seq_scan()


class Filter:
	def __init__(self, child, predicate: Dict[str, Any]):
		self.child = child
		self.predicate = predicate

	def execute(self) -> Iterable[Dict[str, Any]]:
		left = self.predicate['left']
		op = self.predicate['op']
		right = self.predicate['right']
		for row in self.child.execute():
			if eval_predicate(row, left, op, right):
				yield row


class Project:
	def __init__(self, child, columns: List[Union[str, Dict[str, str]]]):
		self.child = child
		self.columns = columns

	def execute(self) -> Iterable[Dict[str, Any]]:
		for row in self.child.execute():
			result = {}
			for col in self.columns:
				if isinstance(col, str):
					# 普通列名
					if col == '*':
						# 选择所有列
						result.update(row)
					else:
						result[col] = row.get(col)
				elif isinstance(col, dict):
					# 带别名的列 {"column": "name", "alias": "student_name"}
					column_name = col.get('column', '')
					alias_name = col.get('alias', column_name)
					result[alias_name] = row.get(column_name)
			yield result


class Insert:
	def __init__(self, table: TableStorage, catalog: SystemCatalog, table_name: str, columns: Optional[List[str]], values: List[Any]):
		self.table = table
		self.catalog = catalog
		self.table_name = table_name
		self.columns = columns
		self.values = values

	def execute(self) -> int:
		cols_meta = self.catalog.get_table_columns(self.table_name)
		if self.columns is None:
			col_names = [c['name'] for c in cols_meta]
		else:
			col_names = self.columns
		
		count = 0
		for row_values in self.values:
			row: Dict[str, Any] = {c['name']: None for c in cols_meta}
			for name, val in zip(col_names, row_values):
				row[name] = val
			self.table.append_row(row)
			count += 1
		return count


class CreateTable:
	def __init__(self, catalog: SystemCatalog, table_name: str, columns: List[Dict[str, str]]):
		self.catalog = catalog
		self.table_name = table_name
		self.columns = columns

	def execute(self) -> None:
		self.catalog.create_table(self.table_name, self.columns)
		return None


class Delete:
	def __init__(self, table: TableStorage, predicates: Optional[List[Dict[str, Any]]]):
		self.table = table
		self.predicates = predicates or []

	def execute(self) -> int:
		if not self.predicates:
			return self.table.delete_where(lambda r: True)

		def and_pred(row: Dict[str, Any]) -> bool:
			for p in self.predicates:
				if not eval_predicate(row, p['left'], p['op'], p['right']):
					return False
			return True

		return self.table.delete_where(and_pred)


class Update:
	def __init__(self, table: TableStorage, set_clause: List[tuple], predicates: Optional[List[Dict[str, Any]]]):
		self.table = table
		self.set_clause = set_clause
		self.predicates = predicates or []

	def execute(self) -> int:
		if not self.predicates:
			# 更新所有行
			def update_func(row):
				for column, value in self.set_clause:
					row[column] = value
				return True
			return self.table.update_where(update_func, lambda r: True)
		else:
			def update_func(row):
				for column, value in self.set_clause:
					row[column] = value
				return True
			
			def and_pred(row: Dict[str, Any]) -> bool:
				for p in self.predicates:
					if not eval_predicate(row, p['left'], p['op'], p['right']):
						return False
				return True
			
			return self.table.update_where(update_func, and_pred)


class OrderBy:
	def __init__(self, child, order_specs: List[Dict[str, str]]):
		self.child = child
		self.order_specs = order_specs  # [{"column": "age", "direction": "DESC"}, ...]

	def execute(self) -> Iterable[Dict[str, Any]]:
		# 收集所有数据
		rows = list(self.child.execute())
		
		# 排序
		def sort_key(row):
			key_values = []
			for spec in self.order_specs:
				column = spec['column']
				direction = spec['direction']
				value = row.get(column)
				
				# 处理None值
				if value is None:
					value = float('-inf') if direction == 'ASC' else float('inf')
				
				# 根据方向调整排序
				if direction == 'DESC':
					value = -value if isinstance(value, (int, float)) else value
				
				key_values.append(value)
			return key_values
		
		rows.sort(key=sort_key)
		return rows


class Join:
	def __init__(self, left_child, right_child, join_type: str, on_condition: Dict[str, Any]):
		self.left_child = left_child
		self.right_child = right_child
		self.join_type = join_type  # INNER, LEFT, RIGHT, OUTER
		self.on_condition = on_condition

	def execute(self) -> Iterable[Dict[str, Any]]:
		# 收集右表数据
		right_rows = list(self.right_child.execute())
		
		# 执行连接
		for left_row in self.left_child.execute():
			matched = False
			for right_row in right_rows:
				if self._matches_condition(left_row, right_row):
					matched = True
					yield {**left_row, **right_row}
			
			# 左连接：即使没有匹配也要输出左表行
			if self.join_type == 'LEFT' and not matched:
				yield {**left_row, **{k: None for k in right_rows[0].keys() if right_rows}}

	def _matches_condition(self, left_row: Dict[str, Any], right_row: Dict[str, Any]) -> bool:
		"""检查连接条件是否满足"""
		left_col = self.on_condition['left']
		op = self.on_condition['op']
		right_col = self.on_condition['right']
		
		left_val = left_row.get(left_col)
		right_val = right_row.get(right_col)
		
		return eval_predicate({left_col: left_val}, left_col, op, right_val)
