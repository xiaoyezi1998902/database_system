from typing import Dict, Any, Iterable, List, Optional

from .system_catalog import SystemCatalog
from storage.table import TableStorage


def eval_predicate(row: Dict[str, Any], left: str, op: str, right: Any) -> bool:
	val = row.get(left)
	if op == '=':
		return val == right
	if op in ('<>', '!='):
		return val != right
	if op == '<':
		return val < right
	if op == '>':
		return val > right
	if op == '<=':
		return val <= right
	if op == '>=':
		return val >= right
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
	def __init__(self, child, columns: List[str]):
		self.child = child
		self.columns = columns

	def execute(self) -> Iterable[Dict[str, Any]]:
		for row in self.child.execute():
			yield {c: row.get(c) for c in self.columns}


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
		row: Dict[str, Any] = {c['name']: None for c in cols_meta}
		for name, val in zip(col_names, self.values):
			row[name] = val
		self.table.append_row(row)
		return 1


class CreateTable:
	def __init__(self, catalog: SystemCatalog, table_name: str, columns: List[Dict[str, str]]):
		self.catalog = catalog
		self.table_name = table_name
		self.columns = columns

	def execute(self) -> None:
		self.catalog.create_table(self.table_name, self.columns)
		return None


class Delete:
	def __init__(self, table: TableStorage, predicate: Optional[Dict[str, Any]]):
		self.table = table
		self.predicate = predicate

	def execute(self) -> int:
		if self.predicate is None:
			return self.table.delete_where(lambda r: True)
		left = self.predicate['left']
		op = self.predicate['op']
		right = self.predicate['right']
		return self.table.delete_where(lambda r: eval_predicate(r, left, op, right))
