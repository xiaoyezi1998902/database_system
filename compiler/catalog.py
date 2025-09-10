from dataclasses import dataclass
from typing import Dict, List


@dataclass
class ColumnInfo:
	name: str
	type_name: str  # INT / TEXT / VARCHAR


@dataclass
class TableInfo:
	name: str
	columns: List[ColumnInfo]


class Catalog:
	"""Simple in-memory catalog for the compiler's semantic checks."""
	def __init__(self):
		self.tables: Dict[str, TableInfo] = {}

	def create_table(self, name: str, columns: List[ColumnInfo]) -> None:
		upper = name.lower()
		if upper in self.tables:
			raise ValueError(f"表已存在: {name}")
		self.tables[upper] = TableInfo(name, columns)

	def has_table(self, name: str) -> bool:
		return name.lower() in self.tables

	def get_table(self, name: str) -> TableInfo:
		return self.tables[name.lower()]
