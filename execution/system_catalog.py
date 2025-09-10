from dataclasses import dataclass
from typing import Dict, List


@dataclass
class Column:
	name: str
	type: str  # INT / TEXT / VARCHAR


class SystemCatalog:
	"""运行时系统目录：维护表的列信息；简单保存在内存。"""

	def __init__(self):
		self.tables: Dict[str, List[Column]] = {}

	def create_table(self, table: str, columns: List[Dict[str, str]]):
		if table in self.tables:
			raise ValueError(f"表已存在: {table}")
		self.tables[table] = [Column(c["name"], c["type"].upper()) for c in columns]

	def list_tables(self) -> List[str]:
		return list(self.tables.keys())

	def get_table_columns(self, table: str) -> List[Dict[str, str]]:
		cols = self.tables.get(table, [])
		return [{"name": c.name, "type": c.type} for c in cols]
