from dataclasses import dataclass
from typing import Dict, List, Optional
import json

@dataclass
class Column:
	name: str
	type: str  # INT / TEXT / VARCHAR

class SystemCatalog:
	"""系统目录：作为特殊表 pg_catalog 存储在存储引擎中。"""

	def __init__(self, table_storage):
		self.table_storage = table_storage
		self.catalog_table_name = "pg_catalog"
		self._init_catalog_table()

	def _init_catalog_table(self):
		"""初始化系统目录表"""
		# 检查目录表是否已存在
		existing_tables = []
		for row in self.table_storage.seq_scan():
			existing_tables.append(row.get("table_name", ""))
			if self.catalog_table_name == row.get("table_name", ""):
				self._memory_tables = row
		
		if self.catalog_table_name not in existing_tables:
			# 创建目录表结构（自引用）
			catalog_columns = [
				{"name": "table_name", "type": "TEXT"},
				{"name": "column_name", "type": "TEXT"},
				{"name": "column_type", "type": "TEXT"},
				{"name": "column_order", "type": "INT"}
			]
			# 直接注册到内存中，避免循环依赖
			self._memory_tables = {}
			self._register_table(self.catalog_table_name, catalog_columns)

	def _register_table(self, table_name: str, columns: List[Dict[str, str]]):
		"""在内存中注册表结构"""
		if table_name not in self._memory_tables:
			self._memory_tables[table_name] = []
		
		for i, col in enumerate(columns):
			# 将表结构信息写入目录表
			catalog_row = {
				"table_name": table_name,
				"column_name": col["name"],
				"column_type": col["type"].upper(),
				"column_order": i
			}
			self.table_storage.append_row(catalog_row)

	def create_table(self, table: str, columns: List[Dict[str, str]]):
		"""创建表并注册到系统目录"""
		if self.has_table(table):
			raise ValueError(f"表已存在: {table}")
		self._register_table(table, columns)

	def has_table(self, table_name: str) -> bool:
		"""检查表是否存在"""
		for row in self.table_storage.seq_scan():
			if row.get("table_name") == table_name:
				return True
		return False

	def list_tables(self) -> List[str]:
		"""获取所有表名"""
		tables = set()
		for row in self.table_storage.seq_scan():
			table_name = row.get("table_name")
			if table_name and table_name != self.catalog_table_name:
				tables.add(table_name)
		return list(tables)

	def get_table_columns(self, table: str) -> List[Dict[str, str]]:
		"""获取表的列信息"""
		columns = []
		for row in self.table_storage.seq_scan():
			if row.get("table_name") == table:
				columns.append({
					"name": row.get("column_name"),
					"type": row.get("column_type")
				})
		# 按列顺序排序
		columns.sort(key=lambda x: next(
			(row.get("column_order") for row in self.table_storage.seq_scan() 
			 if row.get("table_name") == table and row.get("column_name") == x["name"]), 0
		))
		return columns

	def drop_table(self, table_name: str):
		"""删除表（从目录中移除）"""
		if not self.has_table(table_name):
			raise ValueError(f"表不存在: {table_name}")
		
		# 删除目录表中的相关记录
		rows_to_delete = []
		for row in self.table_storage.seq_scan():
			if row.get("table_name") == table_name:
				rows_to_delete.append(row)
		
		# 标记删除
		for row in rows_to_delete:
			row["__deleted__"] = True