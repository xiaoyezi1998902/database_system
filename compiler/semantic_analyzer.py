from typing import List, Union

from .catalog import Catalog, ColumnInfo
from .parser import CreateTable, Insert, Select, Delete, Comparison


class SemanticError(Exception):
	pass


class SemanticAnalyzer:
	def __init__(self, runtime_catalog):
		"""
		runtime_catalog: 运行时系统目录（执行层）。
		本编译器语义检查使用一个最小适配器将其查询为编译时 Catalog。
		"""
		self.runtime_catalog = runtime_catalog
		self.catalog = self._snapshot_runtime_catalog()

	def _snapshot_runtime_catalog(self) -> Catalog:
		cat = Catalog()
		# 适配 execution.system_catalog 中的表结构（若存在）
		if hasattr(self.runtime_catalog, 'list_tables') and hasattr(self.runtime_catalog, 'get_table_columns'):
			for table_name in self.runtime_catalog.list_tables():
				cols = [ColumnInfo(c['name'], c['type']) for c in self.runtime_catalog.get_table_columns(table_name)]
				cat.create_table(table_name, cols)
		return cat

	def check(self, stmt: Union[CreateTable, Insert, Select, Delete]) -> None:
		if isinstance(stmt, CreateTable):
			self._check_create_table(stmt)
		elif isinstance(stmt, Insert):
			self._check_insert(stmt)
		elif isinstance(stmt, Select):
			self._check_select(stmt)
		elif isinstance(stmt, Delete):
			self._check_delete(stmt)
		else:
			raise SemanticError("不支持的语句")

	def _check_create_table(self, stmt: CreateTable) -> None:
		# 名称重复检查（仅在当前快照中）
		if self.catalog.has_table(stmt.table_name):
			raise SemanticError(f"表已存在: {stmt.table_name}")
		# 列重复 / 基本类型校验
		seen = set()
		for col in stmt.columns:
			if col.name.lower() in seen:
				raise SemanticError(f"重复列名: {col.name}")
			seen.add(col.name.lower())
			type_up = col.type_name.upper()
			if type_up not in ("INT", "TEXT", "VARCHAR"):
				raise SemanticError(f"不支持的类型: {col.type_name}")

	def _check_insert(self, stmt: Insert) -> None:
		if not self.catalog.has_table(stmt.table_name):
			raise SemanticError(f"表不存在: {stmt.table_name}")
		tbl = self.catalog.get_table(stmt.table_name)
		column_order = [c.name for c in tbl.columns]
		if stmt.columns is None:
			expected_count = len(column_order)
			use_types = [c.type_name.upper() for c in tbl.columns]
		else:
			expected_count = len(stmt.columns)
			# 列存在性与顺序映射
			index_map = []
			for name in stmt.columns:
				if name not in column_order:
					raise SemanticError(f"列不存在: {name}")
				index_map.append(column_order.index(name))
			use_types = [tbl.columns[i].type_name.upper() for i in index_map]
		if expected_count != len(stmt.values):
			raise SemanticError(f"INSERT 值个数不一致: 期望 {expected_count}, 实际 {len(stmt.values)}")
		for v, t in zip(stmt.values, use_types):
			if t == 'INT' and not isinstance(v, int):
				raise SemanticError(f"类型不匹配: 期望 INT, 实际 {type(v).__name__}")
			if t in ('TEXT', 'VARCHAR') and not isinstance(v, str):
				raise SemanticError(f"类型不匹配: 期望 {t}, 实际 {type(v).__name__}")

	def _check_select(self, stmt: Select) -> None:
		if not self.catalog.has_table(stmt.table_name):
			raise SemanticError(f"表不存在: {stmt.table_name}")
		tbl = self.catalog.get_table(stmt.table_name)
		column_set = {c.name for c in tbl.columns}
		if stmt.columns != ['*']:
			for name in stmt.columns:
				if name not in column_set:
					raise SemanticError(f"列不存在: {name}")
		if stmt.where is not None:
			self._check_comparison(stmt.where, column_set, tbl)

	def _check_delete(self, stmt: Delete) -> None:
		if not self.catalog.has_table(stmt.table_name):
			raise SemanticError(f"表不存在: {stmt.table_name}")
		tbl = self.catalog.get_table(stmt.table_name)
		column_set = {c.name for c in tbl.columns}
		if stmt.where is not None:
			self._check_comparison(stmt.where, column_set, tbl)

	def _check_comparison(self, comp: Comparison, column_set, tbl) -> None:
		if comp.left not in column_set:
			raise SemanticError(f"列不存在: {comp.left}")
		# 类型匹配
		col_index = [c.name for c in tbl.columns].index(comp.left)
		col_type = tbl.columns[col_index].type_name.upper()
		if col_type == 'INT' and not isinstance(comp.right, int):
			raise SemanticError("WHERE 类型不匹配: 期望 INT")
		if col_type in ('TEXT', 'VARCHAR') and not isinstance(comp.right, str):
			raise SemanticError("WHERE 类型不匹配: 期望 TEXT/VARCHAR")
