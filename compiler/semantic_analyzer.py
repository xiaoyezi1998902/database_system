from typing import List, Union

from .catalog import Catalog, ColumnInfo
from .parser import CreateTable, Insert, Select, Delete, Update, Comparison, LogicalExpression, Join, OrderBy, GroupBy, AggregateFunction, ColumnWithAlias


class SemanticError(Exception):
	def __init__(self, message: str, line: int = None, column: int = None, expected: str = None):
		super().__init__(message)
		self.line = line
		self.column = column
		self.expected = expected


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

	def check(self, stmt: Union[CreateTable, Insert, Select, Delete, Update]) -> None:
		if isinstance(stmt, CreateTable):
			self._check_create_table(stmt)
		elif isinstance(stmt, Insert):
			self._check_insert(stmt)
		elif isinstance(stmt, Select):
			self._check_select(stmt)
		elif isinstance(stmt, Update):
			self._check_update(stmt)
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
		
		# 检查每一行的值
		for row_idx, row_values in enumerate(stmt.values):
			if expected_count != len(row_values):
				raise SemanticError(f"INSERT 第{row_idx+1}行值个数不一致: 期望 {expected_count}, 实际 {len(row_values)}")
			for col_idx, (v, t) in enumerate(zip(row_values, use_types)):
				if t == 'INT' and not isinstance(v, int):
					raise SemanticError(f"第{row_idx+1}行第{col_idx+1}列类型不匹配: 期望 INT, 实际 {type(v).__name__}")
				if t in ('TEXT', 'VARCHAR') and not isinstance(v, str):
					raise SemanticError(f"第{row_idx+1}行第{col_idx+1}列类型不匹配: 期望 {t}, 实际 {type(v).__name__}")

	def _check_select(self, stmt: Select) -> None:
		if not self.catalog.has_table(stmt.table_name):
			raise SemanticError(f"表不存在: {stmt.table_name}")
		tbl = self.catalog.get_table(stmt.table_name)
		
		# 构建表别名映射
		table_aliases = {}
		# 主表别名映射
		if stmt.table_alias:
			table_aliases[stmt.table_alias] = stmt.table_name
		table_aliases[stmt.table_name] = stmt.table_name  # 主表
		
		# 检查JOIN表并构建别名映射
		for join in stmt.joins:
			if not self.catalog.has_table(join.table_name):
				raise SemanticError(f"JOIN表不存在: {join.table_name}")
			join_tbl = self.catalog.get_table(join.table_name)
			# JOIN表别名映射
			if join.table_alias:
				table_aliases[join.table_alias] = join.table_name
			table_aliases[join.table_name] = join.table_name
		
		# 检查主表的列
		if stmt.columns != ['*']:
			for col in stmt.columns:
				if isinstance(col, str):
					self._check_column_reference(col, table_aliases)
				elif isinstance(col, AggregateFunction):
					if col.column is not None:
						self._check_column_reference(col.column, table_aliases)
				elif isinstance(col, ColumnWithAlias):
					self._check_column_reference(col.column, table_aliases)
		
		# 检查JOIN条件
		for join in stmt.joins:
			join_tbl = self.catalog.get_table(join.table_name)
			self._check_condition(join.on_condition, table_aliases)
		
		# 检查WHERE条件
		if stmt.where is not None:
			self._check_condition(stmt.where, table_aliases)
		
		# 检查GROUP BY
		if stmt.group_by is not None:
			for col in stmt.group_by.columns:
				self._check_column_reference(col, table_aliases)
			if stmt.group_by.having is not None:
				self._check_condition(stmt.group_by.having, table_aliases)
		
		# 检查ORDER BY
		for order in stmt.order_by:
			self._check_column_reference(order.column, table_aliases)

	def _check_update(self, stmt: Update) -> None:
		if not self.catalog.has_table(stmt.table_name):
			raise SemanticError(f"表不存在: {stmt.table_name}")
		tbl = self.catalog.get_table(stmt.table_name)
		column_set = {c.name for c in tbl.columns}
		
		# 检查SET子句中的列
		for column, value in stmt.set_clause:
			if column not in column_set:
				raise SemanticError(f"列不存在: {column}")
			# 检查类型匹配
			col_index = [c.name for c in tbl.columns].index(column)
			col_type = tbl.columns[col_index].type_name.upper()
			if col_type == 'INT' and not isinstance(value, int):
				raise SemanticError(f"类型不匹配: 期望 INT, 实际 {type(value).__name__}")
			if col_type in ('TEXT', 'VARCHAR') and not isinstance(value, str):
				raise SemanticError(f"类型不匹配: 期望 {col_type}, 实际 {type(value).__name__}")
		
		# 检查WHERE子句
		if stmt.where is not None:
			self._check_condition(stmt.where, column_set, tbl)

	def _check_delete(self, stmt: Delete) -> None:
		if not self.catalog.has_table(stmt.table_name):
			raise SemanticError(f"表不存在: {stmt.table_name}")
		tbl = self.catalog.get_table(stmt.table_name)
		column_set = {c.name for c in tbl.columns}
		if stmt.where is not None:
			self._check_condition(stmt.where, column_set, tbl)

	def _check_condition(self, condition, table_aliases_or_column_set, tbl=None) -> None:
		"""检查条件，支持Comparison和LogicalExpression"""
		if isinstance(condition, Comparison):
			self._check_comparison(condition, table_aliases_or_column_set, tbl)
		elif isinstance(condition, LogicalExpression):
			# 递归检查左右子条件
			self._check_condition(condition.left, table_aliases_or_column_set, tbl)
			self._check_condition(condition.right, table_aliases_or_column_set, tbl)
		else:
			raise SemanticError(f"不支持的条件类型: {type(condition)}")

	def _check_comparison(self, comp: Comparison, table_aliases_or_column_set, tbl=None) -> None:
		"""检查比较条件"""
		if isinstance(table_aliases_or_column_set, dict):
			# 多表查询，使用table_aliases
			self._check_column_reference(comp.left, table_aliases_or_column_set)
			if isinstance(comp.right, str):
				self._check_column_reference(comp.right, table_aliases_or_column_set)
		else:
			# 单表查询，使用column_set
			column_set = table_aliases_or_column_set
			if comp.left not in column_set:
				raise SemanticError(f"列不存在: {comp.left}")
			
			# 类型匹配
			col_index = [c.name for c in tbl.columns].index(comp.left)
			col_type = tbl.columns[col_index].type_name.upper()
			if col_type == 'INT' and not isinstance(comp.right, int):
				raise SemanticError("WHERE 类型不匹配: 期望 INT")
			if col_type in ('TEXT', 'VARCHAR') and not isinstance(comp.right, str):
				raise SemanticError("WHERE 类型不匹配: 期望 TEXT/VARCHAR")

	def _check_column_reference(self, column_ref: str, table_aliases: dict) -> None:
		"""检查列引用是否有效"""
		if '.' in column_ref:
			# 处理table.column格式
			table_alias, column_name = column_ref.split('.', 1)
			if table_alias not in table_aliases:
				raise SemanticError(f"表别名不存在: {table_alias}")
			
			actual_table = table_aliases[table_alias]
			if not self.catalog.has_table(actual_table):
				raise SemanticError(f"表不存在: {actual_table}")
			
			table = self.catalog.get_table(actual_table)
			column_names = {c.name for c in table.columns}
			if column_name not in column_names:
				raise SemanticError(f"列不存在: {column_ref}")
		else:
			# 处理简单列名，需要在所有表中查找
			found = False
			for table_name in table_aliases.values():
				if self.catalog.has_table(table_name):
					table = self.catalog.get_table(table_name)
					column_names = {c.name for c in table.columns}
					if column_ref in column_names:
						found = True
						break
			
			if not found:
				raise SemanticError(f"列不存在: {column_ref}")
