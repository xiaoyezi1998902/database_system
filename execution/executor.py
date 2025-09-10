from typing import Any, Dict, Iterable, Optional

from .operators import SeqScan, Filter, Project, Insert as OpInsert, CreateTable as OpCreateTable, Delete as OpDelete
from .system_catalog import SystemCatalog
from storage.disk_manager import DiskManager
from storage.buffer_manager import BufferManager
from storage.table import TableStorage


class Executor:
	def __init__(self, catalog: SystemCatalog, buffer: BufferManager, disk: DiskManager):
		self.catalog = catalog
		self.buffer = buffer
		self.disk = disk

	def _table(self, name: str) -> TableStorage:
		return TableStorage(name, self.disk, self.buffer)

	def execute_plan(self, plan) -> Any:
		name = plan.name
		if name == 'CreateTable':
			op = OpCreateTable(self.catalog, plan.args['table'], plan.args['columns'])
			return op.execute()
		if name == 'Insert':
			tbl = self._table(plan.args['table'])
			op = OpInsert(tbl, self.catalog, plan.args['table'], plan.args.get('columns'), plan.args['values'])
			return op.execute()
		if name == 'Delete':
			child = plan.children[0]
			# Delete 作用表由其下游 SeqScan 提供
			under = child
			while under.name != 'SeqScan':
				under = under.children[0]
			tbl = self._table(under.args['table'])
			pred = child.args['predicate'] if child.name == 'Filter' else None
			op = OpDelete(tbl, pred)
			# 为一致性，Delete 直接在表上做标记删除
			return op.execute()
		# 其余（Select 管道）
		root_op = self._build_pipeline(plan)
		if root_op is None:
			return []
		return list(root_op.execute())

	def _build_pipeline(self, node):
		if node.name == 'SeqScan':
			return SeqScan(self._table(node.args['table']))
		if node.name == 'Filter':
			child = self._build_pipeline(node.children[0])
			return Filter(child, node.args['predicate'])
		if node.name == 'Project':
			child = self._build_pipeline(node.children[0])
			return Project(child, node.args['columns'])
		return None

	def _materialize_child(self, node) -> Iterable[Dict[str, Any]]:
		op = self._build_pipeline(node)
		return [] if op is None else list(op.execute())

	# 兼容调用名
	def execute(self, plan) -> Any:
		return self.execute_plan(plan)
