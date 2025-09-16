from typing import Dict, List, Any, Iterable, Optional

from .buffer_manager import BufferManager
from .disk_manager import DiskManager
from .page import Page


class TableStorage:
	"""表的物理存储：基于页的行追加与顺序扫描。"""

	def __init__(self, table_name: str, disk: DiskManager, buffer: BufferManager):
		self.table_name = table_name
		self.disk = disk
		self.buffer = buffer

	def append_row(self, row: Dict[str, Any]) -> None:
		# 尝试向最后一页追加，否则分配新页
		num_pages = self.disk.get_num_pages(self.table_name)
		if num_pages == 0:
			page_id, page = self.buffer.new_page(self.table_name)
		else:
			page_id = num_pages - 1
			page = self.buffer.get_page(self.table_name, page_id)
		if not page.try_append_row(row):
			page_id, page = self.buffer.new_page(self.table_name)
			ok = page.try_append_row(row)
			assert ok
		self.buffer.mark_dirty(self.table_name, page_id)

	def seq_scan(self) -> Iterable[Dict[str, Any]]:
		num_pages = self.disk.get_num_pages(self.table_name)
		for pid in range(num_pages):
			page = self.buffer.get_page(self.table_name, pid)
			for r in page.iter_live_rows():
				yield r

	def delete_where(self, predicate) -> int:
		count = 0
		num_pages = self.disk.get_num_pages(self.table_name)
		for pid in range(num_pages):
			page = self.buffer.get_page(self.table_name, pid)
			count += page.mark_deleted(predicate)
			self.buffer.mark_dirty(self.table_name, pid)
		return count

	def update_where(self, update_func, predicate) -> int:
		count = 0
		num_pages = self.disk.get_num_pages(self.table_name)
		for pid in range(num_pages):
			page = self.buffer.get_page(self.table_name, pid)
			for r in page.rows:
				if not r.get("__deleted__") and predicate(r):
					update_func(r)
					count += 1
			self.buffer.mark_dirty(self.table_name, pid)
		return count
