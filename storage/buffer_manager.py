from collections import OrderedDict
from dataclasses import dataclass
from typing import Tuple

from .page import Page
from .disk_manager import DiskManager


PageKey = Tuple[str, int]  # (table_name, page_id)


@dataclass
class Frame:
	page: Page
	dirty: bool = False


class BufferManager:
	"""LRU 页面缓存。"""

	def __init__(self, disk: DiskManager, capacity: int = 64):
		self.disk = disk
		self.capacity = capacity
		self.cache: OrderedDict[PageKey, Frame] = OrderedDict()

	def _evict_if_needed(self):
		while len(self.cache) > self.capacity:
			key, frame = self.cache.popitem(last=False)
			if frame.dirty:
				self.disk.write_page(key[0], key[1], frame.page)

	def get_page(self, table: str, page_id: int) -> Page:
		key = (table, page_id)
		if key in self.cache:
			frame = self.cache.pop(key)
			self.cache[key] = frame
			return frame.page
		page = self.disk.read_page(table, page_id)
		self.cache[key] = Frame(page, dirty=False)
		self._evict_if_needed()
		return page

	def new_page(self, table: str) -> Tuple[int, Page]:
		page_id = self.disk.allocate_page(table)
		page = Page()
		self.cache[(table, page_id)] = Frame(page, dirty=True)
		self._evict_if_needed()
		return page_id, page

	def mark_dirty(self, table: str, page_id: int) -> None:
		key = (table, page_id)
		if key in self.cache:
			self.cache[key].dirty = True

	def flush_page(self, table: str, page_id: int) -> None:
		key = (table, page_id)
		frame = self.cache.get(key)
		if frame is not None and frame.dirty:
			self.disk.write_page(table, page_id, frame.page)
			frame.dirty = False

	def flush_all(self) -> None:
		for (table, page_id), frame in list(self.cache.items()):
			if frame.dirty:
				self.disk.write_page(table, page_id, frame.page)
				frame.dirty = False
