import os
from typing import Optional

from .page import PAGE_SIZE, Page


DATA_DIR = os.path.join(os.getcwd(), 'data')


class DiskManager:
	"""极简磁盘管理：

	- 每张表一个文件：data/<table>.tbl
	- 文件按 4KB 页组织；页号从 0 起。
	"""

	def __init__(self):
		os.makedirs(DATA_DIR, exist_ok=True)

	def _table_path(self, table_name: str) -> str:
		return os.path.join(DATA_DIR, f"{table_name}.tbl")

	def allocate_page(self, table_name: str) -> int:
		path = self._table_path(table_name)
		with open(path, 'ab') as f:
			f.write(b"\x00" * PAGE_SIZE)
			page_id = (f.tell() // PAGE_SIZE) - 1
		return page_id

	def read_page(self, table_name: str, page_id: int) -> Page:
		path = self._table_path(table_name)
		with open(path, 'rb') as f:
			f.seek(page_id * PAGE_SIZE)
			data = f.read(PAGE_SIZE)
			if len(data) < PAGE_SIZE:
				raise IOError("读取页越界")
		return Page.deserialize(data)

	def write_page(self, table_name: str, page_id: int, page: Page) -> None:
		path = self._table_path(table_name)
		with open(path, 'r+b' if os.path.exists(path) else 'wb') as f:
			f.seek(page_id * PAGE_SIZE)
			f.write(page.serialize())

	def get_num_pages(self, table_name: str) -> int:
		path = self._table_path(table_name)
		if not os.path.exists(path):
			return 0
		size = os.path.getsize(path)
		return size // PAGE_SIZE
