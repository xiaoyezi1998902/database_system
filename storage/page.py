from __future__ import annotations

import json
from typing import List, Dict, Any, Iterable


PAGE_SIZE = 4096


class PageFullError(Exception):
	pass


class Page:
	"""简化的页结构：使用 JSON 序列化，固定 4KB。

	- header: {"version":1, "rows": len(rows)}
	- rows: List[Dict[str, Any]]  每条记录包含列名到值的映射，以及可选 "__deleted__" 标记
	"""

	def __init__(self, rows: List[Dict[str, Any]] | None = None):
		self.rows: List[Dict[str, Any]] = rows or []

	def serialize(self) -> bytes:
		obj = {
			"version": 1,
			"rows": self.rows,
		}
		data = json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
		if len(data) > PAGE_SIZE:
			raise PageFullError("页内容超过 4KB")
		return data.ljust(PAGE_SIZE, b"\x00")

	@staticmethod
	def deserialize(data: bytes) -> Page:
		text = data.rstrip(b"\x00").decode("utf-8")
		if not text:
			return Page()
		obj = json.loads(text)
		rows = obj.get("rows", [])
		return Page(rows)

	def try_append_row(self, row: Dict[str, Any]) -> bool:
		"""尝试追加一行，若序列化后超过 4KB 则返回 False，不修改页。"""
		original = list(self.rows)
		self.rows.append(row)
		try:
			self.serialize()
			return True
		except PageFullError:
			self.rows = original
			return False

	def mark_deleted(self, predicate) -> int:
		"""根据谓词标记删除，返回删除条数。"""
		count = 0
		for r in self.rows:
			if not r.get("__deleted__") and predicate(r):
				r["__deleted__"] = True
				count += 1
		return count

	def iter_live_rows(self) -> Iterable[Dict[str, Any]]:
		for r in self.rows:
			if not r.get("__deleted__"):
				yield r
