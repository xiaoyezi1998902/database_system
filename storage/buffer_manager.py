from collections import OrderedDict
from dataclasses import dataclass
from typing import Tuple
import json
import os
import time

from .page import Page
from .disk_manager import DiskManager


PageKey = Tuple[str, int]  # (table_name, page_id)


@dataclass
class Frame:
	page: Page
	dirty: bool = False


class BufferManager:
	"""LRU 页面缓存，带统计功能。"""

	def __init__(self, disk: DiskManager, capacity: int = 64):
		self.disk = disk
		self.capacity = capacity
		self.cache: OrderedDict[PageKey, Frame] = OrderedDict()
		
		# 统计信息
		self.hit_count = 0
		self.miss_count = 0
		self.evict_count = 0
		self.evict_log = []
		
		# 日志文件路径
		self.log_dir = "logs"
		self.stats_file = os.path.join(self.log_dir, "buffer_stats.json")
		self.evict_log_file = os.path.join(self.log_dir, "evict_log.json")
		
		# 确保日志目录存在
		os.makedirs(self.log_dir, exist_ok=True)
		
		# 加载历史统计数据
		self._load_stats()

	def _evict_if_needed(self):
		while len(self.cache) > self.capacity:
			key, frame = self.cache.popitem(last=False)
			if frame.dirty:
				self.disk.write_page(key[0], key[1], frame.page)
			# 记录驱逐日志
			self.evict_count += 1
			evict_entry = {
				"timestamp": time.time(),
				"table": key[0],
				"page_id": key[1],
				"dirty": frame.dirty,
				"reason": "capacity_exceeded"
			}
			self.evict_log.append(evict_entry)
			# 写入驱逐日志文件
			self._append_evict_log(evict_entry)

	def get_page(self, table: str, page_id: int) -> Page:
		key = (table, page_id)
		if key in self.cache:
			# 缓存命中
			self.hit_count += 1
			frame = self.cache.pop(key)
			self.cache[key] = frame
			return frame.page
		else:
			# 缓存未命中
			self.miss_count += 1
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

	def get_stats(self) -> dict:
		"""获取缓存统计信息"""
		total_requests = self.hit_count + self.miss_count
		hit_rate = self.hit_count / total_requests if total_requests > 0 else 0
		stats = {
			"hit_count": self.hit_count,
			"miss_count": self.miss_count,
			"hit_rate": hit_rate,
			"evict_count": self.evict_count,
			"cache_size": len(self.cache),
			"cache_capacity": self.capacity,
			"evict_log": self.evict_log[-10:]  # 最近10条驱逐记录
		}
		# 保存统计数据到文件
		self._save_stats(stats)
		return stats

	def reset_stats(self) -> None:
		"""重置统计信息"""
		self.hit_count = 0
		self.miss_count = 0
		self.evict_count = 0
		self.evict_log = []
		# 重置文件中的统计数据
		self._save_stats({
			"hit_count": 0,
			"miss_count": 0,
			"hit_rate": 0,
			"evict_count": 0,
			"cache_size": len(self.cache),
			"cache_capacity": self.capacity,
			"evict_log": []
		})
		# 清空驱逐日志文件
		self._clear_evict_log()
	
	def _load_stats(self) -> None:
		"""从文件加载历史统计数据"""
		try:
			if os.path.exists(self.stats_file):
				with open(self.stats_file, 'r', encoding='utf-8') as f:
					stats = json.load(f)
					self.hit_count = stats.get('hit_count', 0)
					self.miss_count = stats.get('miss_count', 0)
					self.evict_count = stats.get('evict_count', 0)
			
			if os.path.exists(self.evict_log_file):
				with open(self.evict_log_file, 'r', encoding='utf-8') as f:
					self.evict_log = json.load(f)
		except (json.JSONDecodeError, IOError) as e:
			print(f"加载统计数据失败: {e}")
			# 如果加载失败，使用默认值
			self.hit_count = 0
			self.miss_count = 0
			self.evict_count = 0
			self.evict_log = []
	
	def _save_stats(self, stats: dict) -> None:
		"""保存统计数据到文件"""
		try:
			# 添加时间戳
			stats['last_updated'] = time.time()
			with open(self.stats_file, 'w', encoding='utf-8') as f:
				json.dump(stats, f, indent=2, ensure_ascii=False)
		except IOError as e:
			print(f"保存统计数据失败: {e}")
	
	def _append_evict_log(self, entry: dict) -> None:
		"""追加驱逐记录到日志文件"""
		try:
			# 读取现有日志
			logs = []
			if os.path.exists(self.evict_log_file):
				with open(self.evict_log_file, 'r', encoding='utf-8') as f:
					logs = json.load(f)
			
			# 添加新记录
			logs.append(entry)
			
			# 只保留最近1000条记录
			if len(logs) > 1000:
				logs = logs[-1000:]
			
			# 写回文件
			with open(self.evict_log_file, 'w', encoding='utf-8') as f:
				json.dump(logs, f, indent=2, ensure_ascii=False)
		except IOError as e:
			print(f"写入驱逐日志失败: {e}")
	
	def _clear_evict_log(self) -> None:
		"""清空驱逐日志文件"""
		try:
			with open(self.evict_log_file, 'w', encoding='utf-8') as f:
				json.dump([], f, indent=2, ensure_ascii=False)
		except IOError as e:
			print(f"清空驱逐日志失败: {e}")
	
	def get_full_evict_log(self) -> list:
		"""获取完整的驱逐日志"""
		try:
			if os.path.exists(self.evict_log_file):
				with open(self.evict_log_file, 'r', encoding='utf-8') as f:
					return json.load(f)
		except (json.JSONDecodeError, IOError) as e:
			print(f"读取完整驱逐日志失败: {e}")
		return []
