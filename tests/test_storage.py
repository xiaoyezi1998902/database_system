import os

from storage.disk_manager import DiskManager
from storage.buffer_manager import BufferManager
from storage.table import TableStorage


def test_page_io_and_lru(tmp_path):
	os.chdir(tmp_path)
	disk = DiskManager()
	buf = BufferManager(disk, capacity=2)
	# 使用表存储进行 I/O
	tbl = TableStorage('t', disk, buf)
	for i in range(50):
		tbl.append_row({"id": i})
	# 读取并验证
	vals = [r["id"] for r in tbl.seq_scan()]
	assert vals[:5] == [0, 1, 2, 3, 4]
	buf.flush_all()
