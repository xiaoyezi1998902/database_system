from __future__ import annotations

import os
import signal
import atexit
from typing import List, Dict, Any
import re

from flask import Flask, request, jsonify, send_from_directory

from compiler.lexer import Lexer, LexError
from compiler.parser import Parser, ParseError
from compiler.semantic_analyzer import SemanticAnalyzer, SemanticError
from compiler.planner import Planner
from execution.executor import Executor
from execution.system_catalog import SystemCatalog
from storage.disk_manager import DiskManager
from storage.buffer_manager import BufferManager
from storage.table import TableStorage

app = Flask(__name__, static_folder="web", static_url_path="/")

# 初始化后端数据库组件（进程内简化实例）
disk_manager = DiskManager()
buffer_manager = BufferManager(disk_manager)

# 创建系统目录表的存储
from storage.table import TableStorage

catalog_storage = TableStorage("pg_catalog", disk_manager, buffer_manager)
catalog = SystemCatalog(catalog_storage)
executor = Executor(catalog, buffer_manager, disk_manager)


def split_sql(sql_text: str) -> List[str]:
    stmt = []
    in_string = False
    escape = False
    line_num = 1

    for ch in sql_text:
        stmt.append(ch)
        if ch == "'" and not escape:
            in_string = not in_string
        escape = (ch == '\\' and not escape)
        if ch == '\n':
            line_num += 1
        if ch == ';' and not in_string:
            sql = ''.join(stmt).strip()
            if sql:
                # 为每个语句添加行数信息（用于错误定位）
                yield sql
            stmt = []
    tail = ''.join(stmt).strip()
    if tail:
        yield tail


def compile_and_execute(sql: str) -> Dict[str, Any]:
    lexer = Lexer(sql)
    tokens = lexer.tokenize()
    parser = Parser(tokens)
    ast = parser.parse()
    SemanticAnalyzer(catalog).check(ast)
    plan = Planner().create_plan(ast)
    result = executor.execute(plan)
    # 统一返回格式
    payload: Dict[str, Any] = {
        "tokens": [repr(t) for t in tokens],
        "ast": repr(ast),
        "plan": repr(plan),
    }
    if isinstance(result, int):
        payload["resultType"] = "count"
        payload["count"] = result
    elif isinstance(result, list):
        payload["resultType"] = "rows"
        payload["rows"] = result
    elif result is None:
        table_name = plan.args['table']
        payload["resultType"] = f"{table_name} 表创建成功"
    else:
        payload["resultType"] = "other"
        payload["value"] = result
    return payload


def format_error(err: Exception) -> Dict[str, Any]:
    msg = str(err)
    etype = err.__class__.__name__
    line = None
    column = None
    expected = None

    # 优先使用异常对象中的位置信息
    if hasattr(err, 'line'):
        line = err.line
    if hasattr(err, 'column'):
        column = err.column
    if hasattr(err, 'expected'):
        expected = err.expected

    # 如果异常对象中没有位置信息，尝试从消息中解析
    if line is None or column is None:
        m = re.search(r"行\s*(\d+)\s*列\s*(\d+)", msg)
        if m:
            line = int(m.group(1))
            column = int(m.group(2))

    return {
        "errorType": etype,
        "message": msg,
        "line": line,
        "column": column,
        "expected": expected,
    }


@app.post("/execute")
def http_execute():
    data = request.get_json(force=True, silent=True) or {}
    sql = data.get("sql", "")
    if not isinstance(sql, str) or not sql.strip():
        return jsonify({"error": "SQL 不能为空"}), 400
    
    results: List[Dict[str, Any]] = []
    
    for stmt in split_sql(sql):
        try:
            results.append(compile_and_execute(stmt))
        except (LexError, ParseError, SemanticError, Exception) as e:
            results.append({
                "error": format_error(e),
                "statement": stmt,
            })
    return jsonify({"ok": True, "results": results})


@app.get("/table/<name>")
def http_table_rows(name: str):
    limit = request.args.get("limit", default=100, type=int)
    if name not in catalog.list_tables():
        return jsonify({"ok": False, "error": f"表不存在: {name}"}), 404
    storage = TableStorage(name, disk_manager, buffer_manager)
    rows = []
    for r in storage.seq_scan():
        rows.append(r)
        if len(rows) >= limit:
            break
    return jsonify({"ok": True, "table": name, "rows": rows, "count": len(rows)})


@app.get("/schema")
def http_schema():
    tables = []
    for t in catalog.list_tables():
        tables.append({
            "name": t,
            "columns": catalog.get_table_columns(t),
        })
    return jsonify({"tables": tables})


@app.get("/")
def index():
    return send_from_directory("web", "index.html")


@app.get("/stats")
def http_stats():
    """获取缓存统计信息"""
    try:
        stats = buffer_manager.get_stats()
        return jsonify({"ok": True, "stats": stats})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@app.post("/stats/reset")
def http_reset_stats():
    """重置缓存统计信息"""
    try:
        buffer_manager.reset_stats()
        return jsonify({"ok": True, "message": "统计信息已重置"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@app.get("/stats/evict-log")
def http_evict_log():
    """获取完整的驱逐日志"""
    try:
        evict_log = buffer_manager.get_full_evict_log()
        return jsonify({"ok": True, "evict_log": evict_log})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


def cleanup():
    """程序退出时清理缓存数据"""
    try:
        buffer_manager.flush_all()
        print("缓存数据已写入磁盘")
    except Exception as e:
        print(f"清理缓存时出错: {e}")


def signal_handler(signum, frame):
    """信号处理器"""
    print(f"\n收到信号 {signum}，正在清理...")
    cleanup()
    exit(0)


def main():
    # 注册清理函数
    atexit.register(cleanup)
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    port = int(os.environ.get("PORT", "8000"))
    app.run(host="127.0.0.1", port=port, debug=True)


if __name__ == "__main__":
    main()
