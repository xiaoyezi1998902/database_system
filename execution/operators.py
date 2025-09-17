from typing import Dict, Any, Iterable, List, Optional, Union

from .system_catalog import SystemCatalog
from storage.table import TableStorage


def eval_predicate(row: Dict[str, Any], left: str, op: str, right: Any) -> bool:
    # 提取列名（处理table.column格式）
    left = _extract_column_name(left)

    if isinstance(right, str):
        right = _extract_column_name(right)

    val = row.get(left)

    # 如果右操作数是字符串且看起来像列名，从row中获取值
    right_val = right
    if isinstance(right, str) and '.' in right:
        # 处理table.column格式的列引用
        right_val = row.get(right)
    elif isinstance(right, str) and right in row:
        # 处理简单列名
        right_val = row.get(right)

    if op == '=':
        return val == right_val
    if op in ('<>', '!='):
        return val != right_val
    if op == '<':
        return val < right_val
    if op == '>':
        return val > right_val
    if op == '<=':
        return val <= right_val
    if op == '>=':
        return val >= right_val
    raise ValueError(f"不支持的比较运算符: {op}")


class SeqScan:
    def __init__(self, table: TableStorage):
        self.table = table

    def execute(self) -> Iterable[Dict[str, Any]]:
        return self.table.seq_scan()


class Filter:
    def __init__(self, child, predicate: Dict[str, Any]):
        self.child = child
        self.predicate = predicate

    def execute(self) -> Iterable[Dict[str, Any]]:
        left = self.predicate['left']
        op = self.predicate['op']
        right = self.predicate['right']
        for row in self.child.execute():
            if eval_predicate(row, left, op, right):
                yield row


class Project:
    def __init__(self, child, columns: List[Union[str, Dict[str, str]]]):
        self.child = child
        self.columns = columns

    def execute(self) -> Iterable[Dict[str, Any]]:
        for row in self.child.execute():
            result = {}
            for col in self.columns:
                if isinstance(col, str):
                    # 普通列名
                    if col == '*':
                        # 选择所有列
                        result.update(row)
                    else:
                        # 智能列名匹配：先尝试直接匹配，再尝试带表别名的匹配
                        value = self._get_column_value(row, col)
                        result[col] = value
                elif isinstance(col, dict):
                    # 带别名的列 {"column": "name", "alias": "student_name"}
                    column_name = col.get('column', '')
                    alias_name = col.get('alias', column_name)
                    value = self._get_column_value(row, column_name)
                    result[alias_name] = value
            yield result

    def _get_column_value(self, row: Dict[str, Any], column_name: str) -> Any:
        """智能获取列值，支持表别名前缀匹配"""
        # 1. 先尝试直接匹配
        if column_name in row:
            return row[column_name]

        # 2. 如果直接匹配失败，尝试查找带表别名前缀的键
        # 例如：查找 "name" 时，会匹配 "s.name" 或 "c.name"
        for key in row.keys():
            if key.endswith(f".{column_name}"):
                return row[key]

        # 3. 如果都找不到，返回None
        return None


class Insert:
    def __init__(self, table: TableStorage, catalog: SystemCatalog, table_name: str, columns: Optional[List[str]],
                 values: List[Any]):
        self.table = table
        self.catalog = catalog
        self.table_name = table_name
        self.columns = columns
        self.values = values

    def execute(self) -> int:
        cols_meta = self.catalog.get_table_columns(self.table_name)
        if self.columns is None:
            col_names = [c['name'] for c in cols_meta]
        else:
            col_names = self.columns

        count = 0
        for row_values in self.values:
            row: Dict[str, Any] = {c['name']: None for c in cols_meta}
            for name, val in zip(col_names, row_values):
                row[name] = val
            self.table.append_row(row)
            count += 1
        return count


class CreateTable:
    def __init__(self, catalog: SystemCatalog, table_name: str, columns: List[Dict[str, str]]):
        self.catalog = catalog
        self.table_name = table_name
        self.columns = columns

    def execute(self) -> None:
        self.catalog.create_table(self.table_name, self.columns)
        return None


class Delete:
    def __init__(self, table: TableStorage, predicates: Optional[List[Dict[str, Any]]]):
        self.table = table
        self.predicates = predicates or []

    def execute(self) -> int:
        if not self.predicates:
            return self.table.delete_where(lambda r: True)

        def and_pred(row: Dict[str, Any]) -> bool:
            for p in self.predicates:
                if not eval_predicate(row, p['left'], p['op'], p['right']):
                    return False
            return True

        return self.table.delete_where(and_pred)


class Update:
    def __init__(self, table: TableStorage, set_clause: List[tuple], predicates: Optional[List[Dict[str, Any]]]):
        self.table = table
        self.set_clause = set_clause
        self.predicates = predicates or []

    def execute(self) -> int:
        if not self.predicates:
            # 更新所有行
            def update_func(row):
                for column, value in self.set_clause:
                    row[column] = value
                return True

            return self.table.update_where(update_func, lambda r: True)
        else:
            def update_func(row):
                for column, value in self.set_clause:
                    row[column] = value
                return True

            def and_pred(row: Dict[str, Any]) -> bool:
                for p in self.predicates:
                    if not eval_predicate(row, p['left'], p['op'], p['right']):
                        return False
                return True

            return self.table.update_where(update_func, and_pred)


class OrderBy:
    def __init__(self, child, order_specs: List[Dict[str, str]]):
        self.child = child
        self.order_specs = order_specs  # [{"column": "age", "direction": "DESC"}, ...]

    def execute(self) -> Iterable[Dict[str, Any]]:
        # 收集所有数据
        rows = list(self.child.execute())

        # 排序
        def sort_key(row):
            key_values = []
            for spec in self.order_specs:
                column = spec['column']
                direction = spec['direction']
                value = row.get(column)

                # 处理None值
                if value is None:
                    value = float('-inf') if direction == 'ASC' else float('inf')

                # 根据方向调整排序
                if direction == 'DESC':
                    value = -value if isinstance(value, (int, float)) else value

                key_values.append(value)
            return key_values

        rows.sort(key=sort_key)
        return rows


class Join:
    def __init__(self, left_child, right_child, join_type: str, on_condition: Dict[str, Any],
                 left_table_alias: str = None, right_table_alias: str = None):
        self.left_child = left_child
        self.right_child = right_child
        self.join_type = join_type  # INNER, LEFT, RIGHT, OUTER
        self.on_condition = on_condition
        self.left_table_alias = left_table_alias
        self.right_table_alias = right_table_alias

    def execute(self) -> Iterable[Dict[str, Any]]:
        # 收集右表数据
        right_rows = list(self.right_child.execute())

        # 执行连接
        for left_row in self.left_child.execute():
            matched = False
            for right_row in right_rows:
                if self._matches_condition(left_row, right_row):
                    matched = True
                    # 使用表别名避免键冲突
                    merged_row = self._merge_rows_with_aliases(left_row, right_row)
                    yield merged_row

            # 左连接：即使没有匹配也要输出左表行
            if self.join_type == 'LEFT' and not matched:
                # 为右表列添加别名前缀，值为None
                right_row_with_aliases = {}
                if right_rows:
                    for key in right_rows[0].keys():
                        if self.right_table_alias:
                            right_row_with_aliases[f"{self.right_table_alias}.{key}"] = None
                        else:
                            right_row_with_aliases[key] = None
                merged_row = self._merge_rows_with_aliases(left_row, right_row_with_aliases)
                yield merged_row

    def _merge_rows_with_aliases(self, left_row: Dict[str, Any], right_row: Dict[str, Any]) -> Dict[str, Any]:
        """合并行数据，使用表别名避免键冲突"""
        merged = {}

        # 添加左表数据
        for key, value in left_row.items():
            if self.left_table_alias:
                merged[f"{self.left_table_alias}.{key}"] = value
            else:
                merged[key] = value

        # 添加右表数据
        for key, value in right_row.items():
            if self.right_table_alias:
                merged[f"{self.right_table_alias}.{key}"] = value
            else:
                merged[key] = value

        return merged

    def _matches_condition(self, left_row: Dict[str, Any], right_row: Dict[str, Any]) -> bool:
        """检查连接条件是否满足"""
        left_col = self.on_condition['left']
        op = self.on_condition['op']
        right_col = self.on_condition['right']

        # 提取列名（处理table.column格式）
        left_column_name = _extract_column_name(left_col)
        right_column_name = _extract_column_name(right_col)

        # 从对应的行中获取值
        left_val = left_row.get(left_column_name)
        right_val = right_row.get(right_column_name)

        # 直接比较值
        if op == '=':
            return left_val == right_val
        elif op in ('<>', '!='):
            return left_val != right_val
        elif op == '<':
            return left_val < right_val
        elif op == '>':
            return left_val > right_val
        elif op == '<=':
            return left_val <= right_val
        elif op == '>=':
            return left_val >= right_val
        else:
            raise ValueError(f"不支持的比较运算符: {op}")


def _extract_column_name(column_ref: str) -> str:
    """从列引用中提取列名"""
    if '.' in column_ref:
        # 处理table.column格式，返回列名部分
        return column_ref.split('.', 1)[1]
    else:
        # 简单列名，直接返回
        return column_ref


class GroupBy:
    def __init__(self, child, columns: List[str], having: Optional[Dict[str, Any]] = None):
        self.child = child
        self.columns = columns
        self.having = having

    def execute(self) -> Iterable[Dict[str, Any]]:
        # 收集所有数据
        rows = list(self.child.execute())

        # 按分组列进行分组
        groups = {}
        for row in rows:
            # 构建分组键
            group_key = tuple(row.get(col) for col in self.columns)

            if group_key not in groups:
                groups[group_key] = []
            groups[group_key].append(row)

        # 为每个分组生成结果行
        for group_key, group_rows in groups.items():
            # 构建结果行
            result_row = {}

            # 添加分组列
            for i, col in enumerate(self.columns):
                result_row[col] = group_key[i]

            # 将分组内的所有行作为特殊字段传递，供Aggregate使用
            result_row['__group_rows__'] = group_rows

            yield result_row


class Aggregate:
    def __init__(self, child, functions: List[Dict[str, Any]]):
        self.child = child
        self.functions = functions
        self.group = True
        self.no_group_rows = {}

    def execute(self) -> Iterable[Dict[str, Any]]:
        for row in self.child.execute():
            # 检查是否有分组数据
            if '__group_rows__' in row:
                # 从分组数据中计算聚合函数
                group_rows = row['__group_rows__']
                result_row = {k: v for k, v in row.items() if k != '__group_rows__'}
            else:
                # 没有分组，使用所有数据
                all_rows = list(self.child.execute())
                self.no_group_rows = all_rows
                self.group = False
                break

            # 计算聚合函数
            for func in self.functions:
                func_name = func['func']
                column = func.get('column')
                alias = func.get('alias')

                if func_name == 'COUNT':
                    if column is None:  # COUNT(*)
                        value = len(group_rows)
                    else:
                        # COUNT(column) - 计算非NULL值的数量
                        value = sum(1 for r in group_rows if r.get(column) is not None)
                elif func_name == 'SUM':
                    if column is None:
                        value = 0
                    else:
                        values = [r.get(column) for r in group_rows if r.get(column) is not None]
                        value = sum(values) if values else 0
                elif func_name == 'AVG':
                    if column is None:
                        value = 0
                    else:
                        values = [r.get(column) for r in group_rows if r.get(column) is not None]
                        value = sum(values) / len(values) if values else 0
                elif func_name == 'MIN':
                    if column is None:
                        value = None
                    else:
                        values = [r.get(column) for r in group_rows if r.get(column) is not None]
                        value = min(values) if values else None
                elif func_name == 'MAX':
                    if column is None:
                        value = None
                    else:
                        values = [r.get(column) for r in group_rows if r.get(column) is not None]
                        value = max(values) if values else None
                else:
                    raise ValueError(f"不支持的聚合函数: {func_name}")

                # 使用别名或函数名作为列名
                column_name = alias or f"{func_name}({column or '*'})"
                result_row[column_name] = value

            yield result_row

        if not self.group:
            result_row = {}
            for func in self.functions:
                func_name = func['func']
                column = func.get('column')
                alias = func.get('alias')

                if func_name == 'COUNT':
                    if column is None:  # COUNT(*)
                        value = len(self.no_group_rows)
                    else:
                        # COUNT(column) - 计算非NULL值的数量
                        value = sum(1 for r in self.no_group_rows if r.get(column) is not None)
                elif func_name == 'SUM':
                    if column is None:
                        value = 0
                    else:
                        values = [r.get(column) for r in self.no_group_rows if r.get(column) is not None]
                        value = sum(values) if values else 0
                elif func_name == 'AVG':
                    if column is None:
                        value = 0
                    else:
                        values = [r.get(column) for r in self.no_group_rows if r.get(column) is not None]
                        value = sum(values) / len(values) if values else 0
                elif func_name == 'MIN':
                    if column is None:
                        value = None
                    else:
                        values = [r.get(column) for r in self.no_group_rows if r.get(column) is not None]
                        value = min(values) if values else None
                elif func_name == 'MAX':
                    if column is None:
                        value = None
                    else:
                        values = [r.get(column) for r in self.no_group_rows if r.get(column) is not None]
                        value = max(values) if values else None
                else:
                    raise ValueError(f"不支持的聚合函数: {func_name}")

                # 使用别名或函数名作为列名
                column_name = alias or f"{func_name}({column or '*'})"
                result_row[column_name] = value

            yield result_row
