from dataclasses import dataclass
from typing import List, Optional, Union

from .lexer import Token, TokenType


# === AST Node Definitions ===
@dataclass
class ColumnDef:
	name: str
	type_name: str


@dataclass
class CreateTable:
	table_name: str
	columns: List[ColumnDef]


@dataclass
class Insert:
	table_name: str
	columns: Optional[List[str]]  # None means all columns
	values: List[List[Union[int, str]]]  # 支持多行，每行是一个值列表


@dataclass
class Comparison:
	left: str
	op: str
	right: Union[int, str]  # 可以是常量或列引用

@dataclass
class LogicalExpression:
	left: 'Condition'
	op: str  # AND, OR
	right: 'Condition'

Condition = Union[Comparison, LogicalExpression]


@dataclass
class Join:
	table_name: str
	table_alias: Optional[str]  # 表别名
	join_type: str  # INNER, LEFT, RIGHT, OUTER
	on_condition: Comparison


@dataclass
class OrderBy:
	column: str
	direction: str  # ASC, DESC


@dataclass
class GroupBy:
	columns: List[str]
	having: Optional[Condition]


@dataclass
class AggregateFunction:
	func_name: str  # COUNT, SUM, AVG, MIN, MAX
	column: Optional[str]  # None for COUNT(*)
	alias: Optional[str]

@dataclass
class ColumnWithAlias:
	column: str
	alias: Optional[str]


@dataclass
class Select:
	columns: List[Union[str, AggregateFunction, ColumnWithAlias]]  # '*' is represented as ['*']
	table_name: str
	table_alias: Optional[str]  # 表别名
	joins: List[Join]
	where: Optional[Condition]
	group_by: Optional[GroupBy]
	order_by: List[OrderBy]


@dataclass
class Update:
	table_name: str
	set_clause: List[tuple]  # [(column, value), ...]
	where: Optional[Condition]


@dataclass
class Delete:
	table_name: str
	where: Optional[Condition]


class ParseError(Exception):
	def __init__(self, message: str, line: int = None, column: int = None, expected: str = None):
		super().__init__(message)
		self.line = line
		self.column = column
		self.expected = expected


class Parser:
	def __init__(self, tokens: List[Token]):
		self.tokens = tokens
		self.index = 0

	def parse(self):
		# Return a list of statements or a single statement; for simplicity, single
		stmt = self._statement()
		self._consume_optional_semicolon()
		self._expect(TokenType.EOF, "EOF")
		return stmt

	def _peek(self) -> Token:
		return self.tokens[self.index]

	def _advance(self) -> Token:
		tok = self._peek()
		if tok.type != TokenType.EOF:
			self.index += 1
		return tok

	def _check(self, ttype: TokenType, lexeme: Optional[str] = None) -> bool:
		tok = self._peek()
		if tok.type != ttype:
			return False
		if lexeme is not None and tok.lexeme.upper() != lexeme.upper():
			return False
		return True

	def _match(self, ttype: TokenType, lexeme: Optional[str] = None) -> bool:
		if self._check(ttype, lexeme):
			self._advance()
			return True
		return False

	def _expect(self, ttype: TokenType, what: str, lexeme: Optional[str] = None) -> Token:
		if not self._check(ttype, lexeme):
			tok = self._peek()
			expected = lexeme if lexeme else ttype.name
			raise ParseError(f"语法错误 于行 {tok.line} 列 {tok.column}: 期望 {what}，实际 {tok.lexeme!r}", tok.line, tok.column, expected)
		return self._advance()

	def _consume_optional_semicolon(self) -> None:
		if self._match(TokenType.DELIMITER, ';'):
			pass

	def _statement(self):
		if self._check(TokenType.KEYWORD, 'CREATE'):
			return self._create_table()
		if self._check(TokenType.KEYWORD, 'INSERT'):
			return self._insert()
		if self._check(TokenType.KEYWORD, 'SELECT'):
			return self._select()
		if self._check(TokenType.KEYWORD, 'UPDATE'):
			return self._update()
		if self._check(TokenType.KEYWORD, 'DELETE'):
			return self._delete()
		tok = self._peek()
		raise ParseError(f"不支持的语句 于行 {tok.line} 列 {tok.column}: {tok.lexeme!r}", tok.line, tok.column, "CREATE/INSERT/SELECT/UPDATE/DELETE")

	def _create_table(self) -> CreateTable:
		self._expect(TokenType.KEYWORD, 'CREATE', 'CREATE')
		self._expect(TokenType.KEYWORD, 'TABLE', 'TABLE')
		table = self._expect(TokenType.IDENTIFIER, '表名').lexeme
		self._expect(TokenType.DELIMITER, '(', '(')
		columns: List[ColumnDef] = []
		while True:
			col_name = self._expect(TokenType.IDENTIFIER, '列名').lexeme
			type_tok = self._advance()
			if type_tok.type not in (TokenType.KEYWORD, TokenType.IDENTIFIER):
				raise ParseError(f"期望类型名 于行 {type_tok.line} 列 {type_tok.column}", type_tok.line, type_tok.column, "INT/TEXT/VARCHAR")
			columns.append(ColumnDef(col_name, type_tok.lexeme.upper()))
			if self._match(TokenType.DELIMITER, ','):
				continue
			break
		self._expect(TokenType.DELIMITER, ')', ')')
		return CreateTable(table, columns)

	def _insert(self) -> Insert:
		self._expect(TokenType.KEYWORD, 'INSERT', 'INSERT')
		self._expect(TokenType.KEYWORD, 'INTO', 'INTO')
		table = self._expect(TokenType.IDENTIFIER, '表名').lexeme
		columns: Optional[List[str]] = None
		if self._match(TokenType.DELIMITER, '('):
			columns = []
			while True:
				columns.append(self._expect(TokenType.IDENTIFIER, '列名').lexeme)
				if self._match(TokenType.DELIMITER, ','):
					continue
				break
			self._expect(TokenType.DELIMITER, ')', ')')
		self._expect(TokenType.KEYWORD, 'VALUES', 'VALUES')
		
		# 解析多行VALUES
		values: List[List[Union[int, str]]] = []
		while True:
			self._expect(TokenType.DELIMITER, '(', '(')
			row_values: List[Union[int, str]] = []
			while True:
				if self._check(TokenType.NUMBER):
					row_values.append(int(self._advance().lexeme))
				elif self._check(TokenType.STRING):
					row_values.append(self._advance().lexeme)
				else:
					tok = self._peek()
					raise ParseError(f"期望常量 于行 {tok.line} 列 {tok.column}", tok.line, tok.column, "数字或字符串")
				if self._match(TokenType.DELIMITER, ','):
					continue
				break
			self._expect(TokenType.DELIMITER, ')', ')')
			values.append(row_values)
			
			# 检查是否还有更多行
			if self._match(TokenType.DELIMITER, ','):
				continue
			break
		
		return Insert(table, columns, values)

	def _select(self) -> Select:
		self._expect(TokenType.KEYWORD, 'SELECT', 'SELECT')
		cols: List[Union[str, AggregateFunction, ColumnWithAlias]] = []
		if self._match(TokenType.DELIMITER, '*'):
			cols = ['*']
		else:
			while True:
				col = self._parse_column_or_aggregate()
				cols.append(col)
				if self._match(TokenType.DELIMITER, ','):
					continue
				break
		self._expect(TokenType.KEYWORD, 'FROM', 'FROM')
		table = self._expect(TokenType.IDENTIFIER, '表名').lexeme
		
		# 解析表别名
		table_alias = None
		if self._peek().type == TokenType.IDENTIFIER:
			table_alias = self._advance().lexeme
		
		# 解析JOIN子句
		joins: List[Join] = []
		while self._check(TokenType.KEYWORD, 'JOIN') or self._check(TokenType.KEYWORD, 'LEFT') or self._check(TokenType.KEYWORD, 'RIGHT') or self._check(TokenType.KEYWORD, 'INNER') or self._check(TokenType.KEYWORD, 'OUTER'):
			join = self._parse_join()
			joins.append(join)
		
		# 解析WHERE子句
		where: Optional[Condition] = None
		if self._match(TokenType.KEYWORD, 'WHERE'):
			where = self._parse_condition()
		
		# 解析GROUP BY子句
		group_by: Optional[GroupBy] = None
		if self._match(TokenType.KEYWORD, 'GROUP'):
			self._expect(TokenType.KEYWORD, 'BY', 'BY')
			group_cols = []
			while True:
				group_cols.append(self._expect(TokenType.IDENTIFIER, '列名').lexeme)
				if self._match(TokenType.DELIMITER, ','):
					continue
				break
			having: Optional[Comparison] = None
			if self._match(TokenType.KEYWORD, 'HAVING'):
				having = self._parse_condition()
			group_by = GroupBy(group_cols, having)
		
		# 解析ORDER BY子句
		order_by: List[OrderBy] = []
		if self._match(TokenType.KEYWORD, 'ORDER'):
			self._expect(TokenType.KEYWORD, 'BY', 'BY')
			while True:
				col = self._expect(TokenType.IDENTIFIER, '列名').lexeme
				direction = 'ASC'
				if self._match(TokenType.KEYWORD, 'ASC'):
					direction = 'ASC'
				elif self._match(TokenType.KEYWORD, 'DESC'):
					direction = 'DESC'
				order_by.append(OrderBy(col, direction))
				if self._match(TokenType.DELIMITER, ','):
					continue
				break
		
		return Select(cols, table, table_alias, joins, where, group_by, order_by)

	def _delete(self) -> Delete:
		self._expect(TokenType.KEYWORD, 'DELETE', 'DELETE')
		self._expect(TokenType.KEYWORD, 'FROM', 'FROM')
		table = self._expect(TokenType.IDENTIFIER, '表名').lexeme
		where: Optional[Condition] = None
		if self._match(TokenType.KEYWORD, 'WHERE'):
			where = self._parse_condition()
		return Delete(table, where)

	def _update(self) -> Update:
		self._expect(TokenType.KEYWORD, 'UPDATE', 'UPDATE')
		table = self._expect(TokenType.IDENTIFIER, '表名').lexeme
		self._expect(TokenType.KEYWORD, 'SET', 'SET')
		
		# 解析SET子句
		set_clause = []
		while True:
			column = self._expect(TokenType.IDENTIFIER, '列名').lexeme
			self._expect(TokenType.OPERATOR, '=', '=')
			
			# 解析值
			value_tok = self._advance()
			if value_tok.type == TokenType.NUMBER:
				value = int(value_tok.lexeme)
			elif value_tok.type == TokenType.STRING:
				value = value_tok.lexeme
			else:
				raise ParseError(f"期望常量 于行 {value_tok.line} 列 {value_tok.column}", value_tok.line, value_tok.column, "数字或字符串")
			
			set_clause.append((column, value))
			
			if self._match(TokenType.DELIMITER, ','):
				continue
			break
		
		# 解析WHERE子句
		where: Optional[Condition] = None
		if self._match(TokenType.KEYWORD, 'WHERE'):
			where = self._parse_condition()
		
		return Update(table, set_clause, where)

	def _parse_column_or_aggregate(self) -> Union[str, AggregateFunction, ColumnWithAlias]:
		# 检查是否是聚合函数
		if self._check(TokenType.KEYWORD, 'COUNT') or self._check(TokenType.KEYWORD, 'SUM') or self._check(TokenType.KEYWORD, 'AVG') or self._check(TokenType.KEYWORD, 'MIN') or self._check(TokenType.KEYWORD, 'MAX'):
			func_name = self._advance().lexeme.upper()
			self._expect(TokenType.DELIMITER, '(', '(')
			
			column = None
			if not self._match(TokenType.DELIMITER, '*'):
				column = self._parse_column_reference()
			self._expect(TokenType.DELIMITER, ')', ')')
			
			alias = None
			if self._match(TokenType.KEYWORD, 'AS'):
				alias = self._expect(TokenType.IDENTIFIER, '别名').lexeme
			
			return AggregateFunction(func_name, column, alias)
		else:
			# 解析普通列
			column = self._parse_column_reference()
			
			# 检查是否有别名
			alias = None
			if self._match(TokenType.KEYWORD, 'AS'):
				alias = self._expect(TokenType.IDENTIFIER, '别名').lexeme
			
			# 如果有别名，返回ColumnWithAlias，否则返回字符串
			if alias:
				return ColumnWithAlias(column, alias)
			else:
				return column
	
	def _parse_column_reference(self) -> str:
		"""解析列引用，支持table.column格式"""
		table_or_column = self._expect(TokenType.IDENTIFIER, '表名或列名').lexeme
		
		if self._match(TokenType.DELIMITER, '.'):
			column = self._expect(TokenType.IDENTIFIER, '列名').lexeme
			return f"{table_or_column}.{column}"
		else:
			return table_or_column

	def _parse_join(self) -> Join:
		join_type = 'INNER'
		if self._match(TokenType.KEYWORD, 'LEFT'):
			join_type = 'LEFT'
		elif self._match(TokenType.KEYWORD, 'RIGHT'):
			join_type = 'RIGHT'
		elif self._match(TokenType.KEYWORD, 'INNER'):
			join_type = 'INNER'
		elif self._match(TokenType.KEYWORD, 'OUTER'):
			join_type = 'OUTER'
		
		if not self._match(TokenType.KEYWORD, 'JOIN'):
			raise ParseError("期望JOIN关键字", self._peek().line, self._peek().column, "JOIN")
		
		table = self._expect(TokenType.IDENTIFIER, '表名').lexeme
		
		# 解析表别名
		table_alias = None
		if self._peek().type == TokenType.IDENTIFIER:
			table_alias = self._advance().lexeme
		
		self._expect(TokenType.KEYWORD, 'ON', 'ON')
		on_condition = self._parse_condition()
		
		return Join(table, table_alias, join_type, on_condition)

	def _parse_condition(self) -> Condition:
		"""解析条件表达式，支持AND/OR逻辑运算符"""
		return self._parse_logical_or()
	
	def _parse_logical_or(self) -> Condition:
		"""解析OR逻辑表达式"""
		left = self._parse_logical_and()
		
		while self._match(TokenType.KEYWORD, 'OR'):
			right = self._parse_logical_and()
			left = LogicalExpression(left, 'OR', right)
		
		return left
	
	def _parse_logical_and(self) -> Condition:
		"""解析AND逻辑表达式"""
		left = self._parse_comparison()
		
		while self._match(TokenType.KEYWORD, 'AND'):
			right = self._parse_comparison()
			left = LogicalExpression(left, 'AND', right)
		
		return left
	
	def _parse_comparison(self) -> Comparison:
		"""解析比较表达式"""
		left = self._parse_column_reference()
		op = self._expect(TokenType.OPERATOR, '比较运算符').lexeme
		
		# 检查右操作数是否是列引用
		if self._peek().type == TokenType.IDENTIFIER:
			# 右操作数是列引用
			right = self._parse_column_reference()
		else:
			# 右操作数是常量
			right_tok = self._advance()
			if right_tok.type == TokenType.NUMBER:
				right = int(right_tok.lexeme)
			elif right_tok.type == TokenType.STRING:
				right = right_tok.lexeme
			else:
				raise ParseError(f"期望常量或列引用 于行 {right_tok.line} 列 {right_tok.column}", right_tok.line, right_tok.column, "数字、字符串或列名")
		
		return Comparison(left, op, right)
