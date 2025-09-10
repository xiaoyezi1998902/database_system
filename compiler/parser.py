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
	values: List[Union[int, str]]


@dataclass
class Comparison:
	left: str
	op: str
	right: Union[int, str]


@dataclass
class Select:
	columns: List[str]  # '*' is represented as ['*']
	table_name: str
	where: Optional[Comparison]


@dataclass
class Delete:
	table_name: str
	where: Optional[Comparison]


class ParseError(Exception):
	pass


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
			raise ParseError(f"语法错误 于行 {tok.line} 列 {tok.column}: 期望 {what}，实际 {tok.lexeme!r}")
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
		if self._check(TokenType.KEYWORD, 'DELETE'):
			return self._delete()
		tok = self._peek()
		raise ParseError(f"不支持的语句 于行 {tok.line} 列 {tok.column}: {tok.lexeme!r}")

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
				raise ParseError(f"期望类型名 于行 {type_tok.line} 列 {type_tok.column}")
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
		self._expect(TokenType.DELIMITER, '(', '(')
		values: List[Union[int, str]] = []
		while True:
			if self._check(TokenType.NUMBER):
				values.append(int(self._advance().lexeme))
			elif self._check(TokenType.STRING):
				values.append(self._advance().lexeme)
			else:
				tok = self._peek()
				raise ParseError(f"期望常量 于行 {tok.line} 列 {tok.column}")
			if self._match(TokenType.DELIMITER, ','):
				continue
			break
		self._expect(TokenType.DELIMITER, ')', ')')
		return Insert(table, columns, values)

	def _select(self) -> Select:
		self._expect(TokenType.KEYWORD, 'SELECT', 'SELECT')
		cols: List[str] = []
		if self._match(TokenType.DELIMITER, '*'):
			cols = ['*']
		else:
			while True:
				cols.append(self._expect(TokenType.IDENTIFIER, '列名').lexeme)
				if self._match(TokenType.DELIMITER, ','):
					continue
				break
		self._expect(TokenType.KEYWORD, 'FROM', 'FROM')
		table = self._expect(TokenType.IDENTIFIER, '表名').lexeme
		where: Optional[Comparison] = None
		if self._match(TokenType.KEYWORD, 'WHERE'):
			left = self._expect(TokenType.IDENTIFIER, '列名').lexeme
			op = self._expect(TokenType.OPERATOR, '比较运算符').lexeme
			right_tok = self._advance()
			if right_tok.type == TokenType.NUMBER:
				right = int(right_tok.lexeme)
			elif right_tok.type == TokenType.STRING:
				right = right_tok.lexeme
			else:
				raise ParseError(f"WHERE 右侧需常量 于行 {right_tok.line} 列 {right_tok.column}")
			where = Comparison(left, op, right)
		return Select(cols, table, where)

	def _delete(self) -> Delete:
		self._expect(TokenType.KEYWORD, 'DELETE', 'DELETE')
		self._expect(TokenType.KEYWORD, 'FROM', 'FROM')
		table = self._expect(TokenType.IDENTIFIER, '表名').lexeme
		where: Optional[Comparison] = None
		if self._match(TokenType.KEYWORD, 'WHERE'):
			left = self._expect(TokenType.IDENTIFIER, '列名').lexeme
			op = self._expect(TokenType.OPERATOR, '比较运算符').lexeme
			right_tok = self._advance()
			if right_tok.type == TokenType.NUMBER:
				right = int(right_tok.lexeme)
			elif right_tok.type == TokenType.STRING:
				right = right_tok.lexeme
			else:
				raise ParseError(f"WHERE 右侧需常量 于行 {right_tok.line} 列 {right_tok.column}")
			where = Comparison(left, op, right)
		return Delete(table, where)
