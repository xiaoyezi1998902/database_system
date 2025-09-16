from dataclasses import dataclass
from enum import Enum, auto
from typing import List, Optional


class TokenType(Enum):
	KEYWORD = auto()
	IDENTIFIER = auto()
	NUMBER = auto()
	STRING = auto()
	OPERATOR = auto()
	DELIMITER = auto()
	EOF = auto()


KEYWORDS = {
	"SELECT",
	"FROM",
	"WHERE",
	"CREATE",
	"TABLE",
	"INSERT",
	"INTO",
	"VALUES",
	"DELETE",
	"UPDATE",
	"SET",
	"JOIN",
	"LEFT",
	"RIGHT",
	"INNER",
	"OUTER",
	"ON",
	"ORDER",
	"BY",
	"GROUP",
	"HAVING",
	"ASC",
	"DESC",
	"INT",
	"TEXT",
	"VARCHAR",
	"AND",
	"OR",
	"NOT",
	"NULL",
	"IS",
	"IN",
	"BETWEEN",
	"LIKE",
	"EXISTS",
	"UNION",
	"DISTINCT",
	"AS",
	"COUNT",
	"SUM",
	"AVG",
	"MIN",
	"MAX",
}


@dataclass
class Token:
	type: TokenType
	lexeme: str
	line: int
	column: int

	def __repr__(self) -> str:
		return f"Token({self.type.name}, {self.lexeme!r}, line={self.line}, col={self.column})"


class LexError(Exception):
	def __init__(self, message: str, line: int = None, column: int = None, expected: str = None):
		super().__init__(message)
		self.line = line
		self.column = column
		self.expected = expected


class Lexer:
	def __init__(self, source: str):
		self.source = source
		self.length = len(source)
		self.index = 0
		self.line = 1
		self.col = 1

	def tokenize(self) -> List[Token]:
		tokens: List[Token] = []
		while True:
			tok = self._next_token()
			tokens.append(tok)
			if tok.type == TokenType.EOF:
				break
		return tokens

	def _peek(self) -> str:
		if self.index >= self.length:
			return "\0"
		return self.source[self.index]

	def _advance(self) -> str:
		c = self._peek()
		if c == "\0":
			return c
		self.index += 1
		if c == "\n":
			self.line += 1
			self.col = 1
		else:
			self.col += 1
		return c

	def _match(self, expected: str) -> bool:
		if self._peek() == expected:
			self._advance()
			return True
		return False

	def _skip_whitespace_and_comments(self) -> None:
		while True:
			c = self._peek()
			if c in " \t\r\n":
				self._advance()
				continue
			# line comment -- 单行注释开头
			if c == '-' and self._lookahead(1) == '-':
				while self._peek() not in ('\n', '\0'):
					self._advance()
				continue
			break

	def _lookahead(self, k: int) -> str:
		pos = self.index + k
		if pos >= self.length:
			return "\0"
		return self.source[pos]

	def _next_token(self) -> Token:
		self._skip_whitespace_and_comments()
		start_line = self.line
		start_col = self.col
		c = self._peek()
		if c == "\0":
			return Token(TokenType.EOF, "", start_line, start_col)

		# Identifiers / keywords
		if c.isalpha() or c == '_':
			lexeme = self._read_identifier()
			upper = lexeme.upper()
			if upper in KEYWORDS:
				return Token(TokenType.KEYWORD, upper, start_line, start_col)
			return Token(TokenType.IDENTIFIER, lexeme, start_line, start_col)

		# Numbers (integers only for simplicity)
		if c.isdigit():
			lexeme = self._read_number()
			return Token(TokenType.NUMBER, lexeme, start_line, start_col)

		# String literals '...'
		if c == "'":
			lexeme = self._read_string()
			return Token(TokenType.STRING, lexeme, start_line, start_col)

		# Operators and delimiters
		ch = self._advance()
		if ch in (',', ';', '(', ')', '*', '.'):
			return Token(TokenType.DELIMITER, ch, start_line, start_col)
		if ch in ('=', '+', '-', '/', '%'):
			return Token(TokenType.OPERATOR, ch, start_line, start_col)
		if ch in ('<', '>', '!'):
			# <=, >=, <>, !=
			if self._peek() == '=':
				op = ch + self._advance()
				return Token(TokenType.OPERATOR, op, start_line, start_col)
			if ch == '<' and self._peek() == '>':
				op = ch + self._advance()
				return Token(TokenType.OPERATOR, op, start_line, start_col)
			# single < or > or ! (error for ! alone)
			if ch == '!':
				raise LexError(f"非法字符 '!' 于行 {start_line} 列 {start_col}; 期望 '!='", start_line, start_col, "!=")
			return Token(TokenType.OPERATOR, ch, start_line, start_col)

		raise LexError(f"非法字符 {ch!r} 于行 {start_line} 列 {start_col}", start_line, start_col)

	def _read_identifier(self) -> str:
		chars: List[str] = []
		while True:
			c = self._peek()
			if c.isalnum() or c == '_':
				chars.append(self._advance())
			else:
				break
		return ''.join(chars)

	def _read_number(self) -> str:
		chars: List[str] = []
		while self._peek().isdigit():
			chars.append(self._advance())
		return ''.join(chars)

	def _read_string(self) -> str:
		# consume opening quote
		assert self._peek() == "'"
		self._advance()
		chars: List[str] = []
		start_line = self.line
		start_col = self.col
		while True:
			c = self._peek()
			if c == "\0":
				raise LexError(f"未闭合字符串 于行 {start_line} 列 {start_col}", start_line, start_col, "'")
			if c == "'":
				self._advance()
				break
			# support escaping '' as a single '
			if c == '\\':
				self._advance()
				next_c = self._peek()
				if next_c in ("'", '\\'):
					chars.append(self._advance())
					continue
				chars.append('\\')
				continue
			chars.append(self._advance())
		return ''.join(chars)
