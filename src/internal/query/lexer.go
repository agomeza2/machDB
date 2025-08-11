package query

import (
	"strings"
	"unicode"
)

type TokenType int

const (
	ILLEGAL TokenType = iota
	EOF
	WS

	IDENT    // nombres, comandos (list, select, insert...)
	STRING   // "cadena entre comillas"
	NUMBER   // números
	LBRACE   // {
	RBRACE   // }
	LBRACKET // [
	RBRACKET // ]
	COMMA    // ,
	COLON    // :
	EQ       // =
	ASTERISK // *
)

type Token struct {
	Type  TokenType
	Value string
}

type Lexer struct {
	input        string
	position     int
	readPosition int
	ch           rune
}

func NewLexer(input string) *Lexer {
	l := &Lexer{input: input}
	l.readChar()
	return l
}

func (l *Lexer) readChar() {
	if l.readPosition >= len(l.input) {
		l.ch = 0
	} else {
		l.ch = rune(l.input[l.readPosition])
	}
	l.position = l.readPosition
	l.readPosition++
}

func (l *Lexer) NextToken() Token {
	l.skipWhitespace()

	switch l.ch {
	case 0:
		return Token{Type: EOF}
	case '{':
		l.readChar()
		return Token{Type: LBRACE, Value: "{"}
	case '}':
		l.readChar()
		return Token{Type: RBRACE, Value: "}"}
	case '[':
		l.readChar()
		return Token{Type: LBRACKET, Value: "["}
	case ']':
		l.readChar()
		return Token{Type: RBRACKET, Value: "]"}
	case ',':
		l.readChar()
		return Token{Type: COMMA, Value: ","}
	case ':':
		l.readChar()
		return Token{Type: COLON, Value: ":"}
	case '=':
		l.readChar()
		return Token{Type: EQ, Value: "="}
	case '*':
		l.readChar()
		return Token{Type: ASTERISK, Value: "*"}
	case '"':
		return l.readString()
	default:
		if isLetter(l.ch) {
			return l.readIdentifier()
		} else if isDigit(l.ch) || (l.ch == '-' && isDigit(l.peekChar())) {
			return l.readNumber()
		}
		// ilegales u otros símbolos no manejados
		ch := l.ch
		l.readChar()
		return Token{Type: ILLEGAL, Value: string(ch)}
	}
}

func (l *Lexer) skipWhitespace() {
	for l.ch == ' ' || l.ch == '\t' || l.ch == '\r' || l.ch == '\n' {
		l.readChar()
	}
}

func (l *Lexer) readIdentifier() Token {
	pos := l.position
	for isLetter(l.ch) || isDigit(l.ch) || l.ch == '_' || l.ch == '.' || l.ch == '/' || l.ch == '-' {
		l.readChar()
	}
	val := l.input[pos:l.position]
	return Token{Type: IDENT, Value: strings.ToLower(val)} // lowercase para simplificar
}

func (l *Lexer) readString() Token {
	l.readChar() // skip "
	pos := l.position
	for l.ch != '"' && l.ch != 0 {
		l.readChar()
	}
	val := l.input[pos:l.position]
	l.readChar() // skip "
	return Token{Type: STRING, Value: val}
}

func (l *Lexer) readNumber() Token {
	pos := l.position
	if l.ch == '-' {
		l.readChar()
	}
	for isDigit(l.ch) || l.ch == '.' {
		l.readChar()
	}
	val := l.input[pos:l.position]
	return Token{Type: NUMBER, Value: val}
}

func (l *Lexer) peekChar() rune {
	if l.readPosition >= len(l.input) {
		return 0
	}
	return rune(l.input[l.readPosition])
}

func isLetter(ch rune) bool {
	return unicode.IsLetter(ch)
}

func isDigit(ch rune) bool {
	return '0' <= ch && ch <= '9'
}
