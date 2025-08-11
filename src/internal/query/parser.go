package query

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Comando representa un comando parseado con sus parámetros
type Command struct {
	Name       string
	Args       []string
	Properties []map[string]interface{} // para insert/modify JSON-like
	Filters    []map[string]interface{} // para where / for
	RawQuery   []string                 // para find con varios filtros
}

// Parser estructura principal
type Parser struct {
	lexer     *Lexer
	curToken  Token
	peekToken Token
}

func NewParser(l *Lexer) *Parser {
	p := &Parser{lexer: l}
	p.nextToken()
	p.nextToken()
	return p
}

func (p *Parser) nextToken() {
	p.curToken = p.peekToken
	p.peekToken = p.lexer.NextToken()
}

func (p *Parser) expect(t TokenType) error {
	if p.curToken.Type != t {
		return fmt.Errorf("expected %v, got %v", t, p.curToken.Type)
	}
	return nil
}

func (p *Parser) ParseCommand() (*Command, error) {
	if p.curToken.Type != IDENT {
		return nil, fmt.Errorf("expected command name, got %s", p.curToken.Value)
	}
	cmd := &Command{Name: p.curToken.Value}
	p.nextToken()

	switch cmd.Name {
	case "list":
		if err := p.parseList(cmd); err != nil {
			return nil, err
		}
	case "select":
		if err := p.parseSelect(cmd); err != nil {
			return nil, err
		}
	case "create":
		if err := p.parseCreate(cmd); err != nil {
			return nil, err
		}
	case "insert":
		if err := p.parseInsert(cmd); err != nil {
			return nil, err
		}
	case "modify":
		if err := p.parseModify(cmd); err != nil {
			return nil, err
		}
	case "delete":
		if err := p.parseDelete(cmd); err != nil {
			return nil, err
		}
	case "find":
		if err := p.parseFind(cmd); err != nil {
			return nil, err
		}
	case "import":
		if err := p.parseImport(cmd); err != nil {
			return nil, err
		}
	case "export":
		if err := p.parseExport(cmd); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unknown command %s", cmd.Name)
	}
	return cmd, nil
}

// Parse funciones individuales por comando

func (p *Parser) parseList(cmd *Command) error {
	if p.curToken.Type != IDENT {
		return errors.New("expected argument after list")
	}
	cmd.Args = append(cmd.Args, p.curToken.Value)
	p.nextToken()
	return nil
}

func (p *Parser) parseSelect(cmd *Command) error {
	// select db nombre
	// select collection nombre
	// select * from document
	if p.curToken.Type != IDENT {
		return errors.New("expected db/collection/* after select")
	}
	cmd.Args = append(cmd.Args, p.curToken.Value)
	p.nextToken()

	// Si es *, entonces espera from document
	if cmd.Args[0] == "*" {
		if p.curToken.Type != IDENT || p.curToken.Value != "from" {
			return errors.New("expected 'from' after '*'")
		}
		cmd.Args = append(cmd.Args, p.curToken.Value) // from
		p.nextToken()

		if p.curToken.Type != IDENT {
			return errors.New("expected document name after from")
		}
		cmd.Args = append(cmd.Args, p.curToken.Value)
		p.nextToken()
		return nil
	}

	// si no, es select db nombre o select collection nombre
	if p.curToken.Type == IDENT {
		cmd.Args = append(cmd.Args, p.curToken.Value)
		p.nextToken()
	}
	return nil
}

func (p *Parser) parseCreate(cmd *Command) error {
	// create document name_document
	// create collection name_collection
	if p.curToken.Type != IDENT {
		return errors.New("expected 'document' or 'collection' after create")
	}
	cmd.Args = append(cmd.Args, p.curToken.Value)
	p.nextToken()

	if p.curToken.Type != IDENT {
		return errors.New("expected name after create document/collection")
	}
	cmd.Args = append(cmd.Args, p.curToken.Value)
	p.nextToken()
	return nil
}

func (p *Parser) parseInsert(cmd *Command) error {
	// insert [{name:Luis, age:18}, {...}] in document
	// insert {company_name:IBM} for {id:0} in document

	// Parse properties JSON-like (either [] or {})
	props, err := p.parseProps()
	if err != nil {
		return err
	}
	cmd.Properties = props

	// opcional: parse "for" filter
	if p.curToken.Type == IDENT && p.curToken.Value == "for" {
		p.nextToken()
		filters, err := p.parseProps()
		if err != nil {
			return err
		}
		cmd.Filters = filters
	}

	// espera "in"
	if p.curToken.Type != IDENT || p.curToken.Value != "in" {
		return errors.New("expected 'in' after insert properties")
	}
	p.nextToken()

	// espera document
	if p.curToken.Type != IDENT || p.curToken.Value != "document" {
		return errors.New("expected 'document' after 'in'")
	}
	cmd.Args = append(cmd.Args, "document")
	p.nextToken()

	// espera nombre documento
	if p.curToken.Type != IDENT {
		return errors.New("expected document name after 'document'")
	}
	cmd.Args = append(cmd.Args, p.curToken.Value)
	p.nextToken()
	return nil
}

func (p *Parser) parseModify(cmd *Command) error {
	// modify {age:30} for {id:1} in document

	// propiedades a modificar
	props, err := p.parseProps()
	if err != nil {
		return err
	}
	cmd.Properties = props

	// filtro after "for"
	if p.curToken.Type != IDENT || p.curToken.Value != "for" {
		return errors.New("expected 'for' after modify properties")
	}
	p.nextToken()

	filters, err := p.parseProps()
	if err != nil {
		return err
	}
	cmd.Filters = filters

	// espera in document name
	if p.curToken.Type != IDENT || p.curToken.Value != "in" {
		return errors.New("expected 'in' after for clause")
	}
	p.nextToken()

	if p.curToken.Type != IDENT || p.curToken.Value != "document" {
		return errors.New("expected 'document' after 'in'")
	}
	cmd.Args = append(cmd.Args, "document")
	p.nextToken()

	if p.curToken.Type != IDENT {
		return errors.New("expected document name after 'document'")
	}
	cmd.Args = append(cmd.Args, p.curToken.Value)
	p.nextToken()
	return nil
}

func (p *Parser) parseDelete(cmd *Command) error {
	// delete db nameDB
	// delete collection nameCollection
	// delete document nameDocument
	if p.curToken.Type != IDENT {
		return errors.New("expected db/collection/document after delete")
	}
	cmd.Args = append(cmd.Args, p.curToken.Value)
	p.nextToken()

	if p.curToken.Type != IDENT {
		return errors.New("expected name after delete db/collection/document")
	}
	cmd.Args = append(cmd.Args, p.curToken.Value)
	p.nextToken()
	return nil
}

func (p *Parser) parseFind(cmd *Command) error {
	// find "name:luis"
	// find "name:Luis" in NameCollection
	// find "name:Luis" "city:New york" ventas users_address

	for p.curToken.Type == STRING {
		cmd.RawQuery = append(cmd.RawQuery, p.curToken.Value)
		p.nextToken()
	}

	// opcional: "in" collectionName
	if p.curToken.Type == IDENT && p.curToken.Value == "in" {
		p.nextToken()
		if p.curToken.Type != IDENT {
			return errors.New("expected collection name after 'in'")
		}
		cmd.Args = append(cmd.Args, p.curToken.Value)
		p.nextToken()
	}

	// opcional: db and collections (para join)
	for p.curToken.Type == IDENT {
		cmd.Args = append(cmd.Args, p.curToken.Value)
		p.nextToken()
	}
	return nil
}

func (p *Parser) parseImport(cmd *Command) error {
	// import filename_path
	if p.curToken.Type != IDENT && p.curToken.Type != STRING {
		return errors.New("expected filename after import")
	}
	cmd.Args = append(cmd.Args, p.curToken.Value)
	p.nextToken()
	return nil
}

func (p *Parser) parseExport(cmd *Command) error {
	// export db/collection/document filename_path
	if p.curToken.Type != IDENT {
		return errors.New("expected db/collection/document after export")
	}
	cmd.Args = append(cmd.Args, p.curToken.Value)
	p.nextToken()

	if p.curToken.Type != IDENT && p.curToken.Type != STRING {
		return errors.New("expected filename after export target")
	}
	cmd.Args = append(cmd.Args, p.curToken.Value)
	p.nextToken()
	return nil
}

// parseProps parsea estructuras tipo JSON simples {key:value,...} o listas [{...},{...}]
func (p *Parser) parseProps() ([]map[string]interface{}, error) {
	if p.curToken.Type == LBRACKET {
		// lista de objetos
		p.nextToken()
		var objs []map[string]interface{}
		for {
			if p.curToken.Type == RBRACKET {
				p.nextToken()
				break
			}
			if p.curToken.Type != LBRACE {
				return nil, errors.New("expected '{' in array of objects")
			}
			obj, err := p.parseSingleProp()
			if err != nil {
				return nil, err
			}
			objs = append(objs, obj)
			if p.curToken.Type == COMMA {
				p.nextToken()
				continue
			} else if p.curToken.Type == RBRACKET {
				p.nextToken()
				break
			} else {
				return nil, errors.New("expected ',' or ']' in array")
			}
		}
		return objs, nil
	} else if p.curToken.Type == LBRACE {
		// objeto único
		obj, err := p.parseSingleProp()
		if err != nil {
			return nil, err
		}
		return []map[string]interface{}{obj}, nil
	}
	return nil, errors.New("expected '{' or '[' to start properties")
}

// parseSingleProp parsea un objeto simple {key:value,...}
func (p *Parser) parseSingleProp() (map[string]interface{}, error) {
	m := make(map[string]interface{})
	if p.curToken.Type != LBRACE {
		return nil, errors.New("expected '{' at start of object")
	}
	p.nextToken()
	for p.curToken.Type != RBRACE && p.curToken.Type != EOF {
		if p.curToken.Type != IDENT && p.curToken.Type != STRING {
			return nil, errors.New("expected key in object")
		}
		key := p.curToken.Value
		p.nextToken()

		if p.curToken.Type != COLON {
			return nil, errors.New("expected ':' after key in object")
		}
		p.nextToken()

		val, err := p.parseValue()
		if err != nil {
			return nil, err
		}

		m[key] = val

		if p.curToken.Type == COMMA {
			p.nextToken()
		} else if p.curToken.Type == RBRACE {
			break
		} else {
			return nil, errors.New("expected ',' or '}' in object")
		}
	}
	if p.curToken.Type != RBRACE {
		return nil, errors.New("expected '}' at end of object")
	}
	p.nextToken()
	return m, nil
}

func (p *Parser) parseValue() (interface{}, error) {
	switch p.curToken.Type {
	case STRING:
		val := p.curToken.Value
		p.nextToken()
		return val, nil
	case NUMBER:
		val := p.curToken.Value
		p.nextToken()
		if strings.Contains(val, ".") {
			f, err := strconv.ParseFloat(val, 64)
			if err != nil {
				return nil, err
			}
			return f, nil
		} else {
			i, err := strconv.Atoi(val)
			if err != nil {
				return nil, err
			}
			return i, nil
		}
	case IDENT:
		// booleanos true/false o null
		switch p.curToken.Value {
		case "true":
			p.nextToken()
			return true, nil
		case "false":
			p.nextToken()
			return false, nil
		case "null":
			p.nextToken()
			return nil, nil
		default:
			// lo tratamos como string
			val := p.curToken.Value
			p.nextToken()
			return val, nil
		}
	default:
		return nil, fmt.Errorf("unexpected value token %v", p.curToken)
	}
}
