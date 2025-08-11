package query

import (
	"fmt"
)

type Interpreter struct {
	DBPath      string
	CurrentDB   string
	CurrentColl string
	// pon aquí tus estructuras para manejar db, index, etc.
}

func NewInterpreter(dbpath string) *Interpreter {
	return &Interpreter{
		DBPath: dbpath,
	}
}

func (i *Interpreter) Execute(cmd *Command) error {
	switch cmd.Name {
	case "list":
		return i.cmdList(cmd.Args)
	case "select":
		return i.cmdSelect(cmd.Args)
	case "create":
		return i.cmdCreate(cmd.Args)
	case "insert":
		return i.cmdInsert(cmd.Properties, cmd.Filters, cmd.Args)
	case "modify":
		return i.cmdModify(cmd.Properties, cmd.Filters, cmd.Args)
	case "delete":
		return i.cmdDelete(cmd.Args)
	case "find":
		return i.cmdFind(cmd.RawQuery, cmd.Args)
	case "import":
		return i.cmdImport(cmd.Args)
	case "export":
		return i.cmdExport(cmd.Args)
	default:
		return fmt.Errorf("comando no implementado: %s", cmd.Name)
	}
}

func (i *Interpreter) cmdList(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("list requiere argumento")
	}
	switch args[0] {
	case "db":
		// TODO: listar carpetas en i.DBPath
		fmt.Println("Listando bases de datos...")
	case "collections":
		if i.CurrentDB == "" {
			return fmt.Errorf("no hay base de datos seleccionada")
		}
		// TODO: listar colecciones en la base seleccionada
		fmt.Println("Listando colecciones en", i.CurrentDB)
	case "documents":
		if i.CurrentColl == "" {
			return fmt.Errorf("no hay colección seleccionada")
		}
		// TODO: listar documentos en la colección
		fmt.Println("Listando documentos en", i.CurrentColl)
	default:
		return fmt.Errorf("argumento desconocido para list: %s", args[0])
	}
	return nil
}

func (i *Interpreter) cmdSelect(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("select requiere argumento")
	}
	switch args[0] {
	case "db":
		if len(args) < 2 {
			return fmt.Errorf("select db requiere nombre de base de datos")
		}
		// TODO: verificar si existe la carpeta db, si sí set CurrentDB
		i.CurrentDB = args[1]
		i.CurrentColl = ""
		fmt.Println("Base de datos seleccionada:", i.CurrentDB)
	case "collection":
		if i.CurrentDB == "" {
			return fmt.Errorf("no hay base de datos seleccionada")
		}
		if len(args) < 2 {
			return fmt.Errorf("select collection requiere nombre de colección")
		}
		// TODO: verificar existencia, set CurrentColl
		i.CurrentColl = args[1]
		fmt.Println("Colección seleccionada:", i.CurrentColl)
	case "*":
		// select * from document
		if len(args) < 3 || args[1] != "from" {
			return fmt.Errorf("select * from document: sintaxis incorrecta")
		}
		if i.CurrentDB == "" || i.CurrentColl == "" {
			return fmt.Errorf("no hay base ni colección seleccionada")
		}
		docName := args[2]
		// TODO: mostrar todo el documento
		fmt.Printf("Mostrando documento %s\n", docName)
	default:
		return fmt.Errorf("argumento no soportado para select: %s", args[0])
	}
	return nil
}

// Implementa el resto con la lógica que necesites
func (i *Interpreter) cmdCreate(args []string) error {
	fmt.Println("Comando create no implementado aún", args)
	return nil
}

func (i *Interpreter) cmdInsert(props, filters []map[string]interface{}, args []string) error {
	fmt.Println("Comando insert no implementado aún")
	return nil
}

func (i *Interpreter) cmdModify(props, filters []map[string]interface{}, args []string) error {
	fmt.Println("Comando modify no implementado aún")
	return nil
}

func (i *Interpreter) cmdDelete(args []string) error {
	fmt.Println("Comando delete no implementado aún")
	return nil
}

func (i *Interpreter) cmdFind(rawQueries []string, args []string) error {
	fmt.Println("Comando find no implementado aún")
	return nil
}

func (i *Interpreter) cmdImport(args []string) error {
	fmt.Println("Comando import no implementado aún")
	return nil
}

func (i *Interpreter) cmdExport(args []string) error {
	fmt.Println("Comando export no implementado aún")
	return nil
}
