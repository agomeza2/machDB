package query

import (
	"fmt"
	"machDB/src/internal/index"
)

type Interpreter struct {
	DBPath      string
	CurrentDB   string
	CurrentColl string
	idx         *index.Index
}

func NewInterpreter(dbpath string) (*Interpreter, error) {
	interp := &Interpreter{
		DBPath: dbpath,
		idx:    index.NewIndex(),
	}

	err := interp.idx.LoadFromDisk()
	if err != nil {
		return nil, err
	}

	return interp, nil
}
func (i *Interpreter) Save() {
	i.idx.FlushToDisk()
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
		databases := i.idx.ListDatabases()
		fmt.Println(databases)
	case "collections":
		if i.CurrentDB == "" {
			return fmt.Errorf("no hay base de datos seleccionada")
		}
		collections, err := i.idx.ListCollections(i.CurrentDB)
		if err != nil {
			return err
		}
		fmt.Println(collections)
	case "documents":
		if i.CurrentColl == "" {
			return fmt.Errorf("no hay colección seleccionada")
		}
		docs, err := i.idx.ListDocuments(i.CurrentDB, i.CurrentColl)
		if err != nil {
			return err
		}
		fmt.Println(docs)
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
		dbName := args[1]
		dbs := i.idx.ListDatabases()
		found := false
		for _, d := range dbs {
			if d == dbName {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("base de datos %s no encontrada", dbName)
		}
		i.CurrentDB = dbName
		i.CurrentColl = ""
		fmt.Println("Base de datos seleccionada:", i.CurrentDB)
		return nil

	case "collection":
		if i.CurrentDB == "" {
			return fmt.Errorf("no hay base de datos seleccionada")
		}
		if len(args) < 2 {
			return fmt.Errorf("select collection requiere nombre de colección")
		}
		colName := args[1]
		cols, err := i.idx.ListCollections(i.CurrentDB)
		if err != nil {
			return err
		}
		found := false
		for _, c := range cols {
			if c == colName {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("colección %s no encontrada en base de datos %s", colName, i.CurrentDB)
		}
		i.CurrentColl = colName
		fmt.Println("Colección seleccionada:", i.CurrentColl)
		return nil

	default:
		return fmt.Errorf("argumento desconocido para select: %s", args[0])
	}
}

// Implementa el resto con la lógica que necesites
func (i *Interpreter) cmdCreate(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("create needs an argument")
	}
	switch args[0] {
	case "db":
		i.idx.CreateDatabase(args[1])
	case "collections":
		i.idx.CreateCollection(i.CurrentDB, args[1])
	case "documents":
		i.idx.CreateDocument(i.CurrentDB, i.CurrentColl, args[1])
	default:
		return fmt.Errorf("unkown argument for create: %s", args[0])
	}
	return nil
}

func (i *Interpreter) cmdInsert(props, filters []map[string]interface{}, args []string) error {
	fmt.Println("Comando insert no implementado aún")
	return nil
}

func (i *Interpreter) cmdModify(props, filters []map[string]interface{}, args []string) error {
	return nil
}

func (i *Interpreter) cmdDelete(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("delete needs an argument")
	}
	switch args[0] {
	case "db":
		i.idx.DeleteDatabase(args[1])
	case "collections":
		i.idx.DeleteCollection(i.CurrentDB, args[1])
	case "documents":
		i.idx.DeleteDocument(i.CurrentDB, i.CurrentColl, args[1])
	default:
		return fmt.Errorf("unkown argument for delete: %s", args[0])
	}
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
