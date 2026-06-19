package engine

import (
	"fmt"
	db "machDB/src/internal/db"
	idx "machDB/src/internal/index"
	"strings"
	"sync"
)

type Engine struct {
	Databases map[string]*db.Database `json:"databases"`
	Index     idx.Index
	mu        sync.RWMutex
	basePath  string
}

func NewIndex() *Engine {
	basePath := "/db"
	return &Engine{
		Databases: make(map[string]*db.Database),
		Index:     make(idx.InvertedIndex),
		basePath:  basePath,
	}
}

// CreateDatabase crea una DB en memoria usando tu struct Database
func (idx *Index) CreateDatabase(name string) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if _, exists := idx.Databases[name]; exists {
		return fmt.Errorf("database %s already exists", name)
	}
	idx.Databases[name] = db.NewDatabase(name)
	return nil
}

// CreateCollection crea una colección dentro de una DB ya existente
func (idx *Index) CreateCollection(dbName, colName string) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	database, ok := idx.Databases[dbName]
	if !ok {
		return fmt.Errorf("database %s not found", dbName)
	}
	return database.CreateCollection(colName)
}

// CreateDocument crea un documento (vacío) dentro de una colección
func (idx *Index) CreateDocument(dbName, colName, docName string) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	database, ok := idx.Databases[dbName]
	if !ok {
		return fmt.Errorf("database %s not found", dbName)
	}
	col, err := database.GetCollection(colName)
	if err != nil {
		return err
	}
	return col.CreateDocument(docName)
}

// InsertObject inserta y actualiza el índice. Devuelve el object ID asignado.
func (idx *Index) InsertObject(dbName, colName, docName string, fields map[string]interface{}) (int, error) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	database, ok := idx.Databases[dbName]
	if !ok {
		return 0, fmt.Errorf("database %s not found", dbName)
	}
	col, err := database.GetCollection(colName)
	if err != nil {
		return 0, err
	}
	doc, err := col.GetDocument(docName)
	if err != nil {
		return 0, err
	}

	// Inserta en Document (esto devuelve el id)
	oid := doc.InsertObject(fields)

	// Indexar: por cada campo k:v añadimos ObjectRef
	for k, v := range fields {
		valStr := fmt.Sprintf("%v", v)
		if _, ok := idx.Index[k]; !ok {
			idx.Index[k] = make(map[string][]ObjectRef)
		}
		ref := ObjectRef{DB: dbName, Collection: colName, Document: docName, ID: oid}
		idx.Index[k][valStr] = append(idx.Index[k][valStr], ref)
	}

	return oid, nil
}

// Find busca en todo el índice (todas las DBs). Si quieres restringir por DB/collection, implementamos FindIn
func (idx *Index) Find(field, value string, dbName string, collections ...string) ([]*db.Object, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	m, ok := idx.Index[field]
	if !ok {
		return nil, fmt.Errorf("field %s not indexed", field)
	}

	refs, ok := m[value]
	if !ok {
		return nil, fmt.Errorf("value %s for field %s not found", value, field)
	}

	collectionFilter := make(map[string]bool)
	for _, c := range collections {
		collectionFilter[c] = true
	}

	var results []*db.Object

	for _, ref := range refs {
		if ref.DB != dbName {
			continue
		}
		if len(collectionFilter) > 0 && !collectionFilter[ref.Collection] {
			continue
		}

		database, ok := idx.Databases[ref.DB]
		if !ok {
			continue
		}
		col, err := database.GetCollection(ref.Collection)
		if err != nil {
			continue
		}
		doc, err := col.GetDocument(ref.Document)
		if err != nil {
			continue
		}
		obj := doc.GetObjectByID(ref.ID)
		if obj == nil {
			continue
		}

		results = append(results, obj)
	}

	if len(results) == 0 {
		return nil, fmt.Errorf("no results found")
	}

	return results, nil
}

func (idx *Index) FindByQuery(query string, db string, collections ...string) ([]*db.Object, error) {
	parts := strings.SplitN(query, ":", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("bad query format")
	}
	field := parts[0]
	value := parts[1]
	return idx.Find(field, value, db, collections...)
}
func (idx *Index) test() {
	fmt.Println("Test method called")
}

func (idx *Index) ListDatabases() []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	dbNames := make([]string, 0, len(idx.Databases))
	for name := range idx.Databases {
		dbNames = append(dbNames, name)
	}
	return dbNames
}
func (idx *Index) ListCollections(dbName string) ([]string, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	database, ok := idx.Databases[dbName]
	if !ok {
		return nil, fmt.Errorf("database %s no encontrada", dbName)
	}

	collections := make([]string, 0, len(database.Collections))
	for colName := range database.Collections {
		collections = append(collections, colName)
	}

	return collections, nil
}
func (idx *Index) ListDocuments(dbName, colName string) ([]string, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	database, ok := idx.Databases[dbName]
	if !ok {
		return nil, fmt.Errorf("database %s no encontrada", dbName)
	}

	collection, err := database.GetCollection(colName)
	if err != nil {
		return nil, err
	}

	documents := make([]string, 0, len(collection.Documents))
	for docName := range collection.Documents {
		documents = append(documents, docName)
	}

	return documents, nil
}

// DeleteDatabase elimina una base de datos completa y sus referencias en el índice.
func (idx *Index) DeleteDatabase(dbName string) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	_, ok := idx.Databases[dbName]
	if !ok {
		return fmt.Errorf("database %s not found", dbName)
	}

	// Eliminar referencias del índice invertido relacionadas con esta DB
	for field, valMap := range idx.Index {
		for val, refs := range valMap {
			newRefs := refs[:0]
			for _, ref := range refs {
				if ref.DB != dbName {
					newRefs = append(newRefs, ref)
				}
			}
			if len(newRefs) == 0 {
				delete(valMap, val)
			} else {
				valMap[val] = newRefs
			}
		}
		if len(valMap) == 0 {
			delete(idx.Index, field)
		}
	}

	// Finalmente eliminar la base de datos
	delete(idx.Databases, dbName)
	return nil
}

// DeleteCollection elimina una colección y sus referencias en el índice dentro de una base de datos.
func (idx *Index) DeleteCollection(dbName, colName string) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	database, ok := idx.Databases[dbName]
	if !ok {
		return fmt.Errorf("database %s no encontrada", dbName)
	}

	_, ok = database.Collections[colName]
	if !ok {
		return fmt.Errorf("colección %s no encontrada en database %s", colName, dbName)
	}

	// Eliminar referencias en el índice invertido de esta colección
	for field, valMap := range idx.Index {
		for val, refs := range valMap {
			newRefs := refs[:0]
			for _, ref := range refs {
				if !(ref.DB == dbName && ref.Collection == colName) {
					newRefs = append(newRefs, ref)
				}
			}
			if len(newRefs) == 0 {
				delete(valMap, val)
			} else {
				valMap[val] = newRefs
			}
		}
		if len(valMap) == 0 {
			delete(idx.Index, field)
		}
	}

	// Eliminar colección
	delete(database.Collections, colName)
	return nil
}

// DeleteDocument elimina un documento y sus referencias en el índice dentro de una colección y base de datos.
func (idx *Index) DeleteDocument(dbName, colName, docName string) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	database, ok := idx.Databases[dbName]
	if !ok {
		return fmt.Errorf("database %s no encontrada", dbName)
	}

	collection, ok := database.Collections[colName]
	if !ok {
		return fmt.Errorf("colección %s no encontrada en database %s", colName, dbName)
	}

	doc, ok := collection.Documents[docName]
	if !ok {
		return fmt.Errorf("documento %s no encontrado en colección %s", docName, colName)
	}

	// Eliminar referencias en el índice invertido para cada objeto del documento
	for oid, obj := range doc.Objects {
		for field, value := range obj.Fields {
			valStr := fmt.Sprintf("%v", value)
			if valMap, ok := idx.Index[field]; ok {
				if refs, ok := valMap[valStr]; ok {
					newRefs := refs[:0]
					for _, ref := range refs {
						if !(ref.DB == dbName && ref.Collection == colName && ref.Document == docName && ref.ID == oid) {
							newRefs = append(newRefs, ref)
						}
					}
					if len(newRefs) == 0 {
						delete(valMap, valStr)
					} else {
						valMap[valStr] = newRefs
					}
				}
				if len(valMap) == 0 {
					delete(idx.Index, field)
				}
			}
		}
	}

	// Eliminar documento
	delete(collection.Documents, docName)
	return nil
}

func (e *Engine) catch(bson json) {

}
