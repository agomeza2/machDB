package index

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	// <-- CAMBIA ESTA RUTA por la ruta real de tu paquete db (según go.mod).
	// Por ejemplo: db "github.com/tuuser/machDB/internal/db"
	db "machDB/src/internal/db"
)

// ObjectRef: referencia a un objeto dentro de la jerarquía
type ObjectRef struct {
	DB         string `json:"db"`
	Collection string `json:"collection"`
	Document   string `json:"document"`
	ID         int    `json:"id"`
}

// InvertedIndex: campo -> valor -> lista de refs
type InvertedIndex map[string]map[string][]ObjectRef

// Index: almacén RAM + índice invertido
type Index struct {
	Databases map[string]*db.Database `json:"databases"`
	Index     InvertedIndex           `json:"index"`
	mu        sync.RWMutex
	basePath  string // ruta donde se hará FlushToDisk
}

// NewIndex crea un index en memoria. basePath es la carpeta donde se persistirá al cerrar.
func NewIndex() *Index {
	basePath := "/db"
	return &Index{
		Databases: make(map[string]*db.Database),
		Index:     make(InvertedIndex),
		basePath:  basePath,
	}
}

func (ref ObjectRef) GetObject(idx *Index) (*db.Object, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	db, ok := idx.Databases[ref.DB]
	if !ok {
		return nil, fmt.Errorf("db no encontrada")
	}
	col, ok := db.Collections[ref.Collection]
	if !ok {
		return nil, fmt.Errorf("collection no encontrada")
	}
	doc, ok := col.Documents[ref.Document]
	if !ok {
		return nil, fmt.Errorf("documento no encontrado")
	}
	if ref.ID < 0 || ref.ID >= len(doc.Objects) {
		return nil, fmt.Errorf("ID de objeto fuera de rango")
	}
	return doc.Objects[ref.ID], nil
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
		return nil, fmt.Errorf("query mal formada, debe ser campo:valor")
	}
	field := parts[0]
	value := parts[1]
	return idx.Find(field, value, db, collections...)
}

// FlushToDisk: recorre idx.Databases y escribe en disco (carpetas + archivos JSON)
// Hace mkdir -p basePath/dbName/collectionName y guarda cada documento como JSON <doc>.json
func (idx *Index) FlushToDisk() error {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	for dbName, database := range idx.Databases {
		dbPath := filepath.Join(idx.basePath, dbName)
		if err := os.MkdirAll(dbPath, 0o755); err != nil {
			return err
		}

		for colName, col := range database.Collections {
			colPath := filepath.Join(dbPath, colName)
			if err := os.MkdirAll(colPath, 0o755); err != nil {
				return err
			}

			for docName, doc := range col.Documents {
				// Serializar documento como JSON
				outPath := filepath.Join(colPath, docName+".json")
				tmpPath := outPath + ".tmp"

				f, err := os.Create(tmpPath)
				if err != nil {
					return err
				}
				enc := json.NewEncoder(f)
				enc.SetIndent("", "  ")
				if err := enc.Encode(doc); err != nil {
					f.Close()
					return err
				}
				f.Close()
				// renombrar atómico
				if err := os.Rename(tmpPath, outPath); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
