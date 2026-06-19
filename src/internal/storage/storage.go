package storage

import (
	"sync"
	e "machDB/src/internal/engine"
)

type Storage struct{
	path_save string
	path_read string 
	mu sync.RWMutex  
} 

func NewStorage(path string)*Storage{
	path_read := "/db" 
	return &Storage{
		path_save string, 
		path_read string 
	}
}

func (e *) LoadFromDisk() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Limpiar estructura actual
	e.Databases = make(map[string]*db.Database)
	e.Index = make(InvertedIndex)

	// Leer directorio base (idx.basePath), por ejemplo "/db"
	dbEntries, err := os.ReadDir(idx.basePath)
	if err != nil {
		return err
	}

	for _, dbEntry := range dbEntries {
		if !dbEntry.IsDir() {
			continue
		}
		dbName := dbEntry.Name()
		database := db.NewDatabase(dbName)
		dbPath := filepath.Join(idx.basePath, dbName)

		colEntries, err := os.ReadDir(dbPath)
		if err != nil {
			return err
		}

		for _, colEntry := range colEntries {
			if !colEntry.IsDir() {
				continue
			}
			colName := colEntry.Name()
			if err := database.CreateCollection(colName); err != nil {
				return err
			}
			collection, err := database.GetCollection(colName)
			if err != nil {
				return err
			}

			colPath := filepath.Join(dbPath, colName)
			docEntries, err := os.ReadDir(colPath)
			if err != nil {
				return err
			}

			for _, docEntry := range docEntries {
				if docEntry.IsDir() || !strings.HasSuffix(docEntry.Name(), ".json") {
					continue
				}
				docName := strings.TrimSuffix(docEntry.Name(), ".json")
				docPath := filepath.Join(colPath, docEntry.Name())

				f, err := os.Open(docPath)
				if err != nil {
					return err
				}

				var doc db.Document
				dec := json.NewDecoder(f)
				if err := dec.Decode(&doc); err != nil {
					f.Close()
					return err
				}
				f.Close()

				collection.Documents[docName] = &doc

				// Reconstruir índice invertido para cada objeto del documento
				for oid, obj := range doc.Objects {
					for k, v := range obj.Fields {
						valStr := fmt.Sprintf("%v", v)
						if _, ok := idx.Index[k]; !ok {
							idx.Index[k] = make(map[string][]ObjectRef)
						}
						ref := ObjectRef{DB: dbName, Collection: colName, Document: docName, ID: oid}
						idx.Index[k][valStr] = append(idx.Index[k][valStr], ref)
					}
				}
			}
		}

		idx.Databases[dbName] = database
	}

	return nil
}
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
			return err
				}
			}
		}
	}
	return nil
}
