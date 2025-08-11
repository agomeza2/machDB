package core

import "fmt"

// Database → representa una base de datos en memoria
type Database struct {
	Name        string
	Collections map[string]*Collection `json:"collections"`
}

// NewDatabase → constructor
func NewDatabase(name string) *Database {
	return &Database{
		Name:        name,
		Collections: make(map[string]*Collection),
	}
}

// CreateCollection → crea una colección
func (db *Database) CreateCollection(name string) error {
	if _, exists := db.Collections[name]; exists {
		return fmt.Errorf("collection %s already exists", name)
	}
	db.Collections[name] = NewCollection(name)
	return nil
}

// GetCollection → obtiene una colección
func (db *Database) GetCollection(name string) (*Collection, error) {
	col, ok := db.Collections[name]
	if !ok {
		return nil, fmt.Errorf("collection %s not found", name)
	}
	return col, nil
}

// DeleteCollection → elimina una colección
func (db *Database) DeleteCollection(name string) error {
	if _, ok := db.Collections[name]; !ok {
		return fmt.Errorf("collection %s not found", name)
	}
	delete(db.Collections, name)
	return nil
}
