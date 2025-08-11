package core

import "fmt"

// Collection → representa una colección dentro de una base de datos
type Collection struct {
	Name      string
	Documents map[string]*Document `json:"documents"`
}

// NewCollection → constructor
func NewCollection(name string) *Collection {
	return &Collection{
		Name:      name,
		Documents: make(map[string]*Document),
	}
}

// CreateDocument → crea un documento vacío con un ID
func (c *Collection) CreateDocument(id string) error {
	if _, exists := c.Documents[id]; exists {
		return fmt.Errorf("document %s already exists", id)
	}
	c.Documents[id] = NewDocument(id)
	return nil
}

// GetDocument → obtiene un documento por ID
func (c *Collection) GetDocument(id string) (*Document, error) {
	doc, ok := c.Documents[id]
	if !ok {
		return nil, fmt.Errorf("document %s not found", id)
	}
	return doc, nil
}

// DeleteDocument → elimina un documento
func (c *Collection) DeleteDocument(id string) error {
	if _, ok := c.Documents[id]; !ok {
		return fmt.Errorf("document %s not found", id)
	}
	delete(c.Documents, id)
	return nil
}
