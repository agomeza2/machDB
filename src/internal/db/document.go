package core

import "fmt"

// Document → un documento JSON que contiene múltiples objetos
type Document struct {
	Name      string
	Objects   []*Object `json:"objects"`
	nextObjID int       `json:"-"`
}

func NewDocument(name string) *Document {
	return &Document{
		Name:      name,
		Objects:   []*Object{},
		nextObjID: 0,
	}
}

// InsertObject -> inserta 1 objeto y devuelve su id asignado
func (d *Document) InsertObject(fields map[string]interface{}) int {
	obj := NewObject(d.nextObjID, fields)
	d.Objects = append(d.Objects, obj)
	d.nextObjID++
	return obj.ID
}

// InsertObjects -> inserta varios objetos y devuelve slice de ids
func (d *Document) InsertObjects(objs []map[string]interface{}) []int {
	ids := make([]int, 0, len(objs))
	for _, f := range objs {
		ids = append(ids, d.InsertObject(f))
	}
	return ids
}

// GetObjectByID -> devuelve puntero al objeto o nil
func (d *Document) GetObjectByID(id int) *Object {
	for _, o := range d.Objects {
		if o.ID == id {
			return o
		}
	}
	return nil
}

// ModifyObjects -> aplica updates a objetos que cumplan filter (filter: map[key]value)
// soporta filter {"id": 0} para buscar por id
func (d *Document) ModifyObject(id int, newData map[string]interface{}) error {
	for i, obj := range d.Objects {
		if obj.ID == id {
			// Actualiza solo las claves que estén en newData
			for k, v := range newData {
				d.Objects[i].Fields[k] = v
			}
			return nil
		}
	}
	return fmt.Errorf("objeto con id %d no encontrado", id)
}
func (d *Document) ModifyObjects(filter map[string]interface{}, updates map[string]interface{}) error {
	found := false
	for _, obj := range d.Objects {
		if matchesFilter(obj, filter) {
			for k, v := range updates {
				obj.Fields[k] = v
			}
			found = true
		}
	}
	if !found {
		return fmt.Errorf("no objects match the filter")
	}
	return nil
}

// DeleteObjects -> elimina objetos que cumplan filter
func (d *Document) DeleteObjects(filter map[string]interface{}) error {
	found := false
	newObjs := make([]*Object, 0, len(d.Objects))
	for _, obj := range d.Objects {
		if matchesFilter(obj, filter) {
			found = true
			continue
		}
		newObjs = append(newObjs, obj)
	}
	if !found {
		return fmt.Errorf("no objects match the filter")
	}
	d.Objects = newObjs
	return nil
}
func (d *Document) Print() {
	fmt.Printf("=== Documento: %s ===\n", d.Name)
	for id, obj := range d.Objects {
		fmt.Printf("ID: %d\n", id)
		for k, v := range obj.Fields {
			fmt.Printf("  %s: %v\n", k, v)
		}
	}
}

// helper
func matchesFilter(obj *Object, filter map[string]interface{}) bool {
	if filter == nil || len(filter) == 0 {
		return true
	}
	for k, v := range filter {
		// si piden id especial
		if k == "id" || k == "_id" {
			// comparacion numérica asumiendo v es number/int
			switch tv := v.(type) {
			case int:
				if obj.ID != tv {
					return false
				}
			case float64:
				if obj.ID != int(tv) {
					return false
				}
			default:
				return false
			}
			continue
		}
		if val, ok := obj.Fields[k]; !ok || val != v {
			return false
		}
	}
	return true
}
