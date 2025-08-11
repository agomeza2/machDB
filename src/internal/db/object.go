package core

// Object → entidad dentro de un documento
type Object struct {
	ID     int                    `json:"id"`
	Fields map[string]interface{} `json:"fields"`
}

func NewObject(id int, fields map[string]interface{}) *Object {
	return &Object{
		ID:     id,
		Fields: fields,
	}
}
