package main

import (
	"fmt"
	core "machDB/src/internal/db"
	"machDB/src/internal/index"
)

func main() {
	idx := index.NewIndex()

	// Crea base de datos, colección y documento (simples)
	db := core.NewDatabase("ventas")
	idx.Databases[db.Name] = db
	col := core.NewCollection("clientes")
	db.Collections[col.Name] = col
	doc := core.NewDocument("registro")
	col.Documents[doc.Name] = doc

	col2 := core.NewCollection("usuarios")
	db.Collections[col2.Name] = col2
	doc2 := core.NewDocument("usuarios_sura")
	col2.Documents[doc2.Name] = doc2

	// Inserta objetos
	idx.InsertObject("ventas", "clientes", "registro", map[string]interface{}{
		"name": "Pedro",
		"eps":  "colfamilia",
	})
	idx.InsertObject("ventas", "clientes", "registro", map[string]interface{}{
		"name": "Luis",
		"eps":  "colsanitas",
	})
	idx.InsertObject("ventas", "clientes", "registro", map[string]interface{}{
		"name": "Luis",
		"eps":  "Medico Preventiva",
		"edad": 37,
	})

	idx.InsertObject("ventas", "usuarios", "usuarios_sura", map[string]interface{}{
		"name": "Luis",
		"age":  25,
	})
	idx.InsertObject("ventas", "usuarios", "usuarios_sura", map[string]interface{}{
		"name": "Juan",
		"age":  30,
	})
	idx.InsertObject("ventas", "usuarios", "usuarios_sura", map[string]interface{}{
		"name": "Luis",
		"age":  22,
	})

	fmt.Printf("Índice para campo 'name': %+v\n", idx.Index["name"])

	// Busca "name:Luis"
	results, err1 := idx.FindByQuery("name:Luis", "ventas")
	fmt.Println("Found results:")
	if err1 != nil {
		fmt.Println("Error en búsqueda:", err1)
		return
	}

	for _, obj := range results {
		fmt.Printf("Objeto encontrado: %+v\n", obj.Fields)
	}
}
