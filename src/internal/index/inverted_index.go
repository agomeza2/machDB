package index

import (
	"sync"
)

type ObjectRef struct {
	DB         string `bson:"db"`
	Collection string `bson:"collection"`
	Document   string `bson:"document"`
	ID         int    `bson:"id"`
	mu         sync.RWMutex
}

// InvertedIndex: campo -> valor -> lista de refs
type InvertedIndex map[string]map[string][]ObjectRef

func createIndexDocuments(path string) {

}

func createIndexProperties(path string) {

}
