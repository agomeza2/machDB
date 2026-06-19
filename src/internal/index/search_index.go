package index

type SearchIndex struct{ 
	Index InvertedIndex
}

func NewSearchIndex () *SearchIndex{
	return &SearchIndex{
		Index InvertedIndex
	} 
}

func (s* SearchIndex) findDocument (query string) *String {
    return "" 
}
func (s* SearchIndex) findProperty (query string) *String {
    return "" 
}      