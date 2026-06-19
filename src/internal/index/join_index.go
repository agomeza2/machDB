package index

type JoinIndex struct{ 
	Index InvertedIndex
}

func NewJoinIndex () *JoinIndex{
	return &JoinIndex{
		Index InvertedIndex
	} 
}

func (s* SearchIndex) JoinPropertiesInner (query string) *String {
    return "" 
}

func (s* SearchIndex) JoinPropertiesOuter (query string) *String {
    return "" 
}

func (s* SearchIndex) JoinPropertiesleft (query string) *String {
    return "" 
}

func (s* SearchIndex) JoinPropertiesRight (query string) *String {
    return "" 
}