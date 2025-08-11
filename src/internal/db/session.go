package core

// Base struct for User
type SessionManager struct {
	session   bool
	documents map[string]*Document
}
