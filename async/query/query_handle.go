package query

// HandleFunc ...
type HandleFunc func(q Query) error

// Handle ...
func (qhf HandleFunc) Handle(q Query) error {
	return qhf(q)
}
