package event

// HandleFunc ...
type HandleFunc func(event *Event) error

// Handle serves the EventHandler interface
func (ehf HandleFunc) Handle(e *Event) error {
	return ehf(e)
}

// OnHandledFunc is a callback for successful event handling
type OnHandledFunc func(*Event)
