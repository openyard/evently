package command

// Error codes
const (
	// ErrTimeout thrown when Command isn't executed within given Timeout
	ErrTimeout = iota + 9001
	// ErrUnknownCommand thrown when given Command is unknown
	ErrUnknownCommand
)
