package cmd

// HandleFunc ...
type HandleFunc func(c *Command) error

// Handle ...
func (chf HandleFunc) Handle(c *Command) error {
	return chf(c)
}
