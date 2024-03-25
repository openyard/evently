package command

// HandleFunc ...
type HandleFunc func(c *Command) error

// Handle ...
func (chf HandleFunc) Handle(c *Command) error {
	return chf(c)
}

type BatchFunc func(b *Batch) error

func (bf BatchFunc) Handle(b *Batch) error {
	return bf(b)
}
