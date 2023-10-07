package command

import (
	"time"

	"github.com/openyard/evently/pkg/uuid"
)

// Command defines the structure for an immutable command
type Command struct {
	name            string
	id              string
	aggregateID     string
	expectedVersion uint64
	issuedAt        time.Time
	payload         []byte

	done chan bool
	err  chan error
}

// New returns a new command with given name and aggregateID and unique generated UUIDv4 issued now
// without payload
func New(name, aggregateID string, opts ...Option) *Command {
	c := &Command{
		name:        name,
		id:          uuid.NewV4().String(),
		aggregateID: aggregateID,
		issuedAt:    time.Now().UTC(),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

type Option func(cmd *Command)

// WithPayload adds the given payload
func WithPayload(payload []byte) Option {
	return func(c *Command) {
		c.payload = payload
	}
}

// WithExpectedVersion sets the expected version
func WithExpectedVersion(expectedVersion uint64) Option {
	return func(c *Command) {
		c.expectedVersion = expectedVersion
	}
}

func (c *Command) CommandName() string {
	return c.name
}

func (c *Command) CommandID() string {
	return c.id
}

func (c *Command) AggregateID() string {
	return c.aggregateID
}

func (c *Command) ExpectedVersion() uint64 {
	return c.expectedVersion
}

func (c *Command) Payload() []byte {
	return c.payload
}

// Executed marks the command as done
func (c *Command) Executed() {
	c.done <- true
}

// Failed marks the command as failed with given error
func (c *Command) Failed(err error) {
	c.err <- err
}

// Error returns the error channel of the command
func (c *Command) Error() <-chan error {
	return c.err
}

// Done returns the done channel of the command
func (c *Command) Done() <-chan bool {
	return c.done
}
