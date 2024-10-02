package consume

import (
	"github.com/openyard/evently/tact/es"
)

var _ Consumer = (*Filter)(nil)

// Filter acts as single consume.Consumer
type Filter struct {
	consumer Consumer
}

// NewConsumeFilter returns a new consume.Filter with consume.DefaultConsumer
func NewConsumeFilter() *Filter {
	return &Filter{consumer: DefaultConsumer}
}

// NewConsumeFilterWithConsumer returns a new consume.Filter with the given consume.Consumer
func NewConsumeFilterWithConsumer(c Consumer) *Filter {
	return &Filter{consumer: c}
}

// Consume consumes the provided entries in given consume.Context
func (f *Filter) Consume(ctx Context, entries ...*es.Entry) error {
	return f.consumer.Consume(ctx, entries...)
}
