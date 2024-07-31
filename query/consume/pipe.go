package consume

import "github.com/openyard/evently/command/es"

type consumePipe struct {
	consume ConsumerFunc
	next    Consumer
}

func NewPipe(f ConsumerFunc, next Consumer) Consumer {
	return &consumePipe{
		consume: f,
		next:    next,
	}
}

func (c consumePipe) Consume(ctx Context, entries ...*es.Entry) error {
	if err := c.consume(ctx, entries...); err != nil {
		return err
	}
	return c.next.Consume(ctx, entries...)
}
