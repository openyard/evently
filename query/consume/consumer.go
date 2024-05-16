package consume

import (
	"fmt"
	"github.com/openyard/evently/command/es"
	"github.com/openyard/evently/event"
	"sync"
)

type Consumer interface {
	Handle(ctx Context, entries ...*es.Entry) error
}

type ConsumerFunc func(Context, ...*es.Entry) error

func (f ConsumerFunc) Handle(ctx Context, events ...*es.Entry) error {
	return f(ctx, events...)
}

func Consume(handler ...event.HandleFunc) {
	DefaultConsumer.Consume(handler...)
}

var DefaultConsumer = &defaultConsumer{}

type defaultConsumer struct {
	mux     sync.RWMutex
	handler []event.HandleFunc
}

func (c *defaultConsumer) Handle(ctx Context, entries ...*es.Entry) error {
	c.mux.Lock()
	defer c.mux.Unlock()
	if len(c.handler) == 0 {
		return fmt.Errorf("[%T][ERROR] no handlers registered", c)
	}
	events := entries2events(entries)
	for _, h := range c.handler {
		if err := h.Handle(events...); err != nil {
			return err
		}
	}
	return nil
}

func entries2events(entries []*es.Entry) []*event.Event {
	events := make([]*event.Event, 0)
	for _, entry := range entries {
		events = append(events, entry.Event())
	}
	return events
}

func (c *defaultConsumer) Consumer() Consumer {
	return ConsumerFunc(c.Handle)
}

func (c *defaultConsumer) Consume(handler ...event.HandleFunc) {
	c.mux.Lock()
	defer c.mux.Unlock()
	for _, h := range handler {
		c.handler = append(c.handler, h)
	}
}
