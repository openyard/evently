package consume

import (
	"github.com/openyard/evently/command/es"
	"github.com/openyard/evently/event"
)

type Consumer interface {
	Handle(ctx *Context, events ...*es.Entry) error
}

func Consume(handler ...event.HandleFunc) {
	DefaultConsumer.Consume(handler...)
}

func ConsumeFunc(consumer func(...*event.Event) error) {
	DefaultConsumer.ConsumeFunc(consumer)
}

type ConsumerFunc func(*Context, ...*es.Entry) error

func (f ConsumerFunc) Handle(ctx *Context, events ...*es.Entry) error {
	return f(ctx, events...)
}

var DefaultConsumer defaultConsumer

type defaultConsumer struct {
	handler []event.HandleFunc
}

func (c *defaultConsumer) Handle(ctx *Context, entries ...*es.Entry) error {
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
		events = append(events, entry.Event)
	}
	return events
}

func (c *defaultConsumer) Consumer() Consumer {
	return ConsumerFunc(c.Handle)
}

func (c *defaultConsumer) Consume(handler ...event.HandleFunc) {
	for _, h := range handler {
		c.handler = append(c.handler, h)
	}
}

func (c *defaultConsumer) ConsumeFunc(consumer ...event.HandleFunc) {
	c.Consume(consumer...)
}
