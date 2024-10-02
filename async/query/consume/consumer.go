package consume

import (
	"fmt"
	"sync"

	"github.com/openyard/evently/event"
	"github.com/openyard/evently/tact/es"
)

type Consumer interface {
	Consume(ctx Context, entries ...*es.Entry) error
}

type ConsumerFunc func(Context, ...*es.Entry) error

func (f ConsumerFunc) Consume(ctx Context, events ...*es.Entry) error {
	return f(ctx, events...)
}

func Consume(handler ...event.HandleFunc) {
	DefaultConsumer.RegisterHandler(handler...)
}

var DefaultConsumer = &defaultConsumer{}

type defaultConsumer struct {
	mux     sync.RWMutex
	handler []event.HandleFunc
}

func NewConsumer(handle ...event.HandleFunc) Consumer {
	return &defaultConsumer{handler: handle}
}

func (c *defaultConsumer) Consume(ctx Context, entries ...*es.Entry) error {
	c.mux.Lock()
	defer c.mux.Unlock()
	if len(c.handler) == 0 {
		return fmt.Errorf("[ERROR][%T] no handlers registered", c)
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

//func (c *defaultConsumer) Consumer() Consumer {
//	return ConsumerFunc(c.Consume)
//}

func (c *defaultConsumer) RegisterHandler(handler ...event.HandleFunc) {
	c.mux.Lock()
	defer c.mux.Unlock()
	for _, h := range handler {
		c.handler = append(c.handler, h)
	}
}
