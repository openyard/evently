package consume_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/openyard/evently/command/es"
	"github.com/openyard/evently/event"
	"github.com/openyard/evently/query/consume"
)

func TestDefaultConsumer_Handle(t *testing.T) {
	called := make(chan string, 2)
	firstHandle := func(e ...*event.Event) error {
		for _, ev := range e {
			t.Logf("processed %v by first handle", ev)
		}
		called <- "first handle called"
		return nil
	}
	secondHandle := func(e ...*event.Event) error {
		for _, ev := range e {
			t.Logf("processed %v by second handle", ev)
		}
		called <- "second handle called"
		return errors.New("test-error")
	}
	thirdHandle := func(e ...*event.Event) error {
		t.Fatal("unexpected call for third handle")
		return nil
	}

	go func() {
		c := consume.DefaultConsumer
		c.Consume(firstHandle, secondHandle, thirdHandle)
		err := c.Handle(
			&consume.Context{},
			es.NewEntry(1, event.NewDomainEvent("test/event-1", "4711")),
			es.NewEntry(2, event.NewDomainEvent("test/event-2", "4711")))
		strings.EqualFold("test-error", err.Error())

	}()

	t.Log(<-called)
	t.Log(<-called)
}
