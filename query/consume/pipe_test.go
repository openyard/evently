package consume_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/openyard/evently/command/es"
	"github.com/openyard/evently/event"
	"github.com/openyard/evently/query/consume"
)

func TestConsumePipe_Handle(t *testing.T) {
	called := make(chan string, 2)
	firstConsumer := func(ctx consume.Context, e ...*es.Entry) error {
		for _, ev := range e {
			t.Logf("processed %+v by first handle", ev)
		}
		called <- "test handle #1 called"
		return nil
	}
	secondConsumer := func(ctx consume.Context, e ...*es.Entry) error {
		for _, ev := range e {
			t.Logf("processed %v by second handle", ev)
		}
		called <- "test handle #2 called"
		return errors.New("test-error")
	}
	go func() {
		p := consume.NewPipe(firstConsumer, consume.ConsumerFunc(secondConsumer))
		err := p.Consume(
			consume.Context{},
			es.NewEntry(1, event.NewDomainEvent("test/event-1", "4711")),
			es.NewEntry(2, event.NewDomainEvent("test/event-2", "4711")))
		strings.EqualFold("test-error", err.Error())
	}()

	t.Log(<-called)
	t.Log(<-called)
}
