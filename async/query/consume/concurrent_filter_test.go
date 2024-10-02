package consume_test

import (
	"fmt"
	"testing"

	"github.com/openyard/evently/async/query/consume"
	"github.com/openyard/evently/event"
	"github.com/openyard/evently/tact/es"
	"github.com/stretchr/testify/assert"
)

func TestNewConcurrentFilter(t *testing.T) {
	called := make(chan string, 2)
	acks := make(chan string, 1)
	nacks := make(chan string, 1)

	consumer := func(e ...*event.Event) error {
		t.Helper()
		var err error
		for _, ev := range e {
			t.Logf("processed %v", ev)
			if ev.AggregateID() == "payment-124" {
				err = fmt.Errorf("test-error for <%s-%s>", ev.ID(), ev.Name())
				called <- "handle failed"
			}
		}
		called <- "handle called"
		return err
	}
	go func() {
		t.Helper()
		consume.Consume(consumer)
		sut := consume.NewConcurrentFilter(ackNack(t, acks), ackNack(t, nacks))
		err := sut.Consume(testContext(), entries()...)
		defer sut.Stop()
		assert.NoError(t, err)
	}()

	t.Logf(<-called)
	t.Logf(<-called)
	t.Logf(<-called)

	t.Logf(" acked entry %s", <-acks)
	t.Logf("nacked entry %s", <-nacks)
}

func ackNack(t *testing.T, res chan<- string) func(entries ...*es.Entry) {
	return func(entries ...*es.Entry) {
		t.Helper()
		for _, e := range entries {
			res <- fmt.Sprintf("#%d@%s id=%s", e.GlobalPos(), e.Event().Name(), e.Event().ID())
		}
	}
}
