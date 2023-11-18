package event_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/openyard/evently/event"
)

func TestHandleFunc_Handle(t *testing.T) {
	called := make(chan string, 2)
	testHandle := func(e ...*event.Event) error {
		for _, ev := range e {
			t.Logf("processed %v by test handle", ev)
			called <- "test handle called"
		}
		return nil
	}
	go func() {
		err := testHandle(
			event.NewDomainEvent("test/event-1", "4711"),
			event.NewDomainEvent("test/event-2", "4711"))
		strings.EqualFold("nil", fmt.Sprintf("%v", err))
	}()

	t.Log(<-called)
	t.Log(<-called)
}
