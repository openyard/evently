package volatile_test

import (
	"testing"

	"github.com/openyard/evently/event"
	"github.com/openyard/evently/pkg/volatile"
)

func TestInMemoryEventStore_ReadStream(t *testing.T) {
	volatile.WithVolatileEventStore(func(es *volatile.EventStore) {
		history, err := es.ReadStream("empty")
		if err != nil {
			t.Error(err)
			return
		}
		if len(history) != 0 {
			t.Errorf("there are events in an supposed to be empty history: %+v", history)
			return
		}
	})
}

func TestInMemoryEventStore_AppentToStream(t *testing.T) {
	volatile.WithVolatileEventStore(func(es *volatile.EventStore) {
		changes := []*event.Event{{}, {}}
		if err := es.AppendToStream("parts", 0, changes...); err != nil {
			t.Errorf("failed to append partOne: %s", err.Error())
			return
		}
		if err := es.AppendToStream("parts", 2, changes...); err != nil {
			t.Errorf("failed to append partTwo: %s", err.Error())
			return
		}
		history, err := es.ReadStream("parts")
		if err != nil {
			t.Errorf("could not read stream: %s", err.Error())
			return
		}
		if len(history) != 4 {
			t.Errorf("unexpected history length: %d expected 4", len(history))
			return
		}
	})
}
