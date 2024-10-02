package volatile_test

import (
	"testing"

	"github.com/openyard/evently/event"
	"github.com/openyard/evently/pkg/volatile"
	"github.com/openyard/evently/tact/es"
)

func TestInMemoryEventStore_ReadStream(t *testing.T) {
	volatile.WithVolatileEventStore(func(_es *volatile.EventStore) {
		history, err := _es.Read("empty")
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

func TestInMemoryEventStore_AppendToStream(t *testing.T) {
	volatile.WithVolatileEventStore(func(_es *volatile.EventStore) {
		changes := []*event.Event{{}, {}}
		if err := _es.Append(es.NewChange("parts", 0, changes...)); err != nil {
			t.Errorf("failed to append partOne: %s", err.Error())
			return
		}
		if err := _es.Append(es.NewChange("parts", 2, changes...)); err != nil {
			t.Errorf("failed to append partTwo: %s", err.Error())
			return
		}
		history, err := _es.Read("parts")
		if err != nil {
			t.Errorf("could not read stream: %s", err.Error())
			return
		}
		if len(history[0].Events()) != 4 {
			t.Logf("%+v", history)
			t.Errorf("unexpected history length: %d expected 4", len(history))
			return
		}
	})
}
