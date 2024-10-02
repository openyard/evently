package volatile_test

import (
	"sync"
	"testing"
	"time"

	"github.com/openyard/evently/async/query/consume"
	"github.com/openyard/evently/event"
	"github.com/openyard/evently/pkg/volatile"
	"github.com/openyard/evently/tact/es"
	"github.com/stretchr/testify/assert"
)

func TestInMemoryInbox_QueueEntries(t *testing.T) {
	volatile.WithVolatileInbox(func(sut consume.Inbox) {
		var done sync.WaitGroup
		done.Add(6)

		if err := sut.QueueEntries(consume.NewContext("test-subscription", []consume.Msg{}), time.Now(), []*es.Entry{
			es.NewEntry(124, event.NewEventAt("test-event", "tx-1409", time.Now())),
			es.NewEntry(125, event.NewEventAt("test-event", "tx-1410", time.Now())),
			es.NewEntry(126, event.NewEventAt("test-event", "tx-1411", time.Now())),
			es.NewEntry(127, event.NewEventAt("test-event", "tx-1412", time.Now())),
			es.NewEntry(128, event.NewEventAt("test-event", "tx-1413", time.Now())),
			es.NewEntry(129, event.NewEventAt("test-event", "tx-1414", time.Now())),
		}); err != nil {
			t.Errorf("queue entries failed: %s", err)
		}

		batchTxID, entries := sut.ReceiveEntries("test-subscription")
		var eventIDs []string
		go func() {
			t.Helper()
			t.Logf("entered helper func")
			for _, ev := range entries {
				e := ev.Event()
				t.Logf("received event (globalPos,occuredAt,eventID) <%d,%s,%s,%s,%s>",
					ev.GlobalPos(), e.OccurredAt(), e.ID(), e.AggregateID(), e.Name())
				eventIDs = append(eventIDs, ev.Event().ID())
				done.Done()
			}
		}()
		done.Wait()

		assert.NoError(t, sut.AckEntries(batchTxID, eventIDs))
	})
}
