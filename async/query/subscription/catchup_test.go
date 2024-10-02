package subscription_test

import (
	"sync"
	"testing"
	"time"

	"github.com/openyard/evently/async/query/consume"
	"github.com/openyard/evently/async/query/subscription"
	"github.com/openyard/evently/event"
	"github.com/openyard/evently/pkg/volatile"
	"github.com/openyard/evently/tact/es"
)

func TestCatchUpSubscription_Listen(t *testing.T) {
	volatile.WithVolatileEventStore(func(_es *volatile.EventStore) {
		t.Helper()
		setup(_es, 123)

		var done sync.WaitGroup
		done.Add(1)
		testCheckpoint := subscription.NewCheckpoint("test-subscription", uint64(len(_es.Log())+1), time.Now())
		consume.Consume(func(events ...*event.Event) error {
			t.Helper()
			t.Logf("[TestCatchUpSubscription_Listen] handle <%d> event(s)", len(events))
			return nil
		})

		opts := []subscription.CatchUpOption{
			subscription.WithTicker(time.NewTicker(consume.SLAMicro)),
			subscription.WithAckFunc(func(entries ...*es.Entry) {
				testCheckpoint.Update(testCheckpoint.MaxGlobalPos(entries...))
				done.Done()
			}),
		}
		s := subscription.NewCatchUp("test-subscription", testCheckpoint.GlobalPosition(), _es, opts...)
		s.Listen()
		defer s.Stop()

		_ = _es.Append([]es.Change{
			es.NewChange("test-stream-1", 0, event.NewDomainEvent("test-event-1", "1")),
			es.NewChange("test-stream-2", 0, event.NewDomainEvent("test-event-2", "2")),
			es.NewChange("test-stream-1", 1, event.NewDomainEvent("test-event-3", "1")),
			es.NewChange("test-stream-2", 1, event.NewDomainEvent("test-event-4", "2")),
			es.NewChange("test-stream-3", 0, event.NewDomainEvent("test-event-5", "3")),
			es.NewChange("test-stream-2", 2, event.NewDomainEvent("test-event-6", "2")),
		}...)

		done.Wait()
		if 129 != testCheckpoint.GlobalPosition() {
			t.Errorf("[TestCatchUpSubscription_Listen] expected checkpoint@129, but is @%d", testCheckpoint.GlobalPosition())
		}
	})
}

func setup(_es es.EventStore, j int) {
	for i := 0; i <= j; i++ {
		_ = _es.Append(es.NewChange("test-stream-0", uint64(i), event.NewDomainEvent("test-event", "0")))
	}
}
