package subscription_test

import (
	"sync"
	"testing"
	"time"

	"github.com/openyard/evently/command/es"
	"github.com/openyard/evently/event"
	"github.com/openyard/evently/pkg/volatile"
	"github.com/openyard/evently/query/consume"
	"github.com/openyard/evently/query/subscription"
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
			subscription.WithTicker(time.NewTicker(subscription.SLAMicro)),
			subscription.WithAckFunc(func(entries ...*es.Entry) {
				testCheckpoint.Update(testCheckpoint.MaxGlobalPos(entries...))
				done.Done()
			}),
		}
		s := subscription.NewCatchUpSubscription("test-subscription", testCheckpoint.GlobalPosition(), _es, opts...)
		s.Listen()
		defer s.Stop()

		_ = _es.AppendToStream("test-stream-1", 0, event.NewDomainEvent("test-event-1", "1"))
		_ = _es.AppendToStream("test-stream-2", 0, event.NewDomainEvent("test-event-2", "2"))
		_ = _es.AppendToStream("test-stream-1", 1, event.NewDomainEvent("test-event-3", "1"))
		_ = _es.AppendToStream("test-stream-2", 1, event.NewDomainEvent("test-event-4", "2"))
		_ = _es.AppendToStream("test-stream-3", 0, event.NewDomainEvent("test-event-5", "3"))
		_ = _es.AppendToStream("test-stream-2", 2, event.NewDomainEvent("test-event-6", "2"))

		done.Wait()
		if 129 != testCheckpoint.GlobalPosition() {
			t.Errorf("[TestCatchUpSubscription_Listen] expected checkpoint@129, but is @%d", testCheckpoint.GlobalPosition())
		}
	})
}

func setup(_es es.EventStore, j int) {
	for i := 0; i <= j; i++ {
		_ = _es.AppendToStream("test-stream-0", uint64(i), event.NewDomainEvent("test-event", "0"))
	}
}
