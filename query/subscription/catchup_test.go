package subscription_test

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openyard/evently/command/es"
	"github.com/openyard/evently/event"
	"github.com/openyard/evently/pkg/volatile"
	"github.com/openyard/evently/query/consume"
	"github.com/openyard/evently/query/subscription"
)

func TestCatchUpSubscription_Listen(t *testing.T) {
	t.Logf("START-----------------------------------------------------------------------------------------")
	defer t.Logf("END-----------------------------------------------------------------------------------------\n")
	volatile.WithInMemoryEventStore(func(_es es.EventStore) {
		testCheckpoint := subscription.NewCheckpoint("test-checkpoint", 123, time.Now())
		setup(_es, 123)
		t.Logf("[TestCatchUpSubscription_Listen] setup _es done - len(log)=%d", len(_es.(*volatile.InMemoryEventStore).Log()))
		opts := []subscription.CatchUpOption{
			subscription.WithCheckpoint(testCheckpoint),
			subscription.WithAckFunc(func(entries ...*es.Entry) {
				testCheckpoint.Update(testCheckpoint.MaxGlobalPos(entries...))
			}),
		}
		s := subscription.NewCatchUpSubscription(_es.(es.Transport), opts...)

		var count atomic.Int32
		var wg sync.WaitGroup
		handleChan := make(chan string, 18)
		wg.Add(18)
		firstHandler := handleFunc("handler-a", t, &wg, handleChan, &count)
		secondHandler := handleFunc("handler-b", t, &wg, handleChan, &count)
		thirdHandler := handleFunc("handler-c", t, &wg, handleChan, &count)

		consume.Consume(firstHandler, secondHandler, thirdHandler)
		s.Listen()
		defer s.Stop()
		t.Log("[TestCatchUpSubscription_Listen] listening...")

		_ = _es.AppendToStream("test-stream-1", 0, event.NewDomainEvent("test-event-1", "1"))
		_ = _es.AppendToStream("test-stream-2", 0, event.NewDomainEvent("test-event-2", "2"))
		_ = _es.AppendToStream("test-stream-1", 1, event.NewDomainEvent("test-event-3", "1"))
		_ = _es.AppendToStream("test-stream-2", 1, event.NewDomainEvent("test-event-4", "2"))
		_ = _es.AppendToStream("test-stream-3", 0, event.NewDomainEvent("test-event-5", "3"))
		_ = _es.AppendToStream("test-stream-2", 2, event.NewDomainEvent("test-event-6", "2"))
		t.Log("[TestCatchUpSubscription_Listen] events appended to streams")

		wg.Wait()
		t.Logf("count=%d", count.Load())

		t.Logf("[TestCatchUpSubscription_Listen] new globalPos = %d", testCheckpoint.GlobalPosition())
		if 129 != testCheckpoint.GlobalPosition() {
			t.Errorf("[TestCatchUpSubscription_Listen] expected checkpoint@129, but is @%d", testCheckpoint.GlobalPosition())
		}
	})
}

func handleFunc(id string, t *testing.T, wg *sync.WaitGroup, hc chan string, count *atomic.Int32) func(event ...*event.Event) error {
	return func(event ...*event.Event) error {
		for _, e := range event {
			s := fmt.Sprintf("[TEST] handle event <%s> (%s/%s) by handler <%s>", e.ID(), e.AggregateID(), e.Name(), id)
			count.Store(count.Add(1))
			//t.Log(s)
			hc <- fmt.Sprintf("count: %d, %s", count.Load(), s)
			wg.Done()
		}
		return nil
	}
}

func setup(_es es.EventStore, j int) {
	for i := 0; i < j; i++ {
		_ = _es.AppendToStream("test-stream-0", uint64(i), event.NewDomainEvent("test-event", "0"))
	}
}
