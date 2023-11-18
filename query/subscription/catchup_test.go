package subscription_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/openyard/evently/command/es"
	"github.com/openyard/evently/command/es/estest"
	"github.com/openyard/evently/event"
	"github.com/openyard/evently/query/consume"
	"github.com/openyard/evently/query/subscription"
)

func TestCatchUpSubscription_Listen(t *testing.T) {
	estest.WithTestEventStore(func(_es es.EventStore) {
		testCheckpoint := subscription.NewCheckpoint("test-checkpoint", 123, time.Now())
		setup(_es, 123)
		t.Log("[TestCatchUpSubscription_Listen] setup _es done")
		opts := []subscription.CatchUpOption{
			subscription.WithCheckpoint(testCheckpoint),
			subscription.WithAckFunc(func(entries ...*es.Entry) {
				pos := testCheckpoint.MaxGlobalPos(entries...)
				t.Logf("[TestCatchUpSubscription_Listen] update checkpoint to <%d>", pos)
				testCheckpoint.Update(pos)
			}),
		}
		s := subscription.NewCatchUpSubscription(_es.(es.Transport), opts...)

		handled := make(chan string, 6)
		firstHandler := handleFunc("handler-a", t, handled)
		secondHandler := handleFunc("handler-b", t, handled)
		thirdHandler := handleFunc("handler-c", t, handled)

		consume.Consume(firstHandler, secondHandler, thirdHandler)
		s.Listen()
		defer s.Stop()
		t.Log("[TestCatchUpSubscription_Listen] listening...")

		_ = _es.AppendToStream("test-stream-1", 0, event.NewDomainEvent("test-event", "1"))
		_ = _es.AppendToStream("test-stream-2", 0, event.NewDomainEvent("test-event", "2"))
		_ = _es.AppendToStream("test-stream-1", 1, event.NewDomainEvent("test-event", "1"))
		_ = _es.AppendToStream("test-stream-2", 1, event.NewDomainEvent("test-event", "2"))
		_ = _es.AppendToStream("test-stream-3", 0, event.NewDomainEvent("test-event", "3"))
		_ = _es.AppendToStream("test-stream-2", 2, event.NewDomainEvent("test-event", "2"))
		t.Log("[TestCatchUpSubscription_Listen] events appended to streams")

		for i := 0; i < 18; i++ { // 3 handler * 6 events
			s := <-handled
			t.Logf("[TestCatchUpSubscription_Listen] %s", s)
		}

		t.Logf("[TestCatchUpSubscription_Listen] new globalPos = %d", testCheckpoint.GlobalPosition())

		if 129 != testCheckpoint.GlobalPosition() {
			t.Errorf("[TestCatchUpSubscription_Listen] expected checkpoint@129, but is @%d", testCheckpoint.GlobalPosition())
		}
	})
}

func handleFunc(id string, t *testing.T, handled chan string) func(event ...*event.Event) error {
	return func(event ...*event.Event) error {
		for _, e := range event {
			s := fmt.Sprintf("[TEST] handle event <%s> (%s/%s) by handler <%s>", e.ID(), e.AggregateID(), e.Name(), id)
			t.Log(s)
			handled <- s
		}
		return nil
	}
}

func setup(_es es.EventStore, j int) {
	for i := 0; i < j; i++ {
		_ = _es.AppendToStream("test-stream-0", uint64(i), event.NewDomainEvent("test-event", "0"))
	}
}
