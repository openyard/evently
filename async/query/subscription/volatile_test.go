package subscription_test

import (
	"log"
	"os"
	"testing"
	"time"

	"github.com/openyard/evently/async/query/consume"
	"github.com/openyard/evently/async/query/subscription"
	"github.com/openyard/evently/event"
	"github.com/openyard/evently/pkg/volatile"
	"github.com/openyard/evently/tact/es"
)

func TestVolatile(t *testing.T) {
	_ = os.Setenv("DEBUG", "true")
	_ = os.Setenv("TRACE", "true")
	defer func() {
		_ = os.Unsetenv("DEBUG")
		_ = os.Unsetenv("TRACE")
	}()

	syncChan := make(chan *event.Event)
	eventStore := volatile.NewEventStore()
	ticker := time.NewTicker(consume.SLAMicro) // 10ms

	sut := subscription.NewVolatile("test-subscription", eventStore, subscription.WithVolatileTicker(ticker), subscription.WithVolatileConsumer(
		consume.NewConsumer(func(events ...*event.Event) error {
			for _, e := range events {
				syncChan <- e
			}
			return nil
		})))
	sut.Listen()
	defer sut.Stop()

	_ = eventStore.Append([]es.Change{es.NewChange("test-stream", 0, []*event.Event{
		event.NewDomainEvent("test-event", "1", event.WithID("1")),
		event.NewDomainEvent("test-event", "2", event.WithID("2")),
	}...)}...)

	log.Println(<-syncChan)
	log.Println(<-syncChan)

	log.Println("test finished")
}
