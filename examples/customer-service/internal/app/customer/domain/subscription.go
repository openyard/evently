package domain

import (
	"customer/internal/app/customer/report"
	"encoding/json"
	"time"

	"github.com/openyard/evently/command/es"
	"github.com/openyard/evently/event"
	"github.com/openyard/evently/query/consume"
	"github.com/openyard/evently/query/subscription"
)

type ReportingStore interface {
	StoreCustomer(customer *report.Customer) error
	SetCustomerState(ID, state string) error
}

type consumer struct {
	reportingStore ReportingStore
}

func SubscribeCustomerEvents(
	checkpoint *subscription.Checkpoint,
	reportingStore ReportingStore,
	transport es.Transport) *subscription.CatchUpSubscription {

	p := &consumer{reportingStore: reportingStore}
	consume.Consume(p.Handle) // register handle at defaultConsumer

	opts := []subscription.CatchUpOption{
		subscription.WithCheckpoint(checkpoint),
		subscription.WithAckFunc(func(entries ...*es.Entry) {
			checkpoint.Update(checkpoint.MaxGlobalPos(entries...) + 1)
		}),
	}
	s := subscription.NewCatchUpSubscription(transport, opts...)
	defer s.Listen()
	return s
}

func (c *consumer) Handle(events ...*event.Event) error {
	for _, e := range events {
		switch e.Name() {
		case newCustomerOnboardedEventName:
			if err := c.onCustomerOnboarded(e); err != nil {
				return err
			}
		case customerActivatedEventName:
			if err := c.onCustomerActivated(e); err != nil {
				return err
			}
		case customerBlockedEventName:
			if err := c.onCustomerBlocked(e); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *consumer) onCustomerOnboarded(event *event.Event) error {
	var e OnboardedEvent
	_ = json.Unmarshal(event.Payload(), &e)
	customer := &report.Customer{
		ID:          event.AggregateID(),
		Name:        e.Name,
		Birthdate:   e.Birthdate.Format("2006-01-02"),
		Sex:         e.Sex,
		State:       string(StateOnboarded),
		OnboardedAt: event.OccurredAt().Format(time.RFC3339),
	}
	return c.reportingStore.StoreCustomer(customer)
}

func (c *consumer) onCustomerActivated(event *event.Event) error {
	return c.reportingStore.SetCustomerState(event.AggregateID(), StateActive)
}

func (c *consumer) onCustomerBlocked(event *event.Event) error {
	return c.reportingStore.SetCustomerState(event.AggregateID(), StateBlocked)
}
