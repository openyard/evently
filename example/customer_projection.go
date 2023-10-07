package example

import (
	"encoding/json"
	"log"
	"sync/atomic"
	"time"

	"github.com/openyard/evently/command/es"
	"github.com/openyard/evently/event"
	"github.com/openyard/evently/query/subscription"
)

type CustomerProjection struct {
	subscription *subscription.CatchUpSubscription
	customers    map[string]*APICustomer
	checkpoint   uint64
}

type APICustomer struct {
	ID          string `json:"ID"`
	Name        string `json:"Name"`
	Birthdate   string `json:"Birthdate"`
	Sex         byte   `json:"Sex"`
	State       string `json:"State"`
	OnboardedAt string `json:"OnboardedAt"`
}

func NewCustomerProjection(eventStore es.EventStore) *CustomerProjection {
	cp := &CustomerProjection{}
	opts := []subscription.CatchUpOption{
		subscription.WithLogger(log.Default()),
		subscription.WithOffset(cp.checkpoint),
		subscription.WithOnHandledFunc(func(event *event.Event) {
			atomic.AddUint64(&cp.checkpoint, 1)
		}),
	}
	s := subscription.NewCatchUpSubscription(eventStore, opts...)
	defer s.Listen()
	s.Subscribe(newCustomerOnboardedEventName, cp.onCustomerOnboarded)
	s.Subscribe(customerActivatedEventName, cp.onCustomerActivated)
	s.Subscribe(customerBlockedEventName, cp.onCustomerBlocked)
	cp.subscription = s
	return cp
}

func (cp *CustomerProjection) onCustomerOnboarded(event *event.Event) error {
	var e NewCustomerOnboardedEvent
	_ = json.Unmarshal(event.Payload(), &e)
	c := &APICustomer{
		ID:          event.AggregateID(),
		Name:        e.name,
		Birthdate:   e.birthdate.Format("2006-01-02"),
		Sex:         e.sex,
		State:       string(CustomerStateOnboarded),
		OnboardedAt: event.OccurredAt().Format(time.RFC3339),
	}
	cp.customers[event.AggregateID()] = c
	return nil
}

func (cp *CustomerProjection) onCustomerActivated(event *event.Event) error {
	cp.customers[event.AggregateID()].State = CustomerStateActive
	return nil
}

func (cp *CustomerProjection) onCustomerBlocked(event *event.Event) error {
	cp.customers[event.AggregateID()].State = CustomerStateBlocked
	return nil
}
