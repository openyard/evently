package example

import (
	"encoding/json"
	"time"

	"github.com/openyard/evently/command/es"
	"github.com/openyard/evently/event"
	"github.com/openyard/evently/query/consume"
	"github.com/openyard/evently/query/subscription"
)

type CustomerProjection struct {
	customers  map[string]*APICustomer
	checkpoint *subscription.Checkpoint
}

type APICustomer struct {
	ID          string `json:"ID"`
	Name        string `json:"Name"`
	Birthdate   string `json:"Birthdate"`
	Sex         byte   `json:"Sex"`
	State       string `json:"State"`
	OnboardedAt string `json:"OnboardedAt"`
}

func NewCustomerProjection(eventStore es.Transport) *CustomerProjection {
	cp := &CustomerProjection{}
	consume.Consume(cp.handle)

	opts := []subscription.CatchUpOption{
		subscription.WithCheckpoint(cp.checkpoint),
		subscription.WithConsumer(&consume.DefaultConsumer),
		subscription.WithAckFunc(func(entries ...*es.Entry) {
			cp.checkpoint.Update(cp.checkpoint.MaxGlobalPos(entries...))
		}),
	}
	s := subscription.NewCatchUpSubscription(eventStore, opts...)
	defer s.Listen()
	return cp
}

func (cp *CustomerProjection) Handle(ctx *consume.Context, events ...*event.Event) error {
	return cp.handle(events...)
}

func (cp *CustomerProjection) handle(events ...*event.Event) error {
	for _, e := range events {
		switch e.Name() {
		case newCustomerOnboardedEventName:
			if err := cp.onCustomerOnboarded(e); err != nil {
				return err
			}
		case customerActivatedEventName:
			if err := cp.onCustomerActivated(e); err != nil {
				return err
			}
		case customerBlockedEventName:
			if err := cp.onCustomerBlocked(e); err != nil {
				return err
			}
		}
	}
	return nil
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
