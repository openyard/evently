package domain

import (
	"encoding/json"
	"time"

	"customer/internal/app/customer/reporting"

	"github.com/openyard/evently/event"
)

type allCustomersProjection struct {
	reportingStore ReportingStore
}

func (p *allCustomersProjection) Handle(events ...*event.Event) error {
	for _, e := range events {
		switch e.Name() {
		case newCustomerOnboardedEventName:
			if err := p.onCustomerOnboarded(e); err != nil {
				return err
			}
		case customerActivatedEventName:
			if err := p.onCustomerActivated(e); err != nil {
				return err
			}
		case customerBlockedEventName:
			if err := p.onCustomerBlocked(e); err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *allCustomersProjection) onCustomerOnboarded(event *event.Event) error {
	var e OnboardedEvent
	_ = json.Unmarshal(event.Payload(), &e)
	customer := &reporting.Customer{
		ID:          event.AggregateID(),
		Name:        e.Name,
		Birthdate:   e.Birthdate.Format("2006-01-02"),
		Sex:         string(e.Sex),
		State:       string(StateOnboarded),
		OnboardedAt: event.OccurredAt().Format(time.RFC3339),
	}
	return p.reportingStore.StoreCustomer(customer)
}

func (p *allCustomersProjection) onCustomerActivated(event *event.Event) error {
	return p.reportingStore.SetCustomerState(event.AggregateID(), StateActive)
}

func (p *allCustomersProjection) onCustomerBlocked(event *event.Event) error {
	return p.reportingStore.SetCustomerState(event.AggregateID(), StateBlocked)
}

type ReportingStore interface {
	StoreCustomer(customer *reporting.Customer) error
	SetCustomerState(ID, state string) error
}
