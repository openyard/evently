package example

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/openyard/evently/event"
)

const (
	newCustomerOnboardedEventName = "Customer/v1.newCustomerOnboarded" // domain/version.name
	customerActivatedEventName    = "Customer/v1.customerActivated"
	customerBlockedEventName      = "Customer/v1.customerBlocked"
)

type NewCustomerOnboardedEvent struct {
	// immutable
	name      string
	birthdate time.Time
	sex       byte
}

func NewCustomerOnboarded(aggregateID, name string, sex byte, birthdate time.Time) *event.Event {
	return NewCustomerOnboardedAt(aggregateID, name, sex, birthdate, time.Now().UTC())
}

// NewCustomerOnboardedAt is for testing purposes to pass the time the event happened from outside (the test)
func NewCustomerOnboardedAt(aggregateID, name string, sex byte, birthdate, at time.Time, opts ...event.Option) *event.Event {
	payload, _ := json.Marshal(&NewCustomerOnboardedEvent{name, birthdate, sex})
	opts = append(opts, event.WithPayload(payload))
	return event.NewEventAt(newCustomerOnboardedEventName, aggregateID, at, opts...)
}

type CustomerActivatedEvent struct{}

func CustomerActivated(aggregateID string) *event.Event {
	return event.NewDomainEvent(customerActivatedEventName, aggregateID)
}

type CustomerBlockedEvent struct {
	reason string // optional
}

func CustomerBlocked(aggregateID string, reason ...string) *event.Event {
	if len(reason) > 0 {
		payload, _ := json.Marshal(&CustomerBlockedEvent{reason: strings.Join(reason, ", ")})
		return event.NewDomainEvent(customerBlockedEventName, aggregateID, event.WithPayload(payload))
	}
	return event.NewDomainEvent(customerBlockedEventName, aggregateID)
}
