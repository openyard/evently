package domain

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/openyard/evently/event"
)

const (
	newCustomerOnboardedEventName = "customer/v1.newCustomerOnboarded" // domain/version.name
	customerActivatedEventName    = "customer/v1.customerActivated"
	customerBlockedEventName      = "customer/v1.customerBlocked"
)

type OnboardedEvent struct {
	Name      string
	Birthdate time.Time
	Sex       byte
}

func onboarded(aggregateID, name string, sex byte, birthdate time.Time) *event.Event {
	return onboardedAt(aggregateID, name, sex, birthdate, time.Now().UTC())
}

// onboardedAt is for testing purposes to pass the time the event happened from outside (the test)
func onboardedAt(aggregateID, name string, sex byte, birthdate, at time.Time, opts ...event.Option) *event.Event {
	payload, _ := json.Marshal(&OnboardedEvent{name, birthdate, sex})
	opts = append(opts, event.WithPayload(payload))
	return event.NewEventAt(newCustomerOnboardedEventName, aggregateID, at, opts...)
}

type ActivatedEvent struct{}

func activated(aggregateID string) *event.Event {
	return event.NewDomainEvent(customerActivatedEventName, aggregateID)
}

type BlockedEvent struct {
	Reason string // optional
}

func blocked(aggregateID string, reason ...string) *event.Event {
	if len(reason) > 0 {
		payload, _ := json.Marshal(&BlockedEvent{Reason: strings.Join(reason, ", ")})
		return event.NewDomainEvent(customerBlockedEventName, aggregateID, event.WithPayload(payload))
	}
	return event.NewDomainEvent(customerBlockedEventName, aggregateID)
}
