package event

import (
	"encoding/json"
	"time"

	"github.com/openyard/evently/pkg/uuid"
)

const (
	DomainEvent      Type = "DomainEvent"
	IntegrationEvent      = "IntegrationEvent"
)

// Type ...
type Type string

// Option ...
type Option func(e *Event)

// Event represents either a DomainEvent or an IntegrationEvent. A DomainEvent is
// an event which lives inside the bounded context and belongs to the history of
// an aggregate. An IntegrationEvent is an external event which leaves the
// boundary of the domain context (in sense of domain-driven-design) and gets
// published to an external event-bus or event-/message-broker.
type Event struct {
	kind        Type
	id          string
	name        string
	aggregateID string
	payload     []byte
	occurredAt  time.Time
}

// NewDomainEvent initializes a domain event with given name and aggregateID
func NewDomainEvent(name, aggregateID string, opts ...Option) *Event {
	opts = append(opts, WithEventType(DomainEvent))
	return NewEventAt(name, aggregateID, time.Now().UTC(), opts...)
}

// NewIntegrationEvent initializes an integration event with given name and aggregateID
func NewIntegrationEvent(name, aggregateID string, opts ...Option) *Event {
	opts = append(opts, WithEventType(IntegrationEvent))
	return NewEventAt(name, aggregateID, time.Now().UTC(), opts...)
}

// NewEventAt initializes an event with given name and aggregateID
// plus provided timestamp when occurred
func NewEventAt(name, aggregateID string, occurredAt time.Time, opts ...Option) *Event {
	e := &Event{
		name:        name,
		id:          uuid.NewV4().String(),
		aggregateID: aggregateID,
		occurredAt:  occurredAt.UTC(),
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

func WithID(ID string) Option {
	return func(e *Event) {
		e.id = ID
	}
}

func WithPayload(payload []byte) Option {
	return func(e *Event) {
		e.payload = payload
	}
}

func WithEventType(kind Type) Option {
	return func(e *Event) {
		e.kind = kind
	}
}

// Kind ...
func (e *Event) Kind() Type {
	return e.kind
}

// ID ...
func (e *Event) ID() string {
	return e.id
}

// Name ...
func (e *Event) Name() string {
	return e.name
}

// AggregateID ...
func (e *Event) AggregateID() string {
	return e.aggregateID
}

// Payload ...
func (e *Event) Payload() []byte {
	return e.payload
}

// OccurredAt ...
func (e *Event) OccurredAt() time.Time {
	return e.occurredAt
}

// MarshalJSON is implementation of json.Marshaler
func (e *Event) MarshalJSON() ([]byte, error) {
	v := map[string]any{
		"Kind":        e.kind,
		"Name":        e.name,
		"ID":          e.id,
		"AggregateID": e.aggregateID,
		"Payload":     e.payload,
		"OccurredAt":  e.occurredAt,
	}
	return json.MarshalIndent(v, "", "  ")
}

// UnmarshalJSON is implementation of json.Unmarshaler
func (e *Event) UnmarshalJSON(data []byte) error {
	var v map[string]any
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	switch v["Kind"].(string) {
	case "IntegrationEvent":
		e.kind = IntegrationEvent
	case "DomainEvent":
		fallthrough
	default:
		e.kind = DomainEvent
	}
	e.name = v["Name"].(string)
	e.id = v["ID"].(string)
	e.aggregateID = v["AggregateID"].(string)
	if v["Payload"] != nil {
		e.payload = []byte(v["Payload"].(string))
	}
	e.occurredAt, _ = time.Parse(time.RFC3339Nano, v["OccurredAt"].(string))
	return nil
}
