package es

import (
	"time"

	"github.com/openyard/evently/event"
)

// EventStore interface provides methods to read and write event-streams
type EventStore interface {
	// Append adds the events to the assigned streams in one batch
	Append(changes ...Change) error
	// Read loads all requested streams and returns them with its events
	Read(streams ...string) ([]Stream, error)
	// ReadAt loads requested streams at a certain point in time and returns them with its events up to this point
	ReadAt(at time.Time, streams ...string) ([]Stream, error)
}

// EventHandler ...
type EventHandler event.HandleFunc

// Callback interface provides methods to register handler for new events and calling them immediately when they occur
// to synchronize the caller after async command execution
type Callback interface {
	// Register an event-handler with given ID. The ID could be passed within the command from the commandID to the
	// resulting event as eventID in order to tackle the right handler immediately.
	// Alternative registration can be done using the event-name as ID.
	Register(ID string, handle EventHandler)
	// Unregister the event-handler with the given ID
	Unregister(ID string)
}

// Transition is a func to apply the given ddd.DomainEvent
type Transition func(e *event.Event)

// Transport interface provides methods to receive new events asynchronously
type Transport interface {
	// Subscribe starts to listen for new events
	// You use this method most likely for volatile-subscriptions to receive only new events from now on
	Subscribe(limit uint16) chan []*Entry
	// SubscribeWithID fetches remaining events based on given ID and listen for new events
	// You use this method most likely for persistent-subscriptions if the underlying Transport (EventStore) supports it
	SubscribeWithID(ID string, limit uint16) chan []*Entry
	// SubscribeWithOffset fetches remaining events based on given offset and listen for new events
	// You use this method most likely for catchup-subscriptions where the subscriber keeps the offset
	SubscribeWithOffset(offset uint64, limit uint16) chan []*Entry
}
