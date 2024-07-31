package es

import (
	"time"

	"github.com/openyard/evently/event"
)

// EventStore interface provides methods to read and write event-streams
type EventStore interface {
	// ReadStream loads the stream and returns all its events
	ReadStream(stream string) (History, error)
	// ReadStreamAt loads the stream at a certain point in time and returns all its events up to this point
	ReadStreamAt(stream string, at time.Time) (History, error)
	// AppendToStream adds the events to the stream
	AppendToStream(stream string, expectedVersion uint64, events ...*event.Event) error
}

// Transport interface provides methods to receive new events
type Transport interface {
	// Subscribe starts to listen for new events
	// You use this method most likely for volatile-subscriptions to receive only new events from now on
	Subscribe(limit uint16) chan []*Entry
	// SubscribeWithOffset fetches remaining events based on given offset and listen for new events
	// You use this method most likely for catchup-subscriptions where the subscriber keeps the offset
	SubscribeWithOffset(offset uint64, limit uint16) chan []*Entry
	// SubscribeWithID fetches remaining events based on given ID and listen for new events
	// You use this method most likely for persistent-subscriptions if the underlying Transport (EventStore) supports it
	SubscribeWithID(ID string, limit uint16) chan []*Entry
}

// BatchEventStore appends and reads events for multiple streams at once in batch mode
type BatchEventStore interface {
	// ReadStreams loads multiple stream and returns all streams and its events as map
	ReadStreams(stream []string) (map[string]History, error)
	// AppendToStreams adds the events to the assigned streams in one batch
	AppendToStreams(changes map[string][]Change) error
}
