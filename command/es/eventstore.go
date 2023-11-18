package es

import (
	"time"

	"github.com/openyard/evently/event"
)

type Entry struct {
	GlobalPos uint64
	Event     *event.Event
}

func NewEntry(globalPos uint64, event *event.Event) *Entry {
	return &Entry{
		GlobalPos: globalPos,
		Event:     event,
	}
}

// History is a slice of event.Event
type History []*event.Event

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
	Subscribe() <-chan []*Entry
	// SubscribeWithOffset fetches remaining events based on given offset and listen for new events
	SubscribeWithOffset(offset uint64) <-chan []*Entry
}

// MultiEventStore appends events for multiple streams at once
type MultiEventStore interface {
	// AppendMulti adds the events to the assigned streams in a batch
	AppendMulti(events map[string]map[uint64][]*event.Event) error
}
