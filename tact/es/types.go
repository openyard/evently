package es

import (
	"github.com/openyard/evently/event"
)

// Change represents a single change incl. its expectedVersion in a stream.
// This struct is used to append multiple changes for multiple streams at once to an event-store
type Change struct {
	stream          string
	expectedVersion uint64
	events          []*event.Event
}

// NewChange returns a new change with given stream, expectedVersion and events to append
func NewChange(stream string, expectedVersion uint64, events ...*event.Event) Change {
	return Change{
		stream:          stream,
		expectedVersion: expectedVersion,
		events:          events,
	}
}

// Stream returns the stream name the change is for
func (c *Change) Stream() string {
	return c.stream
}

// ExpectedVersion returns the expected version of the change, which represents the position in the stream
func (c *Change) ExpectedVersion() uint64 {
	return c.expectedVersion
}

// Events returns the events to be appended to the stream
func (c *Change) Events() []*event.Event {
	return c.events
}

// Entry represents an entry with index (global position) in the event-store
type Entry struct {
	globalPos uint64
	event     *event.Event
}

// NewEntry returns an Entry with given global position and event
func NewEntry(globalPos uint64, event *event.Event) *Entry {
	return &Entry{globalPos: globalPos, event: event}
}

// GlobalPos returns the global position of this entry
func (e *Entry) GlobalPos() uint64 {
	return e.globalPos
}

// Event returns the event of this entry
func (e *Entry) Event() *event.Event {
	return e.event
}

// History is a slice of event.Event
type History []*event.Event

type Stream struct {
	name    string
	version uint64
	events  []*event.Event
}

func BuildStream(name string, events ...*event.Event) Stream {
	return Stream{
		name:    name,
		version: uint64(len(events)),
		events:  events,
	}
}

func (s Stream) Name() string {
	return s.name
}

func (s Stream) Version() uint64 {
	return s.version
}

func (s Stream) Events() []*event.Event {
	return s.events
}
