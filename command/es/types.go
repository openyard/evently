package es

import "github.com/openyard/evently/event"

// Change represents a single event incl. its version in a stream.
// This struct is used to append multiple changes for multiple streams at once to an event-store
type Change struct {
	version uint64
	event   *event.Event
}

// NewChange returns a new change with given (stream) version and event
func NewChange(version uint64, event *event.Event) Change {
	return Change{version: version, event: event}
}

// Version returns the version of the change, which represents the position in the stream
func (c *Change) Version() uint64 {
	return c.version
}

// Event returns the event to be appended to the stream
func (c *Change) Event() *event.Event {
	return c.event
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
