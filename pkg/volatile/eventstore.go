package volatile

import (
	"fmt"
	"sync"
	"time"

	"github.com/openyard/evently/event"
	"github.com/openyard/evently/pkg/evently"
	"github.com/openyard/evently/tact/es"
)

const defaultOffset = 0

var (
	_ es.EventStore = (*EventStore)(nil)
	_ es.Transport  = (*EventStore)(nil)
)

// EventStore is a volatile es.EventStore and es.Transport which can be used with single instance purpose.
// The most common use case is to use this EventStore for unit tests or local test scenarios of an event sourced
// application.
type EventStore struct {
	sync.RWMutex
	streams map[string][]*event.Event
	log     []*es.Entry
}

// WithVolatileEventStore creates an in-memory event-store and executes the given func
func WithVolatileEventStore(f func(es *EventStore)) {
	es_ := NewEventStore()
	f(es_)
}

// NewEventStore returns an initialized in-memory event-store with empty log and no streams yet
func NewEventStore() *EventStore {
	return &EventStore{
		streams: make(map[string][]*event.Event),
		log:     make([]*es.Entry, 0),
	}
}

func (_es *EventStore) Append(changes ...es.Change) error {
	_es.Lock()
	defer _es.Unlock()
	for _, c := range changes {
		history := _es.streams[c.Stream()]
		if uint64(len(history)) != c.ExpectedVersion() {
			return evently.Errorf(es.ErrConcurrentChange, "ErrConcurrentChange", c.Stream()).
				CausedBy(fmt.Errorf("concurrent change detected - expectedVersion: %d, actualVersion: %d", c.ExpectedVersion(), len(history)))
		}
		_es.streams[c.Stream()] = append(history, c.Events()...)
		for _, e := range c.Events() {
			_es.log = append(_es.log, es.NewEntry(uint64(len(_es.log)), e))
		}
	}
	return nil
}

func (_es *EventStore) Read(streams ...string) ([]es.Stream, error) {
	_es.RLock()
	defer _es.RUnlock()
	result := make([]es.Stream, 0)
	for _, name := range streams {
		if events, ok := _es.streams[name]; ok {
			result = append(result, es.BuildStream(name, events...))
		}
	}
	return result, nil
}

func (_es *EventStore) ReadAt(at time.Time, streams ...string) ([]es.Stream, error) {
	_es.RLock()
	defer _es.RUnlock()
	result := make([]es.Stream, 0)
	for _, name := range streams {
		if events, ok := _es.streams[name]; ok {
			var elems []*event.Event
			for _, e := range events {
				if e.OccurredAt().Before(at) {
					elems = append(elems, e)
				}
			}
			result = append(result, es.BuildStream(name, elems...))
		}
	}
	return result, nil
}

func (_es *EventStore) Log() []*es.Entry {
	return _es.log
}

func (_es *EventStore) Subscribe(limit uint16) chan []*es.Entry {
	currentPos := uint64(len(_es.log))
	entries := make(chan []*es.Entry)
	go _es.receiveEntries(currentPos, limit, entries)
	return entries
}

func (_es *EventStore) SubscribeWithOffset(offset uint64, limit uint16) chan []*es.Entry {
	entries := make(chan []*es.Entry)
	go _es.receiveEntries(offset, limit, entries)
	return entries
}

func (_es *EventStore) SubscribeWithID(_ string, _ uint16) chan []*es.Entry {
	panic(fmt.Sprintf("[FATAL][%T] subscription with ID not supported", _es))
}

func (_es *EventStore) receiveEntries(offset uint64, limit uint16, next chan []*es.Entry) {
	defer close(next)
	_es.sync(func() {
		count := len(_es.log)
		if offset >= uint64(count) {
			next <- nil
			return
		}
		if limit > uint16(count) {
			limit = uint16(count)
		}
		next <- _es.log[offset:len(_es.log):limit]
	})
}

func (_es *EventStore) receiveChunkEntries(offset uint64, limit uint16, entries chan []*es.Entry) {
	defer close(entries)
	_es.sync(func() {
		count := uint64(len(_es.log))
		if offset >= count {
			entries <- make([]*es.Entry, 0)
			return
		}
		end := offset + uint64(limit)
		if limit == 0 {
			end = uint64(len(_es.log))
		}
		if end > count {
			end = count
		}
		entries <- _es.log[offset:end]
	})
}

func (_es *EventStore) sync(f func()) {
	_es.RLock()
	defer _es.RUnlock()
	f()
}
