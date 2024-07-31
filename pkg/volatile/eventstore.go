package volatile

import (
	"fmt"
	"sync"
	"time"

	"github.com/openyard/evently/command/es"
	"github.com/openyard/evently/event"
	"github.com/openyard/evently/pkg/evently"
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

func (_es *EventStore) AppendToStream(name string, expectedVersion uint64, events ...*event.Event) error {
	_es.Lock()
	defer _es.Unlock()
	history := _es.streams[name]
	if uint64(len(history)) != expectedVersion {
		return evently.Errorf(es.ErrConcurrentChange, "ErrConcurrentChange", name).
			CausedBy(fmt.Errorf("concurrent change detected - expectedVersion: %d, actualVersion: %d", expectedVersion, len(history)))
	}
	_es.streams[name] = append(history, events...)
	for _, e := range events {
		_es.log = append(_es.log, es.NewEntry(uint64(len(_es.log)), e))
	}
	return nil
}

func (_es *EventStore) Log() []*es.Entry {
	return _es.log
}

func (_es *EventStore) ReadStream(name string) (es.History, error) {
	_es.RLock()
	defer _es.RUnlock()
	return _es.streams[name], nil
}

func (_es *EventStore) ReadStreamAt(name string, at time.Time) (es.History, error) {
	_es.RLock()
	defer _es.RUnlock()
	hist := make(es.History, 0)
	for _, e := range _es.streams[name] {
		if e.OccurredAt().Before(at) {
			hist = append(hist, e)
		}
	}
	return hist, nil
}

func (_es *EventStore) Subscribe(limit uint16) chan []*es.Entry {
	return _es.SubscribeWithOffset(defaultOffset, limit)
}

func (_es *EventStore) SubscribeWithOffset(offset uint64, limit uint16) chan []*es.Entry {
	entries := make(chan []*es.Entry)
	go _es.receiveEntries(offset, limit, entries)
	return entries
}

func (_es *EventStore) SubscribeWithID(_ string, _ uint16) chan []*es.Entry {
	panic(fmt.Sprintf("[%T][FATAL] subscription with ID not supported", _es))
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
