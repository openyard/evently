package estest

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/openyard/evently"
	"github.com/openyard/evently/command/es"
	"github.com/openyard/evently/event"
)

var _ es.EventStore = (*TestEventStore)(nil)
var _ es.Transport = (*TestEventStore)(nil)

type TestEventStore struct {
	sync.RWMutex
	streams map[string][]*event.Event
	log     []*es.Entry
	entries chan []*es.Entry
}

func WithTestEventStore(f func(es es.EventStore)) {
	es_ := NewTestEventStore()
	f(es_)
}

func NewTestEventStore() *TestEventStore {
	return &TestEventStore{
		streams: make(map[string][]*event.Event),
		log:     make([]*es.Entry, 0),
		entries: make(chan []*es.Entry),
	}
}

func (_es *TestEventStore) ReadStream(name string) (es.History, error) {
	_es.RLock()
	defer _es.RUnlock()
	return _es.streams[name], nil
}

func (_es *TestEventStore) ReadStreamAt(name string, at time.Time) (es.History, error) {
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

func (_es *TestEventStore) AppendToStream(name string, expectedVersion uint64, events ...*event.Event) error {
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

func (_es *TestEventStore) Subscribe() <-chan []*es.Entry {
	offset := len(_es.log)
	_es.entries <- _es.log[offset:]
	return _es.entries
}

func (_es *TestEventStore) SubscribeWithOffset(offset uint64) <-chan []*es.Entry {
	_es.RLock()
	defer _es.RUnlock()
	log.Printf("len(log)=%d, offset=%d", len(_es.log), offset)
	entries := _es.log[offset:]
	log.Printf("_es.log[offset:]=%d", len(entries))
	go func() {
		if len(entries) > 0 {
			_es.entries <- entries
		}
	}()
	log.Printf("len(entries)=%d", len(_es.entries))
	return _es.entries
}
