package estest

import (
	"fmt"
	"sync"
	"time"

	"github.com/openyard/evently"
	"github.com/openyard/evently/command/es"
	"github.com/openyard/evently/event"
)

var _ es.EventStore = (*TestEventStore)(nil)

type TestEventStore struct {
	sync.RWMutex
	streams map[string][]*event.Event
	log     []*event.Event
	events  chan *event.Event
}

func WithTestEventStore(f func(es es.EventStore)) {
	es_ := NewTestEventStore()
	f(es_)
}

func NewTestEventStore() *TestEventStore {
	return &TestEventStore{
		streams: make(map[string][]*event.Event),
		log:     make([]*event.Event, 0),
		events:  make(chan *event.Event),
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
	defer func() {
		go func(_es *TestEventStore, events ...*event.Event) {
			_es.Lock()
			defer _es.Unlock()
			for _, e := range events {
				_es.log = append(_es.log, e)
				_es.events <- e
			}
		}(_es, events...)
	}()
	_es.Lock()
	defer _es.Unlock()
	history := _es.streams[name]
	if uint64(len(history)) != expectedVersion {
		return evently.Errorf(es.ErrConcurrentChange, "ErrConcurrentChange", name).
			CausedBy(fmt.Errorf("concurrent change detected - expectedVersion: %d, actualVersion: %d", expectedVersion, len(history)))
	}
	_es.streams[name] = append(history, events...)
	return nil
}

func (_es *TestEventStore) Subscribe() <-chan *event.Event {
	return _es.events
}

func (_es *TestEventStore) SubscribeWithOffset(offset uint64) <-chan *event.Event {
	_es.RLock()
	defer _es.RUnlock()
	for _, e := range _es.log[offset:] {
		_es.events <- e
	}
	return _es.Subscribe()
}
