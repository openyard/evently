package cmd

import (
	"log"
	"sync"
	"time"

	"github.com/openyard/evently/event"
	"github.com/openyard/evently/pkg/evently"
	"github.com/openyard/evently/pkg/timeutil"
	"github.com/openyard/evently/tact/es"
)

type DomainModel struct {
	sync.Mutex
	id, name string
	version  uint64
	history  *timeutil.TemporalCollection

	changes         []*event.Event
	commandHandlers map[string]HandleFunc
	transitions     map[string]es.Transition
}

// Init an event-sourced domain model with the given map of transitions and command-handlers
func (m *DomainModel) Init(name string, transitions map[string]es.Transition, commandHandlers map[string]HandleFunc) {
	m.Lock()
	defer m.Unlock()
	m.history = timeutil.NewTemporalCollection()
	m.name = name
	m.transitions = transitions
	m.commandHandlers = commandHandlers
}

func (m *DomainModel) EntityID() string {
	return m.id
}

func (m *DomainModel) AggregateName() string {
	return m.name
}

func (m *DomainModel) History() *timeutil.TemporalCollection {
	return m.history
}

func (m *DomainModel) StateAt(at time.Time) (*DomainModel, error) {
	model, err := m.history.Get(at)
	return model.(*DomainModel), err
}

func (m *DomainModel) Causes(events ...*event.Event) {
	m.changes = append(m.changes, events...)
	m.apply(events...)
}

func (m *DomainModel) Changes() []*event.Event {
	return m.changes
}

func (m *DomainModel) Load(history es.History) {
	m.apply(history...)
}

func (m *DomainModel) Execute(c *Command) ([]*event.Event, error) {
	ch, ok := m.commandHandlers[c.CommandName()]
	if !ok {
		return nil, evently.Errorf(ErrUnknownCommand, "ErrUnknownCommand", "[%T] unknown command: %q", m, c.CommandName())
	}
	err := ch.Handle(c)
	return m.changes, err
}

func (m *DomainModel) Version() uint64 {
	return m.version
}

func (m *DomainModel) apply(events ...*event.Event) {
	if len(events) == 0 {
		return
	}
	defer m.history.Put(events[len(events)-1].OccurredAt(), m) // save state at point in time
	for _, e := range events {
		m.version++
		eh, known := m.transitions[e.Name()]
		if !known {
			log.Printf("[%T] unhandled event %q", m, e.Name())
			continue
		}
		eh(e)
	}
}
