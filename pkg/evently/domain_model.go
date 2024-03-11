package evently

import (
	"log"
	"sync"
	"time"

	"github.com/openyard/evently/command"
	"github.com/openyard/evently/command/es"
	"github.com/openyard/evently/event"
	"github.com/openyard/evently/pkg/timeutil"
)

// Transition is a func to apply the given ddd.DomainEvent
type Transition func(e *event.Event)

type DomainModel struct {
	sync.Mutex
	id, name string
	version  uint64
	history  *timeutil.TemporalCollection

	changes         []*event.Event
	commandHandlers map[string]command.HandleFunc
	transitions     map[string]Transition
}

// Init an event-sourced domain model with the given map of transitions and command-handlers
func (dm *DomainModel) Init(name string, transitions map[string]Transition, commandHandlers map[string]command.HandleFunc) {
	dm.Lock()
	defer dm.Unlock()
	dm.history = timeutil.NewTemporalCollection()
	dm.name = name
	dm.transitions = transitions
	dm.commandHandlers = commandHandlers
}

func (dm *DomainModel) EntityID() string {
	return dm.id
}

func (dm *DomainModel) AggregateName() string {
	return dm.name
}

func (dm *DomainModel) History() *timeutil.TemporalCollection {
	return dm.history
}

func (dm *DomainModel) StateAt(at time.Time) (*DomainModel, error) {
	model, err := dm.history.Get(at)
	return model.(*DomainModel), err
}

func (dm *DomainModel) Causes(events ...*event.Event) {
	dm.changes = append(dm.changes, events...)
	dm.apply(events...)
}

func (dm *DomainModel) Changes() []*event.Event {
	return dm.changes
}

func (dm *DomainModel) Load(history es.History) {
	dm.apply(history...)
}

func (dm *DomainModel) Execute(c *command.Command) ([]*event.Event, error) {
	ch, ok := dm.commandHandlers[c.CommandName()]
	if !ok {
		return nil, Errorf(command.ErrUnknownCommand, "ErrUnknownCommand", "[%T] unknown command: %q", dm, c.CommandName())
	}
	err := ch.Handle(c)
	return dm.changes, err
}

func (dm *DomainModel) Version() uint64 {
	return dm.version
}

func (dm *DomainModel) apply(events ...*event.Event) {
	defer dm.history.Put(events[len(events)-1].OccurredAt(), dm) // save state at point in time
	for _, e := range events {
		dm.version++
		eh, known := dm.transitions[e.Name()]
		if !known {
			log.Printf("[%T] unhandled event %q", dm, e.Name())
			continue
		}
		eh(e)
	}
}
