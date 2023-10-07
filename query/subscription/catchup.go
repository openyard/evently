package subscription

import (
	"context"
	"log"
	"sync"

	"github.com/openyard/evently/command/es"
	"github.com/openyard/evently/event"
	"github.com/openyard/evently/pkg/uuid"
)

type CatchUpOption func(s *CatchUpSubscription)

type EventHandlers map[string]map[string]event.HandleFunc // event-name, event-handle (ID), event-handler

// CatchUpSubscription is a subscription.CatchUpSubscription listening
// on new eda.Event from EventStore
type CatchUpSubscription struct {
	sync.RWMutex
	logger  *log.Logger
	cancel  context.CancelFunc
	context context.Context

	offset       uint64
	events       <-chan *event.Event
	eventStore   es.EventStore
	eventHandler EventHandlers
	onHandled    event.OnHandledFunc
}

func NewCatchUpSubscription(eventStore es.EventStore, opts ...CatchUpOption) *CatchUpSubscription {
	s := &CatchUpSubscription{eventStore: eventStore, eventHandler: make(EventHandlers)}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Listen starts to listen for subscribed events
func (s *CatchUpSubscription) Listen() {
	s.events = s.eventStore.SubscribeWithOffset(s.offset)
	go func(s *CatchUpSubscription) {
		s.context, s.cancel = context.WithCancel(context.Background())
		for {
			select {
			case event := <-s.events:
				s.handle(event)
			case <-s.context.Done():
				return
			}
		}
	}(s)
}

// Unsubscribe cancels the CatchUpSubscription
func (s *CatchUpSubscription) Unsubscribe() {
	s.cancel()
}

// Subscribe registers the given event handler by event-types
func (s *CatchUpSubscription) Subscribe(topic string, eventHandler event.HandleFunc) {
	s.Lock()
	defer s.Unlock()
	projections, found := s.eventHandler[topic]
	if !found {
		projections = make(map[string]event.HandleFunc)
	}
	projections[uuid.NewV4().String()] = eventHandler
	s.eventHandler[topic] = projections
}

// WithLogger sets the given logger to the CatchUpSubscription
func WithLogger(logger *log.Logger) CatchUpOption {
	return func(s *CatchUpSubscription) {
		s.logger = logger
	}
}

// WithOffset sets the given logger to the CatchUpSubscription
func WithOffset(offset uint64) CatchUpOption {
	return func(s *CatchUpSubscription) {
		s.offset = offset
	}
}

// WithOnHandledFunc registers the given func to be called after handling an event
func WithOnHandledFunc(f event.OnHandledFunc) CatchUpOption {
	return func(s *CatchUpSubscription) {
		s.onHandled = f
	}
}

func (s *CatchUpSubscription) handle(event *event.Event) {
	s.RLock()
	defer s.RUnlock()
	eh, found := s.eventHandler[event.Name()]
	if !found {
		s.logger.Printf("no handle for event [%s], ignore...", event.Name())
		return
	}

	for _, handle := range eh {
		err := handle.Handle(event)
		if err == nil && s.onHandled != nil {
			s.onHandled(event)
			continue
		}
		s.logger.Printf("could not publish event(%s, %q): %s", event.ID(), event.Name(), err.Error())
	}
}
