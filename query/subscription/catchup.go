package subscription

import (
	"context"
	"log"
	"sync"

	"github.com/openyard/evently/command/es"
	"github.com/openyard/evently/pkg/evently"
	"github.com/openyard/evently/pkg/uuid"
	"github.com/openyard/evently/query/consume"
)

type CatchUpOption func(s *CatchUpSubscription)

// CatchUpSubscription is a subscription.CatchUpSubscription listening
// on new eda.Event from EventStore
type CatchUpSubscription struct {
	sync.RWMutex
	id      string
	context context.Context
	cancel  context.CancelFunc

	entries    <-chan []*es.Entry
	checkpoint *Checkpoint

	transport es.Transport
	consume   consume.ConsumerFunc
	ack       AckFunc
	nack      NackFunc

	listening bool
}

func NewCatchUpSubscription(transport es.Transport, opts ...CatchUpOption) *CatchUpSubscription {
	s := &CatchUpSubscription{
		id:        uuid.NewV4().String(),
		transport: transport,
		consume:   consume.DefaultConsumer.Handle,
		ack:       noopAck,
		nack:      noopNack,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Listen starts to listen for new events from the transport
func (s *CatchUpSubscription) Listen() {
	log.Printf("[DEBUG][%T] Listen - ID=%s", s, s.id)
	s.Lock()
	defer s.Unlock()
	if s.listening {
		log.Printf("[%T] %s already listening - ignore", s, s.checkpoint.ID())
		return
	}
	s.context, s.cancel = context.WithCancel(context.Background())
	go func(s *CatchUpSubscription) {
		s.listening = true
		evently.DEBUG("[DEBUG][%T] start listening...", s)
		for {
			s.entries = s.transport.SubscribeWithOffset(s.checkpoint.GlobalPosition())
			evently.DEBUG("[DEBUG][%T] enter for loop...", s)
			evently.DEBUG("[DEBUG][%T] listening: %v", s, s.listening)
			select {
			case entries := <-s.entries:
				log.Printf("[DEBUG][%T] len(entries)=%d", s, len(entries))
				if err := s.consume(&consume.Context{Context: s.context}, entries...); err != nil {
					log.Printf("[ERROR][%T] couldn't handle all events: %s\n%v", s, err, entries)
					s.nack(entries...)
				}
				evently.DEBUG("[DEBUG][%T] ack all <%d> events: n%+v", s, len(entries), entries)
				s.ack(entries...)
			case <-s.context.Done():
				evently.DEBUG("[DEBUG][%T] context done <%v>", s, s.context.Err())
				return
			}
		}
	}(s)
}

func (s *CatchUpSubscription) Stop() {
	s.Lock()
	defer s.Unlock()
	s.cancel()
}

// WithCheckpoint sets the given checkpoint to the CatchUpSubscription
func WithCheckpoint(checkpoint *Checkpoint) CatchUpOption {
	return func(s *CatchUpSubscription) {
		s.checkpoint = checkpoint
	}
}

// WithConsumer sets the given consumer for the CatchUpSubscription instead of consume.DefaultConsumer
func WithConsumer(consumer consume.Consumer) CatchUpOption {
	return func(s *CatchUpSubscription) {
		s.consume = consumer.Handle
	}
}

// WithAckFunc uses the given ackFunc for the CatchUpSubscription if consumer result was successful
func WithAckFunc(ackFunc AckFunc) CatchUpOption {
	return func(s *CatchUpSubscription) {
		s.ack = ackFunc
	}
}

// WithNackFunc uses the given nackFunc for the CatchUpSubscription if consumer result wasn't successful
func WithNackFunc(nackFunc NackFunc) CatchUpOption {
	return func(s *CatchUpSubscription) {
		s.nack = nackFunc
	}
}

type AckFunc func(entries ...*es.Entry)
type NackFunc func(entries ...*es.Entry)

func noopAck(_ ...*es.Entry)  {}
func noopNack(_ ...*es.Entry) {}
