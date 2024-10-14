package subscription

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/openyard/evently/async/query/consume"
	"github.com/openyard/evently/pkg/evently"
	"github.com/openyard/evently/pkg/uuid"
	"github.com/openyard/evently/tact/es"
)

// Volatile is a subscription.Volatile listening
// on new es.Entry from es.EventStore without offset
type Volatile struct {
	id       string
	workerID string

	entries chan []*es.Entry
	consume consume.ConsumerFunc
	ctx     context.Context
	cancel  context.CancelFunc

	ticker      *time.Ticker
	offset      uint64
	isListening bool
}

// NewVolatile returns a subscription.Volatile with given ID starting without offset
// subsribing at given es.Transport
func NewVolatile(ID string, transport es.Transport) *Volatile {
	s := &Volatile{
		id:       ID,
		workerID: uuid.NewV4().String(),
		entries:  make(chan []*es.Entry),
		consume:  consume.DefaultConsumer.Consume,
		offset:   0,
		ticker:   time.NewTicker(defaultSLA),
	}
	go s.start(transport)
	return s
}

func (s *Volatile) Listen() {
	if s.isListening {
		log.Printf("[WARN][%T] %s/%s already listening - ignore", s, s.id, s.workerID)
		return
	}
	go func() {
		s.isListening = true
		for {
			select {
			case entries := <-s.entries:
				s.distribute(entries...)
			}
		}
	}()
}

func (s *Volatile) distribute(entries ...*es.Entry) {
	var errs []error
	var messages []consume.Msg
	for _, e := range entries {
		messages = append(messages, consume.NewMsg(e.Event().ID(), e.Event().Name(), e.Event().Payload()))
	}
	newContext := consume.NewContext(s.id, messages)
	if err := s.consume(newContext, entries...); err != nil {
		s := fmt.Errorf("consume entries(%+v) failed: %s", entries, err)
		log.Printf("[ERROR] %s", s)
		errs = append(errs, s)
	}
}

func (s *Volatile) start(transport es.Transport) {
	log.Printf("[INFO][%T] start listening <id=%s,worker-id=%s>", s, s.id, s.workerID)
	s.ctx, s.cancel = context.WithCancel(context.Background())
	for {
		select {
		case <-s.ctx.Done():
			log.Printf("[INFO][%T] stop subscription <id=%s,worker-id=%s>", s, s.id, s.workerID)
			return
		case <-s.ticker.C:
			if s.offset == 0 {
				inflight := transport.Subscribe(defaultBatchSize)
				entries := <-inflight
				evently.DEBUG("[DEBUG][%T] inflight new entries(%+v) ...", s, len(entries))
				if len(entries) == 0 {
					continue
				}
				s.offset += uint64(len(entries))
				s.entries <- entries
			} else {
				inflight := transport.SubscribeWithOffset(s.offset, defaultBatchSize)
				entries := <-inflight
				evently.DEBUG("[DEBUG][%T] inflight new entries(%+v) ...", s, len(entries))
				if len(entries) == 0 {
					continue
				}
				s.offset += uint64(len(entries))
				s.entries <- entries
			}
		}
	}
}

func (s *Volatile) Stop() {
	s.cancel()
}
