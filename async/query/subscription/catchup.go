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

// CatchUp is a subscription.CatchUp listening
// on new es.Entry from EventStore starting with given offset
type CatchUp struct {
	id       string
	workerID string

	commands chan *cmd
	consume  consume.ConsumerFunc
	ack      consume.AckFunc
	nack     consume.NackFunc
	ctx      context.Context
	cancel   context.CancelFunc

	ticker      *time.Ticker
	isListening bool
}

// NewCatchUp returns a subscription.CatchUp with given ID starting with given offset
// used for subscribing at given es.Transport
func NewCatchUp(ID string, offset uint64, transport es.Transport, opts ...CatchUpOption) *CatchUp {
	s := &CatchUp{
		id:       ID,
		workerID: uuid.NewV4().String(),
		commands: make(chan *cmd),
		consume:  consume.DefaultConsumer.Consume,
		ack:      noopAck,
		nack:     noopNack,
		ticker:   time.NewTicker(defaultSLA),
	}
	for _, opt := range opts {
		opt(s)
	}
	go s.start(offset, transport)
	return s
}

func (s *CatchUp) Listen() {
	if s.isListening {
		log.Printf("[WARN][%T] %s/%s already listening - ignore", s, s.id, s.workerID)
		return
	}
	go func() {
		s.isListening = true
		for {
			select {
			case cmd := <-s.commands:
				s.distribute(cmd.entries, cmd.onHandled, cmd.onError)
			}
		}
	}()
}

func (s *CatchUp) distribute(entries []*es.Entry, onHandled consume.AckFunc, onError consume.NackFunc) {
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
	if len(errs) == 0 && onHandled != nil {
		onHandled(entries...)
	} else if onError != nil {
		onError(entries...)
	}
	return
}

func (s *CatchUp) start(offset uint64, transport es.Transport) {
	log.Printf("[INFO][%T] start listening <id=%s,worker-id=%s>", s, s.id, s.workerID)
	s.ctx, s.cancel = context.WithCancel(context.Background())
	for {
		evently.DEBUG("...")
		select {
		case <-s.ctx.Done():
			log.Printf("[INFO][%T] stop subscription <id=%s,worker-id=%s>", s, s.id, s.workerID)
			return
		case <-s.ticker.C:
			inflight := transport.SubscribeWithOffset(offset, defaultBatchSize)
			entries := <-inflight
			if len(entries) == 0 {
				continue
			}
			offset += uint64(len(entries))
			s.commands <- &cmd{
				entries:   entries,
				onHandled: s.ack,
				onError:   s.nack,
			}
			evently.DEBUG("[DEBUG][%T] new offset=%d", s, offset)
		}
	}
}

func (s *CatchUp) Stop() {
	s.cancel()
}

type cmd struct {
	entries   []*es.Entry
	onHandled consume.AckFunc
	onError   consume.NackFunc
}

func noopAck(_ ...*es.Entry)  {}
func noopNack(_ ...*es.Entry) {}
