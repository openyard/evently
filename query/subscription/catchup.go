package subscription

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/openyard/evently/command/es"
	"github.com/openyard/evently/pkg/evently"
	"github.com/openyard/evently/pkg/uuid"
	"github.com/openyard/evently/query/consume"
)

const (
	SLAMicro  = time.Millisecond * 10
	SLAShort  = time.Second * 5
	SLAMedium = time.Second * 60
	SLALarge  = time.Minute * 10
	SLALong   = time.Minute * 60

	BatchSizeXS BatchSize = 1024
	BatchSizeS            = 2048
	BatchSizeM            = 4096
	BatchSizeL            = 8192
	BatchSizeXL           = 16384

	defaultSLA       = SLAShort
	defaultBatchSize = BatchSizeM
)

// SLA defines the maximum time-period where eventual consistency
// between event-store and projections / read-models can occur
// Ability to make adjustments should be done on startup and via ENV as config value
// Default SLA is set to SLAShort which is 5s
type SLA time.Duration
type BatchSize uint16

// CatchUp is a subscription.CatchUp listening
// on new eda.Event from EventStore
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

func NewCatchUpSubscription(ID string, offset uint64, transport es.Transport, opts ...CatchUpOption) *CatchUp {
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
		log.Printf("[%T][WARN] %s/%s already listening - ignore", s, s.id, s.workerID)
		return
	}
	go func() {
		s.isListening = true
		for {
			select {
			case cmd := <-s.commands:
				s.distribute(cmd.entries, cmd.onPublish, cmd.onError)
			}
		}
	}()
}

func (s *CatchUp) distribute(entries []*es.Entry, onPublished consume.AckFunc, onError consume.NackFunc) {
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
	if len(errs) == 0 && onPublished != nil {
		onPublished(entries...)
	} else if onError != nil {
		onError(entries...)
	}
	return
}

func (s *CatchUp) start(offset uint64, transport es.Transport) {
	log.Printf("[%T][INFO] start listening <id=%s,worker-id=%s>", s, s.id, s.workerID)
	s.ctx, s.cancel = context.WithCancel(context.Background())
	for {
		select {
		case <-s.ctx.Done():
			log.Printf("[%T][INFO] stop subscription <id=%s,worker-id=%s>", s, s.id, s.workerID)
			return
		case <-s.ticker.C:
			inflight := transport.SubscribeWithOffset(offset, defaultBatchSize)
			entries := <-inflight
			evently.DEBUG("[%T][DEBUG] inflight new entries(%+v) ...", s, len(entries))
			if len(entries) == 0 {
				continue
			}
			offset += 1
			s.commands <- &cmd{
				entries:   entries,
				onPublish: s.ack,
				onError:   s.nack,
			}
			evently.DEBUG("[%T][DEBUG] new offset=%d", s, offset)
		}
	}
}

func (s *CatchUp) Stop() {
	s.cancel()
}

type cmd struct {
	entries   []*es.Entry
	onPublish consume.AckFunc
	onError   consume.NackFunc
}

func noopAck(_ ...*es.Entry)  {}
func noopNack(_ ...*es.Entry) {}
