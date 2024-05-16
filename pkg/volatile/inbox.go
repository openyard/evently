package volatile

import (
	"log"
	"sync"
	"time"

	"github.com/openyard/evently/command/es"
	"github.com/openyard/evently/pkg/uuid"
	"github.com/openyard/evently/query/consume"
)

const defaultBufferSize = 128

var (
	_ consume.Inbox    = (*Inbox)(nil)
	_ consume.Consumer = (*Inbox)(nil)
)

type queue struct {
	ctx       consume.Context
	noticedAt time.Time
	entries   []*es.Entry
}

// Inbox is a volatile consume.Inbox which can be used with a local instance purpose.
// The most common use case is to use this volatile.Inbox for unit tests or local test scenarios of an event sourced
// application.
// It can also be used for individually not shared read-models.
type Inbox struct {
	mu      sync.RWMutex
	batches map[string][]*es.Entry // batchTxID, entries
	queues  map[string]chan *queue // subscriptionID, queues
}

// WithVolatileInbox creates an in-memory inbox executes the given func
func WithVolatileInbox(f func(ib consume.Inbox)) {
	ib_ := NewInbox()
	f(ib_)
}

func NewInbox() *Inbox {
	ib := &Inbox{
		queues:  make(map[string]chan *queue),
		batches: make(map[string][]*es.Entry),
	}
	return ib
}

func (ib *Inbox) Handle(ctx consume.Context, entries ...*es.Entry) error {
	return ib.QueueEntries(ctx, time.Now(), entries)
}

func (ib *Inbox) QueueEntries(ctx consume.Context, noticedAt time.Time, newEntries []*es.Entry) error {
	qCh, ok := ib.queues[ctx.SubscriptionID()]
	if !ok {
		qCh = make(chan *queue, defaultBufferSize)
	}
	qCh <- &queue{ctx, noticedAt, newEntries}
	ib.queues[ctx.SubscriptionID()] = qCh
	return nil
}

func (ib *Inbox) ReceiveEntries(subscriptionID string) (batchTxID string, entries []*es.Entry) {
	q, ok := ib.queues[subscriptionID]
	if !ok {
		log.Printf("no entries")
		return "", make([]*es.Entry, 0)
	}
	batch := <-q
	entries = batch.entries
	batchTxID = uuid.NewV4().String()

	ib.mu.Lock()
	defer ib.mu.Unlock()
	ib.batches[batchTxID] = entries
	return
}

func (ib *Inbox) AckEntries(batchTxID string, _ []string) error {
	ib.mu.Lock()
	defer ib.mu.Unlock()
	delete(ib.batches, batchTxID)
	return nil
}
