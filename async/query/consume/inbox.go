package consume

import (
	"time"

	"github.com/openyard/evently/tact/es"
)

var _ Inbox = (*noopInbox)(nil)

type Inbox interface {
	QueueEntries(ctx Context, noticedAt time.Time, newEvents []*es.Entry) error
	ReceiveEntries(subscriptionID string) (batchTxID string, entries []*es.Entry)
	AckEntries(batchTxID string, entryIDs []string) error
}

type noopInbox struct {
}

func (i noopInbox) QueueEntries(_ Context, _ time.Time, _ []*es.Entry) error {
	return nil
}

func (i noopInbox) ReceiveEntries(_ string) (string, []*es.Entry) {
	return "", make([]*es.Entry, 0)
}

func (i noopInbox) AckEntries(_ string, _ []string) error {
	return nil
}
