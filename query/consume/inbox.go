package consume

import (
	"time"

	"github.com/openyard/evently/command/es"
)

var _ Inbox = (*noopInbox)(nil)

type Inbox interface {
	QueueEntries(subscriptionID, workerID string, noticedAt time.Time, newEvents []*es.Entry) error
	ReceiveEntries(subscriptionID, workerID string, receiver chan []*es.Entry)
	AckEntries(subscriptionID, workerID string, eventIDs []string) error
}

type noopInbox struct {
}

func (i noopInbox) QueueEntries(_, _ string, _ time.Time, _ []*es.Entry) error {
	return nil
}

func (i noopInbox) ReceiveEntries(_, _ string, r chan []*es.Entry) {
	r <- make([]*es.Entry, 0)
}

func (i noopInbox) AckEntries(_, _ string, _ []string) error {
	return nil
}
