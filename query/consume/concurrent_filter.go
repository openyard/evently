package consume

import (
	"context"
	"log"

	"github.com/openyard/evently/command/es"
)

var _ Consumer = (*ConcurrentFilter)(nil)

type ConcurrentFilter struct {
	ack  AckFunc
	nack NackFunc
	ch   chan *entriesWithContext
	next Consumer

	cancel   context.CancelFunc
	stopChan chan struct{}
}

func NewConcurrentFilter(ackFunc AckFunc, nackFunc NackFunc) *ConcurrentFilter {
	c := &ConcurrentFilter{
		ack:  ackFunc,
		nack: nackFunc,
		ch:   make(chan *entriesWithContext, defaultBufferSize),
		next: DefaultConsumer.Consumer(),
	}
	go c.start()
	return c
}

func (f *ConcurrentFilter) WithConsumer(c Consumer) *ConcurrentFilter {
	cf := NewConcurrentFilter(f.ack, f.nack)
	cf.next = c
	return cf
}

func (f *ConcurrentFilter) Consume(ctx Context, entries ...*es.Entry) error {
	f.ch <- &entriesWithContext{ctx, entries}
	return nil
}

func (f *ConcurrentFilter) start() (cancelFunc context.CancelFunc) {
	_, cancelFunc = context.WithCancel(context.Background())
	for {
		select {
		case <-f.stopChan:
			return
		default:
			entries := <-f.ch
			for _, e := range entries.entries {
				err := f.next.Consume(entries.ctx, e)
				if err != nil && f.nack != nil {
					log.Printf("[WARN] couldn't handle entry(#%d@%s id=%s): %s", e.GlobalPos(), e.Event().Name(), e.Event().ID(), err)
					f.nack(e)
					return
				}
				if f.ack != nil {
					f.ack(e)
				}
			}
		}
	}
}

func (f *ConcurrentFilter) Stop() {
	f.stopChan <- struct{}{}
	f.cancel()
}

type entriesWithContext struct {
	ctx     Context
	entries []*es.Entry
}
