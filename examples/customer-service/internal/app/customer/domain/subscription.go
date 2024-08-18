package domain

import (
	"github.com/openyard/evently/command/es"
	"github.com/openyard/evently/query/consume"
	"github.com/openyard/evently/query/subscription"
)

func SubscribeCustomerEvents(
	checkpoint *subscription.Checkpoint,
	reportingStore ReportingStore,
	transport es.Transport) *subscription.CatchUp {

	p := &allCustomersProjection{reportingStore: reportingStore}
	consume.Consume(p.Handle) // register handle at defaultConsumer

	opts := []subscription.CatchUpOption{
		subscription.WithAckFunc(func(entries ...*es.Entry) {
			checkpoint.Update(checkpoint.MaxGlobalPos(entries...))
		}),
	}
	s := subscription.NewCatchUpSubscription("customer-events", 0, transport, opts...)
	defer s.Listen()
	return s
}
