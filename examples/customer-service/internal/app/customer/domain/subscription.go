package domain

import (
	"log"

	"github.com/openyard/evently/async/query/consume"
	"github.com/openyard/evently/async/query/subscription"
	"github.com/openyard/evently/pkg/evently"
	"github.com/openyard/evently/tact/es"
)

func SubscribeCustomerEvents(
	checkpointStore subscription.CheckpointStore,
	reportingStore ReportingStore,
	subscriber es.Transport) *subscription.CatchUp {

	p := &allCustomersProjection{reportingStore: reportingStore}
	consume.Consume(p.Handle) // register handle at defaultConsumer
	tf := consume.NewTracingFilter("customer-events")
	cf := consume.NewConcurrentFilter(
		func(entries ...*es.Entry) {
			evently.DEBUG("[DEBUG] ackFunc - update checkpoint")
			checkpoint := checkpointStore.GetLatestCheckpoint("customer-events")
			checkpoint.Update(checkpoint.MaxGlobalPos(entries...))
			checkpointStore.StoreCheckpoint(checkpoint)
		},
		func(args ...*es.Entry) {
			log.Printf("[ERROR] nackFunc - nack entries %+v", args)
		}).WithConsumer(tf)
	cp := consume.NewPipe(cf.Consume, consume.DefaultConsumer)

	opts := []subscription.CatchUpOption{
		subscription.WithConsumer(cp),
	}
	s := subscription.NewCatchUp("customer-events", 0, subscriber, opts...)
	defer s.Listen()
	return s
}
