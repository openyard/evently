package subscription

import (
	"time"

	"github.com/openyard/evently/async/query/consume"
)

type CatchUpOption func(s *CatchUp)

// WithTicker sets the given ticker for the CatchUp instead of a ticker with defaultSLA (subscription.SLAShort)
func WithTicker(ticker *time.Ticker) CatchUpOption {
	return func(s *CatchUp) {
		s.ticker = ticker
	}
}

// WithConsumer sets the given consumer for the CatchUp instead of consume.DefaultConsumer
func WithConsumer(consumer consume.Consumer) CatchUpOption {
	return func(s *CatchUp) {
		s.consume = consumer.Consume
	}
}

// WithAckFunc uses the given ackFunc for the CatchUp if consumer result was successful
func WithAckFunc(ackFunc consume.AckFunc) CatchUpOption {
	return func(s *CatchUp) {
		s.ack = ackFunc
	}
}

// WithNackFunc uses the given nackFunc for the CatchUp if consumer result wasn't successful
func WithNackFunc(nackFunc consume.NackFunc) CatchUpOption {
	return func(s *CatchUp) {
		s.nack = nackFunc
	}
}
