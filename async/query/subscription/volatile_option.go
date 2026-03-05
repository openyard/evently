package subscription

import (
	"time"

	"github.com/openyard/evently/async/query/consume"
)

type VolatileOption func(s *Volatile)

// WithVolatileConsumer sets the given consumer for the Volatile instead of consume.DefaultConsumer
func WithVolatileConsumer(consumer consume.Consumer) VolatileOption {
	return func(s *Volatile) {
		s.consume = consumer.Consume
	}
}

// WithVolatileTicker sets the given ticker for the Volatile instead of a ticker with defaultSLA (subscription.SLAShort)
func WithVolatileTicker(ticker *time.Ticker) VolatileOption {
	return func(s *Volatile) {
		s.ticker = ticker
	}
}
