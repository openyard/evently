package subscription_test

import (
	"testing"
	"time"

	"github.com/openyard/evently/async/query/subscription"
	"github.com/openyard/evently/event"
	"github.com/openyard/evently/tact/es"
	"github.com/stretchr/testify/assert"
)

func TestNewCheckpoint(t *testing.T) {
	start := time.Now()
	sut := subscription.NewCheckpoint("my-subscription", 123, time.Now().Add(-24*time.Hour))
	assert.EqualValues(t, 123, sut.GlobalPosition())

	sut.Update(144)
	assert.EqualValues(t, 144, sut.GlobalPosition())
	assert.Greater(t, sut.LastSeenAt(), start)

	assert.EqualValues(t, 195, sut.MaxGlobalPos(entries()...))
	sut.Update(sut.MaxGlobalPos(entries()...))
	assert.EqualValues(t, 195, sut.GlobalPosition())
}

func entries() []*es.Entry {
	return []*es.Entry{
		es.NewEntry(193, event.NewEventAt("test-event", "payment-193", time.Now())),
		es.NewEntry(194, event.NewEventAt("test-event", "payment-194", time.Now())),
		es.NewEntry(195, event.NewEventAt("test-event", "payment-195", time.Now())),
	}
}
