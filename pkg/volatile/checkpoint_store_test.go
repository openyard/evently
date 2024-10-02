package volatile_test

import (
	"testing"
	"time"

	"github.com/openyard/evently/async/query/subscription"
	"github.com/openyard/evently/pkg/volatile"
	"github.com/stretchr/testify/assert"
)

func TestCheckpointStore_GetLatestCheckpoint(t *testing.T) {
	sut := volatile.NewCheckpointStore()
	cpA := sut.GetLatestCheckpoint("other-subscription")
	cpB := sut.GetLatestCheckpoint("test-subscription")
	assert.EqualValues(t, 0, cpA.GlobalPosition())
	assert.EqualValues(t, 0, cpB.GlobalPosition())
}

func TestCheckpointStore_StoreCheckpoint(t *testing.T) {
	sut := volatile.NewCheckpointStore()
	sut.StoreCheckpoint(subscription.NewCheckpoint(
		"test-subscription",
		123,
		time.Date(2024, time.March, 30, 11, 50, 30, 0, time.UTC)))
	cpA := sut.GetLatestCheckpoint("other-subscription")
	cpB := sut.GetLatestCheckpoint("test-subscription")
	assert.EqualValues(t, 0, cpA.GlobalPosition())
	assert.EqualValues(t, 123, cpB.GlobalPosition())
	assert.Equal(t, "2024-03-30T11:50:30Z", cpB.LastSeenAt().Format(time.RFC3339))
}
