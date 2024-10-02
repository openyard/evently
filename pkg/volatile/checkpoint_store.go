package volatile

import (
	"sync"
	"time"

	"github.com/openyard/evently/async/query/subscription"
)

var _ subscription.CheckpointStore = (*CheckpointStore)(nil)

// CheckpointStore stores checkpoints in memory
type CheckpointStore struct {
	mu sync.RWMutex
	cp map[string]*subscription.Checkpoint
}

// NewCheckpointStore returns an empty volatile.CheckpointStore
func NewCheckpointStore() *CheckpointStore {
	return &CheckpointStore{cp: make(map[string]*subscription.Checkpoint)}
}

// GetLatestCheckpoint returns the latest checkpoint for given checkpointID
func (c *CheckpointStore) GetLatestCheckpoint(checkpointID string) *subscription.Checkpoint {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if checkpoint, ok := c.cp[checkpointID]; ok {
		return checkpoint
	}
	return subscription.NewCheckpoint(checkpointID, 0, time.Now())
}

// StoreCheckpoint stores the given checkpoint as latest version
func (c *CheckpointStore) StoreCheckpoint(checkpoint *subscription.Checkpoint) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cp[checkpoint.ID()] = checkpoint
}
