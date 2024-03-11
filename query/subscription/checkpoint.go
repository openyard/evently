package subscription

import (
	"github.com/openyard/evently/pkg/evently"
	"sync/atomic"
	"time"

	"github.com/openyard/evently/command/es"
)

type CheckpointStore interface {
	GetLatestCheckpoint(checkpointID string) *Checkpoint
	StoreCheckpoint(checkpoint *Checkpoint)
}

type Checkpoint struct {
	id         string
	globalPos  atomic.Uint64
	lastSeenAt time.Time
}

func NewCheckpoint(id string, globalPos uint64, lastSeenAt time.Time) *Checkpoint {
	cp := &Checkpoint{
		id:         id,
		lastSeenAt: lastSeenAt,
	}
	cp.globalPos.Store(globalPos)
	return cp
}

func (cp *Checkpoint) ID() string {
	return cp.id
}

func (cp *Checkpoint) GlobalPosition() uint64 {
	return cp.globalPos.Load()
}

func (cp *Checkpoint) LastSeenAt() time.Time {
	return cp.lastSeenAt
}

func (cp *Checkpoint) Update(newPos uint64) {
	evently.DEBUG("[DEBUG][%T] update globalPos to <%d>", cp, newPos)
	cp.globalPos.Store(newPos)
}

func (cp *Checkpoint) MaxGlobalPos(entries ...*es.Entry) uint64 {
	if len(entries) == 0 {
		evently.DEBUG("[DEBUG][%T] fallback to globalPos=%d", cp, cp.GlobalPosition())
		return cp.GlobalPosition() // fallback
	}
	maxGlobalPos := entries[0].GlobalPos()
	for _, e := range entries {
		if e.GlobalPos() > maxGlobalPos {
			maxGlobalPos = e.GlobalPos()
		}
	}
	evently.DEBUG("[DEBUG][%T] max globalPos=%d", cp, maxGlobalPos)
	return maxGlobalPos + 1
}
