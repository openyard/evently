package subscription

import (
	"github.com/openyard/evently/command/es"
	"log"
	"sync/atomic"
	"time"
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
	log.Printf("[%T] [DEBUG] update globalPos to <%d>", cp, newPos)
	cp.globalPos.Store(newPos)
}

func (cp *Checkpoint) MaxGlobalPos(entries ...*es.Entry) uint64 {
	if len(entries) == 0 {
		log.Printf("[%T] [DEBUG] fallback to globalPos=%d", cp, cp.GlobalPosition())
		return cp.GlobalPosition() // fallback
	}
	maxGlobalPos := entries[0].GlobalPos
	for _, e := range entries {
		if e.GlobalPos > maxGlobalPos {
			maxGlobalPos = e.GlobalPos
		}
	}
	log.Printf("[%T] [DEBUG] max globalPos=%d", cp, maxGlobalPos)
	return maxGlobalPos + 1
}
