package subscription

import (
	"sort"
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
	defer cp.updatedNow()
	if newPos < cp.GlobalPosition() {
		return
	}
	cp.globalPos.Store(newPos)
}

func (cp *Checkpoint) MaxGlobalPos(entries ...*es.Entry) uint64 {
	if len(entries) == 0 {
		return cp.GlobalPosition() // fallback
	}
	sort.Slice(entries[:], func(i, j int) bool {
		return entries[i].GlobalPos() < entries[j].GlobalPos()
	})
	return entries[len(entries)-1].GlobalPos()
}

func (cp *Checkpoint) updatedNow() {
	cp.lastSeenAt = time.Now()
}
