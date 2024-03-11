package evently

import (
	"github.com/openyard/evently/command"
	"github.com/openyard/evently/command/es"
)

type Batch struct {
	es es.MultiStreamEventStore
	cf CreateFunc
}

func NewBatch(es es.MultiStreamEventStore, cf CreateFunc) *Batch {
	return &Batch{
		es: es,
		cf: cf,
	}
}

func (b *Batch) Process(commands []*command.Command) error {
	var streamIDs []string
	for _, cmd := range commands {
		streamIDs = append(streamIDs, cmd.AggregateID())
	}
	h, err := b.es.ReadStreams(streamIDs)
	if err != nil {
		return err
	}
	var eventMap map[string][]es.Change
	for _, cmd := range commands {
		dm := b.cf()
		dm.Load(h[cmd.AggregateID()])
		changes, err := dm.Execute(cmd)
		if err != nil {
			return err
		}
		var changeList []es.Change
		for v, e := range changes {
			changeList = append(changeList, es.NewChange(dm.version+uint64(v), e))
		}
		eventMap[cmd.AggregateID()] = changeList
	}
	return b.es.AppendToStreams(eventMap)
}
