package evently

import (
	"github.com/openyard/evently/command"
	"github.com/openyard/evently/command/es"
)

type CreateFunc func() *DomainModel

type CommandService struct {
	es es.EventStore
	cf CreateFunc
}

func NewService(es es.EventStore, cf CreateFunc) *CommandService {
	return &CommandService{
		es: es,
		cf: cf,
	}
}

func (cs *CommandService) Process(cmd *command.Command) error {
	h, err := cs.es.ReadStream(cmd.AggregateID())
	if err != nil {
		return err
	}
	dm := cs.cf()
	dm.Load(h)
	changes, err := dm.Execute(cmd)
	if err != nil {
		return err
	}
	return cs.es.AppendToStream(cmd.AggregateID(), cmd.ExpectedVersion(), changes...)
}
