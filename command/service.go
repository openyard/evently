package command

import "github.com/openyard/evently/command/es"

type CreateFunc func() *DomainModel

type Service struct {
	es es.EventStore
	cf CreateFunc
}

func NewService(es es.EventStore, cf CreateFunc) *Service {
	return &Service{
		es: es,
		cf: cf,
	}
}

func (cs *Service) Process(cmd *Command) error {
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
