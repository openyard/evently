package cmd

import (
	"log"

	"github.com/openyard/evently/tact/es"
)

var (
	_ API         = (*Service)(nil)
	_ es.Callback = (*Service)(nil)
)

type Service struct {
	es      es.EventStore
	create  CreateFunc
	handler map[string]es.EventHandler
}

func NewService(_es es.EventStore, cf CreateFunc) *Service {
	return &Service{
		es:      _es,
		create:  cf,
		handler: make(map[string]es.EventHandler),
	}
}

func (svc *Service) Process(cmd *Command) error {
	h, err := svc.es.Read(cmd.AggregateID())
	if err != nil {
		return err
	}
	dm := svc.create()
	if dm.Version() > 0 {
		dm.Load(h[0].Events())
	}
	changes, err := dm.Execute(cmd)
	if err != nil {
		return err
	}
	change := es.NewChange(cmd.AggregateID(), cmd.ExpectedVersion(), changes...)
	if err := svc.es.Append(change); err != nil {
		return err
	}
	defer svc.handle([]es.Change{change})
	return nil
}

func (svc *Service) handle(list []es.Change) {
	for _, ch := range list {
		for _, e := range ch.Events() {
			if h, ok := svc.handler[e.ID()]; ok && h != nil {
				if err := h(e); err != nil {
					log.Printf("[ERROR]\t%T.handle - couldn't handle: %s", svc, err)
				}
			}
			if h, ok := svc.handler[e.Name()]; ok && h != nil {
				if err := h(e); err != nil {
					log.Printf("[ERROR]\t%T.handle - couldn't handle: %s", svc, err)
				}
			}
		}
	}
}

func (svc *Service) Register(ID string, handle es.EventHandler) {
	svc.handler[ID] = handle
}

func (svc *Service) Unregister(ID string) {
	delete(svc.handler, ID)
}
