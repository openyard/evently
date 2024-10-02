package domain

import (
	"github.com/openyard/evently/tact/cmd"
	"github.com/openyard/evently/tact/es"
)

type Factory struct{}

func NewFactory() *Factory {
	return &Factory{}
}

func (cf *Factory) Create() *cmd.DomainModel {
	c := &Customer{}
	c.Init(
		"Customer",
		map[string]es.Transition{
			newCustomerOnboardedEventName: c.onOnboarding,
			customerActivatedEventName:    c.onActivated,
			customerBlockedEventName:      c.onBlocked,
		},
		map[string]cmd.HandleFunc{
			onboardCustomerCommandName:  c.create,
			activateCustomerCommandName: c.activate,
			blockCustomerCommandName:    c.block,
		},
	)
	return &c.DomainModel
}
