package example

import (
	"github.com/openyard/evently/command"
	"github.com/openyard/evently/command/es/estest"
)

type CustomerApp struct {
	api *CustomerAPI
}

func NewCustomerApp(opts ...APIOption) *CustomerApp {
	factory := new(CustomerFactory)
	svc := command.NewService(estest.NewTestEventStore(), factory.Create)
	api := NewCustomerAPI(svc, opts...)
	return &CustomerApp{api}
}

func (a *CustomerApp) Run() {

}
