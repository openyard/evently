package example

import (
	"fmt"
	"time"

	"github.com/openyard/evently/command"
	"github.com/openyard/evently/command/async"
	"github.com/openyard/evently/command/es/estest"
	"github.com/openyard/evently/pkg/uuid"
)

type CommandService func(c *command.Command) error

type APIOption func(a *CustomerAPI)

type CustomerAPI struct {
	cs CommandService
	cp *CustomerProjection
}

func NewCustomerAPI(cs *command.Service, opts ...APIOption) *CustomerAPI {
	c := &CustomerAPI{cs: cs.Process, cp: NewCustomerProjection(estest.NewTestEventStore())}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

func WithAsyncWriteSide() APIOption {
	return func(a *CustomerAPI) {
		q := async.NewCommandQueue(2, async.WithQueueTimeout(time.Millisecond*200))
		q.Register(command.HandleFunc(a.cs))
		a.cs = q.Send
	}
}

func (api *CustomerAPI) OnboardCustomer(name string, birthdate time.Time, sex byte) error {
	return api.cs(OnboardCustomer(uuid.NewV4().String(), name, birthdate, sex))
}

func (api *CustomerAPI) ActivateCustomer(ID string) error {
	return api.ActivateCustomer(ID)
}

func (api *CustomerAPI) BlockCustomer(ID string) error {
	return api.BlockCustomer(ID)
}

func (api *CustomerAPI) FetchCustomer(ID string) (any, error) {
	c, ok := api.cp.customers[ID]
	if !ok {
		return nil, fmt.Errorf("customer <%s> not found", ID)
	}
	return c, nil
}
