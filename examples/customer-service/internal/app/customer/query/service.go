package query

import (
	"reflect"

	"github.com/openyard/evently/pkg/evently"
	q "github.com/openyard/evently/query"
)

type Service struct {
	customerStore ReportingStore
}

// NewService returns a new query service with given projections
func NewService(customerStore ReportingStore) *Service {
	return &Service{customerStore: customerStore}
}

func (p *Service) Handle(query q.Query) (any, error) {
	switch query.(type) {
	case ListCustomer:
		return p.customerStore.ListCustomers(), nil
	case GetCustomerByID:
		return p.customerStore.GetCustomerByID(query.(GetCustomerByID).ID), nil
	default:
		return nil, evently.Errorf(1002, "unknown query <%s>", reflect.TypeOf(query).Name())
	}
}

type ListCustomer struct{}
type GetCustomerByID struct {
	ID string
}
