package projection

import (
	"customer/internal/app/customer/report"
	"github.com/openyard/evently/pkg/evently"
	"reflect"

	"github.com/openyard/evently/query"
)

type ListCustomerQuery struct{}
type GetCustomerByIDQuery struct {
	ID string
}

type ReportingStore interface {
	ListCustomers() map[string]*report.Customer
	GetCustomerByID(ID string) *report.Customer
}

type AllCustomers struct {
	customerStore ReportingStore
}

func AllCustomersProjection(customerStore ReportingStore) *AllCustomers {
	return &AllCustomers{customerStore: customerStore}
}

func (p *AllCustomers) Handle(query query.Query) (any, error) {
	switch query.(type) {
	case ListCustomerQuery:
		return p.customerStore.ListCustomers(), nil
	case GetCustomerByIDQuery:
		return p.customerStore.GetCustomerByID(query.(*GetCustomerByIDQuery).ID), nil
	default:
		return nil, evently.Errorf(1002, "unknown query <%s>", reflect.TypeOf(query).Name())
	}
}
