package query

import "customer/internal/app/customer/reporting"

type ReportingStore interface {
	ListCustomers() map[string]*reporting.Customer
	GetCustomerByID(ID string) *reporting.Customer
}
