package edge

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"customer/internal/app/customer/query"
	"customer/internal/app/customer/reporting"

	"github.com/julienschmidt/httprouter"
)

type HttpTransport struct {
	q *query.Service
}

func NewHttpTransport(reportingStore query.ReportingStore) *HttpTransport {
	q := query.NewService(reportingStore)
	return &HttpTransport{q}
}

func (t *HttpTransport) ListCustomers(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	res, err := t.q.Handle(query.ListCustomer{})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Println(err)
		return
	}

	b, err := json.Marshal(res.(map[string]*reporting.Customer))
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Println(err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, err = w.Write(b)
	if err != nil {
		log.Println(err)
	}
}

func (t *HttpTransport) FetchCustomerByID(w http.ResponseWriter, _ *http.Request, p httprouter.Params) {
	ID := p.ByName("ID")

	res, err := t.q.Handle(query.GetCustomerByID{ID: ID})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Println(err)
		return
	}

	if res == nil {
		w.WriteHeader(http.StatusNotFound)
		w.Header().Set("Content-Type", "plain/text")
		_, err := w.Write([]byte(fmt.Sprintf("customer <%s> not found", ID)))
		if err != nil {
			log.Println(err)
		}
		log.Printf("customer <%s> not found", ID)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	b, err := json.Marshal(res.(*reporting.Customer))
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Println(err)
		return
	}
	_, err = w.Write(b)
	if err != nil {
		log.Println(err)
	}
}
