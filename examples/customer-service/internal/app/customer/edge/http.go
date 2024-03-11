package edge

import (
	"customer/internal/app/customer/domain"
	"customer/internal/app/customer/projection"
	"customer/internal/app/customer/report"
	"customer/internal/app/customer/store"
	"encoding/json"
	"fmt"
	"github.com/openyard/evently/pkg/volatile"
	"log"
	"net/http"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/openyard/evently/query/subscription"
)

type HttpTransport struct {
	allCustomerProjection *projection.AllCustomers
}

func NewHttpTransport() *HttpTransport {
	reportingStore := store.NewInMemoryCustomerStore()
	_ = domain.SubscribeCustomerEvents(
		subscription.NewCheckpoint("customer-events", 0, time.Now()),
		reportingStore,
		volatile.NewInMemoryEventStore())
	p := projection.AllCustomersProjection(reportingStore)
	return &HttpTransport{p}
}

func (t *HttpTransport) ListCustomers(w http.ResponseWriter, _ *http.Request, p httprouter.Params) {
	res, err := t.allCustomerProjection.Handle(projection.ListCustomerQuery{})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Println(err)
		return
	}

	b, err := json.Marshal(res.(map[string]*report.Customer))
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

func (t *HttpTransport) FetchCustomerByID(w http.ResponseWriter, _ *http.Request, p httprouter.Params) {
	ID := p.ByName("ID")

	res, err := t.allCustomerProjection.Handle(projection.GetCustomerByIDQuery{ID: ID})
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
	b, err := json.Marshal(res.(*report.Customer))
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
