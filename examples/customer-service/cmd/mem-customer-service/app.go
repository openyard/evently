package main

import (
	"customer/internal/app/customer/domain"
	"customer/internal/app/customer/edge"
	"customer/internal/app/customer/store"
	"customer/pkg/genproto/grpcapi"

	"log"
	"net"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/openyard/evently/async/query/subscription"
	"github.com/openyard/evently/pkg/volatile"
	"github.com/openyard/evently/tact/cmd"
	"github.com/openyard/evently/tact/es"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type App struct {
	grpcTransport *edge.GrpcTransport
	httpTransport *edge.HttpTransport
	subscription  *subscription.CatchUp
}

func NewCustomerApp(opts ...edge.GrpcTransportOption) *App {
	eventStore, reportingStore := initStores()
	svc := initService(eventStore)
	customerEvents := subscribeCustomerEvents(reportingStore, eventStore)
	grpcTransport := edge.NewGrpcTransport(svc.Process, opts...)
	httpTransport := edge.NewHttpTransport(reportingStore)

	return &App{
		grpcTransport: grpcTransport,
		httpTransport: httpTransport,
		subscription:  customerEvents,
	}
}

func (a *App) Run(grpcAddr, httpAddr string) {
	r := httprouter.New()
	// HTTP API (read side)
	r.GET("/customers", a.httpTransport.ListCustomers)
	r.GET("/customers/:ID", a.httpTransport.FetchCustomerByID)

	go func() {
		log.Fatal(http.ListenAndServe(httpAddr, r))
	}()

	// gRPC API (write side)
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	grpcapi.RegisterCustomerServer(grpcServer, a.grpcTransport)
	// Register reflection service on gRPC server.
	reflection.Register(grpcServer)

	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	log.Fatal(grpcServer.Serve(lis))
}

func initStores() (*volatile.EventStore, *store.InMemoryCustomerStore) {
	eventStore := volatile.NewEventStore()
	reportingStore := store.NewInMemoryCustomerStore()
	return eventStore, reportingStore
}

func initService(eventStore es.EventStore) *cmd.Service {
	factory := domain.NewFactory()
	return cmd.NewService(eventStore, factory.Create)
}

func subscribeCustomerEvents(
	reportingStore *store.InMemoryCustomerStore,
	eventStore *volatile.EventStore) *subscription.CatchUp {
	return domain.SubscribeCustomerEvents(
		volatile.NewCheckpointStore(),
		reportingStore,
		eventStore)
}
