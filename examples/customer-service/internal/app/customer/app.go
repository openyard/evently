package customer

import (
	"customer/internal/app/customer/domain"
	"customer/internal/app/customer/edge"
	"customer/pkg/genproto/grpcapi"
	"github.com/openyard/evently/pkg/evently"
	"github.com/openyard/evently/pkg/volatile"

	"google.golang.org/grpc"
	"log"
	"net"
	"net/http"

	"github.com/julienschmidt/httprouter"
)

type App struct {
	grpcTransport *edge.GrpcTransport
	httpTransport *edge.HttpTransport
}

func NewCustomerApp(opts ...edge.GrpcTransportOption) *App {
	factory := domain.NewFactory()
	svc := evently.NewService(volatile.NewInMemoryEventStore(), factory.Create)
	grpcTransport := edge.NewGrpcTransport(svc.Process, opts...)
	httpTransport := edge.NewHttpTransport()
	return &App{
		grpcTransport: grpcTransport,
		httpTransport: httpTransport,
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

	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	log.Fatal(grpcServer.Serve(lis))
}
