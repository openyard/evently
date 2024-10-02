package main

import (
	"database/sql"
	"fmt"
	"github.com/openyard/evently/tact/cmd"
	"log"
	"net"
	"net/http"
	"regexp"
	"time"

	"customer/internal/app/customer/domain"
	"customer/internal/app/customer/edge"
	"customer/internal/app/customer/store"
	"customer/pkg/genproto/grpcapi"

	"github.com/openyard/evently/async/query/subscription"
	"github.com/openyard/evently/pkg/config"
	"github.com/openyard/evently/pkg/pg"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/julienschmidt/httprouter"
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
	db := openDB()
	eventStore, reportingStore := initStores(db)
	svc := initService(eventStore)
	customerEvents := subscribeCustomerEvents(reportingStore, eventStore, db)
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

func openDB() *sql.DB {
	dbHost := config.GetEnv("DB_HOST", "localhost")
	dbPort := config.GetEnv("DB_PORT", "5432")
	dbName := config.GetEnv("DB_NAME", "customer")
	dbUser := config.GetEnv("DB_USER", "customer")
	dbPass := config.GetSecret("DB_PASS", "secret")
	dbParams := config.GetEnv("DB_PARAMS", "?sslmode=disable")

	connStr := fmt.Sprintf("postgres://%s:%s@%s:%s/%s%s", dbUser, dbPass, dbHost, dbPort, dbName, dbParams)

	re := regexp.MustCompile(`(^.*):.*@(.*$)`)
	log.Printf("using connection: %s\n", re.ReplaceAllString(connStr, `$1:*****@$2`))

	db, err := sql.Open("pgx", connStr)
	if err != nil {
		log.Println(err)
	}

	for i := 0; i < 10; i++ {
		if err = db.Ping(); err != nil {
			log.Println(err)
			time.Sleep(10 * time.Second)
		} else {
			break
		}
	}
	if err != nil {
		log.Fatal(err)
	}
	db.SetMaxOpenConns(16)
	db.SetMaxIdleConns(16)
	//db.SetConnMaxIdleTime(time.Second * 5)
	//db.SetConnMaxLifetime(time.Second * 2)
	go logDBStats(db)
	return db
}

func logDBStats(db *sql.DB) {
	for {
		timer := time.NewTimer(10 * time.Minute)
		select {
		case <-timer.C:
			log.Printf("[DEBUG][DBstats] %#+v", db.Stats())
		}
	}
}

func initStores(db *sql.DB) (*pg.EventStore, *store.InMemoryCustomerStore) {
	eventStore := pg.NewEventStore(db, pg.WithBulkMode())
	reportingStore := store.NewInMemoryCustomerStore()
	return eventStore, reportingStore
}

func initService(eventStore es.EventStore) *cmd.Service {
	factory := domain.NewFactory()
	return cmd.NewService(eventStore, factory.Create)
}

func subscribeCustomerEvents(reportingStore *store.InMemoryCustomerStore, eventStore *pg.EventStore, db *sql.DB) *subscription.CatchUp {
	return domain.SubscribeCustomerEvents(
		pg.NewCheckpointStore(db),
		reportingStore,
		eventStore)
}
