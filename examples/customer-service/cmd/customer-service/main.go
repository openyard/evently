package main

import (
	"customer/internal/app/customer"
	"customer/internal/app/customer/edge"
	"flag"
)

func main() {
	grpcaddr := flag.String("grpcaddr", ":8205", "grpc listening address")
	httpaddr := flag.String("httpaddr", ":8206", "http listening address")
	flag.Parse()

	app := customer.NewCustomerApp(edge.WithAsyncWriteSide())
	app.Run(*grpcaddr, *httpaddr)
}
