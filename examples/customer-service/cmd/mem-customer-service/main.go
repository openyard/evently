package main

import "flag"

func main() {
	grpcaddr := flag.String("grpcaddr", ":8205", "grpc listening address")
	httpaddr := flag.String("httpaddr", ":8206", "http listening address")
	flag.Parse()

	//edge.WithAsyncWriteSide()
	app := NewCustomerApp()
	app.Run(*grpcaddr, *httpaddr)
}
