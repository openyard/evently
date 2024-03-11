module customer

go 1.21.5

require (
	github.com/julienschmidt/httprouter v1.3.0
	github.com/openyard/evently v0.0.0-20231118095301-9921af7e2e5b
	google.golang.org/genproto v0.0.0-20240205150955-31a09d347014
	google.golang.org/grpc v1.61.0
	google.golang.org/protobuf v1.32.0
)

require (
	github.com/golang/protobuf v1.5.3 // indirect
	golang.org/x/net v0.20.0 // indirect
	golang.org/x/sys v0.16.0 // indirect
	golang.org/x/text v0.14.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240125205218-1f4bbc51befe // indirect
)

replace github.com/openyard/evently => ../../.
