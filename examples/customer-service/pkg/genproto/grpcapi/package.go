//go:generate protoc --go_out=. --go-grpc_out=. --proto_path=../../../api/proto --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative customer.proto
package grpcapi
