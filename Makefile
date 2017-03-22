

GOPATH := $(shell pwd)
export GOPATH
PATH := ${PATH}:$(shell pwd)/bin

main:
	go install protograph-cmd

proto-build:
	go get github.com/golang/protobuf/protoc-gen-go/
	protoc --go_out=Mgoogle/protobuf/struct.proto=github.com/golang/protobuf/ptypes/struct\
:./ src/protograph/ProtoGraph.proto

download:
	go get github.com/grpc-ecosystem/grpc-gateway/protoc-gen-grpc-gateway
	go get -d protograph-cmd
	go get google.golang.org/genproto/googleapis/api
