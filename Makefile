

GOPATH := $(shell pwd)
export GOPATH
PATH := ${PATH}:$(shell pwd)/bin

proto_build:
	go get github.com/golang/protobuf/protoc-gen-go/
	protoc --go_out=Mgoogle/protobuf/struct.proto=github.com/golang/protobuf/ptypes/struct:./ src/gaia/protograph/ProtoGraph.proto