#!/bin/bash -e

go get
go build -buildmode=plugin -o testdata/rules/a.so testdata/rules/a.go
go build -buildmode=plugin -o testdata/rules/length.so testdata/rules/length.go
go build -buildmode=plugin -o testdata/eventTypes/example_event_type.so testdata/eventTypes/example_event_type.go

go test
