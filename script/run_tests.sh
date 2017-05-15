#!/bin/bash

go get
go build -buildmode=plugin -o testdata/plugins/a.so testdata/plugins/a.go
go build -buildmode=plugin -o testdata/plugins/length.so testdata/plugins/length.go

go test
