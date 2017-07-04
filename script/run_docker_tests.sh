#!/bin/bash

docker run -ti -v $PWD:/go/src/github.com/patrobinson/go-fish -w /go/src/github.com/patrobinson/go-fish golang:1.8 make get test
