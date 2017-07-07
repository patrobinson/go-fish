TIMEOUT  = 30
TESTDATA = "testdata"
GOOS     = "linux"

get:
	@go get ./...

build-testdata:
	@for FILE in $(TESTDATA)/eventTypes/*.go $(TESTDATA)/rules/*.go; do \
		GOOS=$(GOOS) go build -buildmode=plugin -o $${FILE%.go}.so $${FILE}	 ;\
	done

check test tests: build-testdata
	@go test -timeout $(TIMEOUT)s ./...
