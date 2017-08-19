TIMEOUT  = 30
TESTDATA = "testdata"
GOOS     = "linux"

get:
	@go get ./...

build-testdata:
	@for FILE in $(TESTDATA)/eventTypes/*.go $(TESTDATA)/rules/*.go; do \
		GOOS=$(GOOS) go build -buildmode=plugin -o $${FILE%.go}.so $${FILE}	 ;\
	done

check test tests: get build-testdata
	@go test -timeout $(TIMEOUT)s ./...

integration: get
	GOOS=$(GOOS) go build -buildmode=plugin -o testdata/statefulIntegrationTests/rules/cloudTrail.so testdata/statefulIntegrationTests/rules/cloudTrail.go
	GOOS=$(GOOS) go build -buildmode=plugin -o testdata/statefulIntegrationTests/eventTypes/cloudTrail.so testdata/statefulIntegrationTests/eventTypes/cloudTrail.go
	go test -timeout 30s -run Integration