TIMEOUT  = 30
TESTDATA = "testdata"
GOOS     = "linux"

get:
	@go get ./...

fake-get:
	@cp -r vendor/github.com/* $$GOPATH/src/github.com/

build:
	@go build

build-testdata:
	@for FILE in $(TESTDATA)/eventTypes/*.go $(TESTDATA)/rules/*.go; do \
		GOOS=$(GOOS) go build -buildmode=plugin -o $${FILE%.go}.so $${FILE}	 ;\
	done

check test tests: build-testdata
	@go test -short -timeout $(TIMEOUT)s ./...
	rm *.db

integration: get build-testdata
	cd testdata/statefulIntegrationTests/s2s_rules && go get || true
	GOOS=$(GOOS) go build -buildmode=plugin -o testdata/statefulIntegrationTests/s2s_rules/cloudTrail_s2s_join.so testdata/statefulIntegrationTests/s2s_rules/cloudTrail_s2s_join.go
	GOOS=$(GOOS) go build -buildmode=plugin -o testdata/statefulIntegrationTests/agg_rules/cloudTrail_agg.so testdata/statefulIntegrationTests/agg_rules/cloudTrail_agg.go
	GOOS=$(GOOS) go build -buildmode=plugin -o testdata/statefulIntegrationTests/eventTypes/cloudTrail.so testdata/statefulIntegrationTests/eventTypes/cloudTrail.go
	go test -timeout 30s -tags=integration
	rm *.db

docker-integration:
	docker-compose up --build -d
	docker-compose run gofish make integration
	docker-compose down

build-certstream-example:
	cd examples/certstream/rules && go get || true
	GOOS=$(GOOS) go build -buildmode=plugin -o examples/certstream/rules/domain_cert_issued.so examples/certstream/rules/domain_cert_issued.go
	GOOS=$(GOOS) go build -buildmode=plugin -o examples/certstream/eventTypes/cert_stream.so examples/certstream/eventTypes/cert_stream.go

certstream-example: get build build-certstream-example 
	./go-fish -config examples/certstream/config.json
