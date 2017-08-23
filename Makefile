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
	@go test -short -timeout $(TIMEOUT)s ./...

integration: get
	cd testdata/statefulIntegrationTests/s2s_rules && go get || true
	GOOS=$(GOOS) go build -buildmode=plugin -o testdata/statefulIntegrationTests/s2s_rules/cloudTrail_s2s_join.so testdata/statefulIntegrationTests/s2s_rules/cloudTrail_s2s_join.go
	GOOS=$(GOOS) go build -buildmode=plugin -o testdata/statefulIntegrationTests/agg_rules/cloudTrail_agg.so testdata/statefulIntegrationTests/agg_rules/cloudTrail_agg.go
	GOOS=$(GOOS) go build -buildmode=plugin -o testdata/statefulIntegrationTests/eventTypes/cloudTrail.so testdata/statefulIntegrationTests/eventTypes/cloudTrail.go
	go test -timeout 30s -run Integration
