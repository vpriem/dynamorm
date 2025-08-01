GOBIN ?= $$(go env GOPATH)/bin

.PHONY: install-mockgen
install-mockgen:
	@if [ ! -f "${GOBIN}/mockgen" ]; then \
		echo "Installing mockgen..."; \
		go install go.uber.org/mock/mockgen@latest; \
	fi

.PHONY: install-go-test-coverage
install-go-test-coverage:
	@if [ ! -f "${GOBIN}/go-test-coverage" ]; then \
		echo "Installing go-test-coverage..."; \
		go install github.com/vladopajic/go-test-coverage/v2@latest; \
	fi

.PHONY: test
test:
	@echo "Running all tests..."
	go test ./...

.PHONY: coverage
coverage: install-go-test-coverage
	@echo "Running all tests with coverage..."
	go test ./... -coverprofile=./coverage.out -covermode=atomic -coverpkg=./...
	${GOBIN}/go-test-coverage --config=./.testcoverage.yml

.PHONY: gen
gen: install-mockgen
	@echo "Running go generate..."
	go generate ./...

.PHONY: lint
lint:
	@echo "Running linter..."
	 golangci-lint run
