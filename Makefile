.DEFAULT_GOAL := all

all: fmt lint test

fmt:
	go fmt $$(go list ./...)

lint:
	golangci-lint run

test:
	go test -v -race -run ^Test -parallel=8 ./...

test-acc:
	go test -timeout 30s -run ^Example ./

.PHONY: fmt lint test