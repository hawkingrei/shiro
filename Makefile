.PHONY: help build test fmt lint tidy report web report-web show report-show

help:
	@echo "Targets:"
	@echo "  build   - Build shiro CLI"
	@echo "  test    - Run Go tests"
	@echo "  fmt     - Format Go code"
	@echo "  lint    - Run golangci-lint"
	@echo "  tidy    - Run go mod tidy"
	@echo "  report  - Generate report.json for web UI"
	@echo "  web     - Build static web UI (Next.js)"
	@echo "  report-web - Generate report.json and build web UI"
	@echo "  report-show - Generate report.json and show web UI"

build:
	mkdir -p bin
	go build -o bin/shiro ./cmd/shiro

test:
	go test ./...

fmt:
	gofmt -w .

lint:
	golangci-lint run

tidy:
	go mod tidy

report:
	go run ./cmd/shiro-report -output web/public

web:
	cd web && npm install && npm run build

show:
	cd web && npm install && npm run dev

report-web: report web

report-show: report show
