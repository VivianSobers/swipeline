GO_IMAGE := golang:1.27.2
GO_RUN = docker run --rm --user "$$(id -u):$$(id -g)" -e GOCACHE=/tmp/go-cache -e GOMODCACHE=/tmp/go-mod -e GOFLAGS=-buildvcs=false -v "$(CURDIR):/src:ro" -w /src $(GO_IMAGE)
.PHONY: check test fmt build
check:
	$(GO_RUN) sh -ec 'test -z "$$(gofmt -l cmd internal schemas tests 2>/dev/null)"; go vet ./...; go test -race ./...'
test:
	$(GO_RUN) go test -race ./...
fmt:
	docker run --rm --user "$$(id -u):$$(id -g)" -v "$(CURDIR):/src" -w /src $(GO_IMAGE) sh -ec 'find . -name "*.go" -not -path "./.git/*" -exec gofmt -w {} +'
build:
	$(GO_RUN) go build ./...
.PHONY: deploy-check
deploy-check:
	@set -eu; config_file=$$(mktemp); trap 'rm -f "$$config_file"' EXIT; docker compose --profile core config --format json > "$$config_file"; docker run --rm --user "$$(id -u):$$(id -g)" -e GOCACHE=/tmp/go-cache -e GOFLAGS=-buildvcs=false -e SWIPELINE_COMPOSE_JSON=/compose.json -v "$(CURDIR):/src:ro" -v "$$config_file:/compose.json:ro" -w /src $(GO_IMAGE) go test ./tests -run TestRenderedComposeBudgetsAndScope -v
	docker compose run --rm --no-deps --entrypoint /bin/promtool prometheus check config /etc/prometheus/prometheus.yml
.PHONY: demo
demo:
	@set -eu; project=swipeline-m1-test-$$$$; export SWIPELINE_HTTP_PORT=0 SWIPELINE_PROM_PORT=0; trap 'docker compose -p "$$project" down -v --remove-orphans' EXIT; docker compose -p "$$project" up -d --build --wait; docker run --rm --network "$${project}_default" --user "$$(id -u):$$(id -g)" -e GOCACHE=/tmp/go-cache -e GOFLAGS=-buildvcs=false -e SWIPELINE_TEST_URL=http://replayer:8080 -e SWIPELINE_TEST_PROM_URL=http://prometheus:9090 -v "$(CURDIR):/src:ro" -w /src $(GO_IMAGE) go test ./tests -run TestInstalledPreviewAndPrometheus -count=1 -v
