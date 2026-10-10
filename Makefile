GO_IMAGE := golang:1.27.2
GO_RUN = docker run --rm --user "$$(id -u):$$(id -g)" -e GOCACHE=/tmp/go-cache -e GOMODCACHE=/tmp/go-mod -e GOFLAGS=-buildvcs=false -v "$(CURDIR):/src:ro,z" -w /src $(GO_IMAGE)
.PHONY: check test fmt build
check:
	$(GO_RUN) sh -ec 'test -z "$$(gofmt -l cmd internal schemas tests 2>/dev/null)"; go vet ./...; go test -race ./...'
test:
	$(GO_RUN) go test -race ./...
fmt:
	docker run --rm --user "$$(id -u):$$(id -g)" -v "$(CURDIR):/src:z" -w /src $(GO_IMAGE) sh -ec 'find . -name "*.go" -not -path "./.git/*" -exec gofmt -w {} +'
build:
	$(GO_RUN) go build ./...
.PHONY: deploy-check
deploy-check:
	@set -eu; config_file=$$(mktemp); trap 'rm -f "$$config_file"' EXIT; docker compose --profile core config --format json > "$$config_file"; docker run --rm --user "$$(id -u):$$(id -g)" -e GOCACHE=/tmp/go-cache -e GOFLAGS=-buildvcs=false -e SWIPELINE_COMPOSE_JSON=/compose.json -v "$(CURDIR):/src:ro,z" -v "$$config_file:/compose.json:ro,z" -w /src $(GO_IMAGE) go test ./tests -run TestRendered -v
	docker compose run --rm --no-deps --entrypoint /bin/promtool prometheus check config /etc/prometheus/prometheus.yml
.PHONY: demo
demo:
	@set -eu; project=swipeline-m1-test-$$$$; export SWIPELINE_HTTP_PORT=0 SWIPELINE_PROM_PORT=0; trap 'docker compose -p "$$project" down -v --remove-orphans' EXIT; docker compose -p "$$project" up -d --build --wait; docker run --rm --network "$${project}_default" --user "$$(id -u):$$(id -g)" -e GOCACHE=/tmp/go-cache -e GOFLAGS=-buildvcs=false -e SWIPELINE_TEST_URL=http://replayer:8080 -e SWIPELINE_TEST_PROM_URL=http://prometheus:9090 -v "$(CURDIR):/src:ro,z" -w /src $(GO_IMAGE) go test ./tests -run TestInstalledPreviewAndPrometheus -count=1 -v
.PHONY: m2-storage
m2-storage:
	docker compose up -d --wait postgres kafka
	docker compose exec -T kafka bash /opt/swipeline/topics.sh
.PHONY: db-test
db-test:
	@set -eu; container=$$(docker compose ps -q postgres); test -n "$$container" || { echo "run make m2-storage first" >&2; exit 1; }; network=$$(docker inspect "$$container" --format '{{range $$k,$$v := .NetworkSettings.Networks}}{{$$k}}{{end}}'); docker run --rm --network "$$network" --user "$$(id -u):$$(id -g)" -e GOCACHE=/tmp/go-cache -e GOMODCACHE=/tmp/go-mod -e GOFLAGS=-buildvcs=false -e SWIPELINE_DB='postgres://gateway:gateway-local@postgres:5432/gateway?sslmode=disable' -e SWIPELINE_KAFKA=kafka:9092 -v "$(CURDIR):/src:ro,z" -w /src $(GO_IMAGE) go test -race -p 1 ./... -count=1
.PHONY: streaming-check
streaming-check:
	docker build -f deploy/streaming.Dockerfile -t swipeline-streaming:m2 .
.PHONY: m2-demo
m2-demo:
	@set -eu; project=swipeline-m2-demo-$$$$; export SWIPELINE_HTTP_PORT=0 SWIPELINE_PROM_PORT=0 SWIPELINE_GATEWAY_PORT=0 SWIPELINE_FLINK_PORT=0; trap 'rc=$$?; if [ $$rc -ne 0 ]; then docker compose -p "$$project" logs --no-color --tail 200; fi; docker compose -p "$$project" down -v --remove-orphans; exit $$rc' EXIT; docker compose -p "$$project" up -d --build --wait; ready=0; for attempt in $$(seq 1 60); do if docker compose -p "$$project" exec -T flink-jobmanager curl -fsS --max-time 2 http://gateway:8081/readyz; then ready=1; break; fi; sleep 1; done; test "$$ready" = 1; docker run --rm --memory=768m --cpus=2 --network "$${project}_default" --user "$$(id -u):$$(id -g)" -e GOMEMLIMIT=512MiB -e GOCACHE=/tmp/go-cache -e GOMODCACHE=/tmp/go-mod -e GOFLAGS=-buildvcs=false -e SWIPELINE_TEST_GATEWAY=http://gateway:8081 -v "$(CURDIR):/src:ro,z" -w /src $(GO_IMAGE) go test ./tests -run '^TestInstalledAuthorization$$' -count=1 -v
