GO ?= go

.PHONY: help build test race lint experiment demo clean

help:
	@echo "make test        unit and integration tests, with the race detector"
	@echo "make experiment  the load sweep, the latency fault, the capacity swing"
	@echo "make demo        upstream + gateway + a burst of load, in one command"

build:
	$(GO) build -o bin/ ./cmd/...

test:
	$(GO) vet ./...
	$(GO) test -race -count=1 ./...

race: test

lint:
	gofmt -l . | tee /dev/stderr | (! read)
	$(GO) vet ./...

experiment: build
	./bin/ballast-bench -duration 5s -scenario 24s

demo: build
	@./bin/ballast-upstream -workers 8 -fast 25ms -slow 120ms & echo $$! > /tmp/ballast-up.pid; \
	sleep 1; \
	./bin/ballast -upstream http://127.0.0.1:9101 -limiter gradient & echo $$! > /tmp/ballast-gw.pid; \
	sleep 1; \
	./bin/ballast-load -rate 2000 -duration 15s; \
	echo; echo "--- gateway snapshot ---"; curl -s localhost:8081/snapshot; echo; \
	kill `cat /tmp/ballast-gw.pid` `cat /tmp/ballast-up.pid` 2>/dev/null || true

clean:
	rm -rf bin
