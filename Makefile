.PHONY: test test-docker build clean

# Run tests locally (requires Go and redis-server on PATH)
test:
	go test ./...

# Run tests in Docker (no local dependencies needed)
test-docker:
	docker compose -f docker-compose.test.yml run --rm --build test

# Build the Faktory binary
build:
	go build -o bin/faktory ./cmd/faktory

clean:
	rm -rf bin/
	docker compose -f docker-compose.test.yml down -v --remove-orphans 2>/dev/null || true
