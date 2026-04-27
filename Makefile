.PHONY: build test test-e2e-postgres lint docker-up docker-down run debug-dashboard scale-test mega-scale-test generate-mega-fixtures lint-store-imports

build:
	go build ./...

test:
	go test ./...

lint:
	golangci-lint run

docker-up:
	podman-compose up -d

docker-down:
	podman-compose down

run:
	go run ./cmd/merkle-service

debug-dashboard:
	go run ./tools/debug-dashboard

scale-test:
	go test -tags scale -v -count=1 -timeout 10m ./test/scale/

mega-scale-test:
	go test -tags scale -v -count=1 -timeout 15m -run TestScaleMega ./test/scale/

generate-mega-fixtures:
	cd test/scale && go run ./cmd/generate-fixtures/ --instances 100 --txids-per-instance 10000 --subtrees 250 --txids-per-subtree 4000 --out testdata-mega --seed 42

# Runs the PostgreSQL-backed e2e suite. Requires Docker on the host; spins up
# a disposable postgres:16-alpine container per test via testcontainers.
# Some environments (e.g., systems without a docker bridge network) need the
# reaper disabled — set TESTCONTAINERS_RYUK_DISABLED=true if the run fails at
# reaper startup.
test-e2e-postgres:
	go test -tags=e2e_postgres -count=1 -timeout=10m ./internal/e2e/...

# Fails if any cmd/ binary imports a backend implementation package directly,
# which would defeat the interface abstraction.
lint-store-imports:
	@if grep -rE 'internal/store/(aerospike|sql)"' cmd/ | grep -v '^Binary' | grep -v '_ "'; then \
		echo "ERROR: cmd/ must import backend packages only as blank (_) imports for side-effect registration"; \
		exit 1; \
	fi
	@echo "ok: cmd/ store imports look clean"
