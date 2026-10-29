.PHONY: all engine engine-test node gw bin clean demo mesh-demo test test-race fuzz-smoke chaos fmt
# Docker: `docker compose up --build` (Linux; see README). Native macOS cluster: `make demo`.

ROOT      := $(abspath .)
BIN       := $(ROOT)/bin
ENGINE    := $(ROOT)/kvengine/libkvengine.a
NODE_BIN  := $(ROOT)/kvnode/target/release/kvnode

CFLAGS    ?= -std=c11 -Wall -Wextra -O2

all: engine node gw

engine: $(ENGINE)

$(ENGINE): kvengine/kvengine.c kvengine/kvengine.h
	$(CC) $(CFLAGS) -c kvengine/kvengine.c -o kvengine/kvengine.o
	ar rcs $(ENGINE) kvengine/kvengine.o

engine-test: $(ENGINE)
	$(CC) $(CFLAGS) kvengine/kvengine_test.c $(ENGINE) -o kvengine/kvengine_test
	./kvengine/kvengine_test

node: $(ENGINE)
	cd kvnode && cargo build --release
	mkdir -p $(BIN)
	cp $(NODE_BIN) $(BIN)/kvnode

gw:
	mkdir -p $(BIN)
	cd kvgw && go build -o $(BIN)/gateway ./cmd/gateway
	cd kvgw && go build -o $(BIN)/kvstore ./cmd/kvstore
	cd kvgw && go build -o $(BIN)/kvmesh ./cmd/kvmesh
	cd kvgw && go build -o $(BIN)/ktpcat ./cmd/ktpcat

test: engine-test
	cd kvnode && cargo test
	cd kvgw && go test -short ./...
	cd kvgw && go vet ./...
	node clients/kv_test.js

# The runtime and networking stack under the race detector.
test-race:
	cd kvgw && go test -race -short ./netstack/... ./runtime/... ./internal/...

# A few seconds of coverage-guided fuzzing per network-facing decoder.
FUZZTIME ?= 10s
fuzz-smoke:
	cd kvgw && ./scripts/fuzz-smoke.sh $(FUZZTIME)

# The seeded fault-injection suites at full size (the default test run uses
# -short): Raft chaos (120 seeds), SWIM partitions and loss, KTP transfers
# across the loss matrix, actors and the whole mesh on simnet.
chaos:
	cd kvgw && go test -count=1 ./runtime/... ./netstack/...

demo: all
	./scripts/demo.sh

mesh-demo: all
	./scripts/mesh-demo.sh

clean:
	rm -rf $(BIN)
	rm -f  kvengine/*.o kvengine/*.a kvengine/kvengine_test
	rm -rf kvnode/target
	rm -f  /tmp/kvstore-node-*.db
	rm -f  /tmp/kvstore_kv_test.db
