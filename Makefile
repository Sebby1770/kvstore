.PHONY: all engine engine-test node gw bin clean demo test fmt
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

test: engine-test
	cd kvnode && cargo test
	cd kvgw && go test ./...
	cd kvgw && go vet ./...

demo: all
	./scripts/demo.sh

clean:
	rm -rf $(BIN)
	rm -f  kvengine/*.o kvengine/*.a kvengine/kvengine_test
	rm -rf kvnode/target
	rm -f  /tmp/kvstore-node-*.db
	rm -f  /tmp/kvstore_kv_test.db
