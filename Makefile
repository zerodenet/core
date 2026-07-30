.DEFAULT_GOAL := help

CARGO ?= cargo
CONFIG ?= config.json
STATUS_LISTEN ?= 127.0.0.1:9090
PREFIX ?= /usr/local
BUILD_ID := $(shell grep -m1 'version = ' Cargo.toml | sed 's/.*= *"//;s/".*//')

.PHONY: help fmt check test clippy build build-full release release-full strip clean install uninstall run run-status status status-json build-info hooks

help:
	@echo Available targets:
	@echo   make fmt          - cargo fmt --all
	@echo   make check        - cargo check --workspace
	@echo   make test         - cargo test --workspace
	@echo   make clippy       - cargo clippy --workspace --all-targets
	@echo   make build        - cargo build
	@echo   make build-full   - cargo build --features full,status-api
	@echo   make release      - cargo build --release
	@echo   make release-full - cargo build --release --features full,status-api
	@echo   make strip        - strip release binary (减小文件大小)
	@echo   make clean        - cargo clean
	@echo   make install      - install to $(PREFIX)/bin
	@echo   make uninstall    - remove from $(PREFIX)/bin
	@echo   make run          - run zero with CONFIG=$(CONFIG)
	@echo   make run-status   - run zero with local status endpoint
	@echo   make status       - print text status for CONFIG=$(CONFIG)
	@echo   make status-json  - print JSON status for CONFIG=$(CONFIG)
	@echo   make build-info  - show build info
	@echo   make hooks       - install git hooks (pre-commit: fmt + clippy)

fmt:
	$(CARGO) fmt --all

check:
	$(CARGO) check --workspace

test:
	$(CARGO) test --workspace

clippy:
	$(CARGO) clippy --workspace --all-targets

build:
	$(CARGO) build

build-full:
	$(CARGO) build --features full,status-api

release:
	$(CARGO) build --release

release-full:
	$(CARGO) build --release --features full,status-api

strip:
	strip target/release/zero

clean:
	$(CARGO) clean

install: release-full strip
	install -m 755 target/release/zero $(PREFIX)/bin/zero

uninstall:
	rm -f $(PREFIX)/bin/zero

run:
	$(CARGO) run -- run $(CONFIG)

run-status:
	$(CARGO) run -- run --status-listen $(STATUS_LISTEN) $(CONFIG)

status:
	$(CARGO) run -- status $(CONFIG)

status-json:
	$(CARGO) run -- status --json $(CONFIG)

build-info:
	@echo $(BUILD_ID)

# ── Documentation ──────────────────────────────────────────────────────

hooks:
	@git config core.hooksPath scripts/hooks
	@echo "git hooks -> scripts/hooks (pre-commit: fmt + clippy)"
