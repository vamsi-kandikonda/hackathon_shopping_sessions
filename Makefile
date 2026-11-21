IMAGE ?= shopping-sessions:latest
CPUS  ?= 8
MEM   ?= 24g
PLATFORM ?= linux/amd64
DEV   ?= .local/dev
ANSWER ?= .local/dev-answer
FIXTURES ?= fixtures
export IMAGE CPUS MEM PLATFORM

.PHONY: build build-native fixtures fixtures-native dev run-dev run-dev-native check-dev

build:
	docker buildx build --platform $(PLATFORM) --load -t $(IMAGE) .

build-native:
	cargo build --release

fixtures: build
	uv run --with polars python3 scripts/run_fixtures.py $(FIXTURES) -- scripts/docker_run.sh

fixtures-native: build-native
	uv run --with polars python3 scripts/run_fixtures.py $(FIXTURES) -- ./target/release/shopping-sessions

dev:
	.local/generator/target/release/generate --seed 1 --events 1000000 --clicks 250000 --output $(DEV)

run-dev: build
	rm -rf out && time scripts/docker_run.sh $(DEV) out

run-dev-native: build-native
	rm -rf out && time ./target/release/shopping-sessions $(DEV) out

check-dev:
	uv run --with polars python3 scripts/compare.py out $(ANSWER)
