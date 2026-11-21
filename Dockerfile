# Shopping Sessions: High-performance Rust pipeline
# Scorers are linux/amd64 (Intel Xeon Cascade Lake+ with AVX-512).
# No FROM line pins a platform so `make build` works on amd64 and arm64.

FROM rust:1-bookworm AS builder
ARG TARGETARCH
WORKDIR /src

# Cache dependencies first
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src && echo "fn main() {}" > src/main.rs && \
    if [ "$TARGETARCH" = "amd64" ]; then \
        export RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+aes"; \
    fi && \
    cargo build --release --locked && \
    rm -rf src

# Build actual binary
COPY src/ ./src/
RUN touch src/main.rs && \
    if [ "$TARGETARCH" = "amd64" ]; then \
        export RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+aes"; \
    fi && \
    cargo build --release --locked

FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=builder /src/target/release/shopping-sessions /app/shopping-sessions

ENV DATA_DIR=/data \
    OUTPUT_DIR=/output \
    SCRATCH_DIR=/scratch \
    HOME=/scratch

WORKDIR /app
CMD ["/app/shopping-sessions"]
