FROM rust:alpine AS builder
RUN apk add --no-cache musl-dev
WORKDIR /app

COPY Cargo.toml Cargo.lock* ./
RUN mkdir src templates && \
    echo "fn main() {}" > src/main.rs && \
    touch templates/alert.html && \
    cargo build --release && \
    rm -rf src templates

COPY src ./src
COPY templates ./templates
RUN touch src/main.rs && cargo build --release

FROM scratch
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /app/target/release/docker-autoheal-rs /docker-autoheal-rs

ENTRYPOINT ["/docker-autoheal-rs"]
