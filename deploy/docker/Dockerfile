# Build stage
FROM rust:bookworm AS builder

WORKDIR /app
COPY . .
RUN cargo build --release --bin spacetime-db-ha

# Runtime stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/spacetime-db-ha /usr/local/bin/spacetime-db-ha

EXPOSE 3001 4001

ENTRYPOINT ["spacetime-db-ha"]
