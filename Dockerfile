# Dockerfile for alkanes-contract-indexer

# Stage 1: Build the binary
FROM rust:latest as builder

WORKDIR /usr/src/alkanes-contract-indexer

# Install build dependencies
RUN apt-get update && apt-get install -y libpq-dev && rm -rf /var/lib/apt/lists/*

ENV CARGO_HOME=/usr/local/cargo
RUN cargo install sqlx-cli

# Copy source code
COPY . .

# Build the project
RUN cargo build --release

# Stage 2: Create the final image
FROM ubuntu:latest

# Install runtime dependencies and postgresql-client for pg_isready
RUN apt-get update && apt-get install -y libpq-dev postgresql-client curl && rm -rf /var/lib/apt/lists/*

# Copy the built binary and sqlx-cli from the builder stage
COPY --from=builder /usr/src/alkanes-contract-indexer/target/release/alkanes-contract-indexer /usr/local/bin/alkanes-contract-indexer
COPY --from=builder /usr/local/cargo/bin/sqlx /usr/local/bin/sqlx

# Copy the entrypoint script
COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Copy migrations
COPY ./migrations /migrations

WORKDIR /app

# Set the entrypoint
ENTRYPOINT ["docker-entrypoint.sh"]

# The default command to run when the container starts
CMD ["alkanes-contract-indexer"]
