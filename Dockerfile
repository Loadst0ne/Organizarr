# Multi-stage build: compile a release binary, then ship it in a slim
# runtime image. TLS is provided by rustls (compiled in), so no OpenSSL
# is needed at runtime.
FROM rust:1-slim AS build
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /app/target/release/organizarr /usr/local/bin/organizarr

# The tool reads config.yaml from (and writes its log to) the working
# directory; mount a volume here.
WORKDIR /config
ENTRYPOINT ["/usr/local/bin/organizarr"]
