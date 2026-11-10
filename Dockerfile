ARG RUST_IMAGE=rust:1.99.1-bookworm
ARG RUNTIME_IMAGE=debian:bookworm-slim

FROM ${RUST_IMAGE} AS build
WORKDIR /build
COPY minivectordb/Cargo.toml minivectordb/Cargo.lock ./
COPY minivectordb/src ./src
RUN cargo build --release --locked

FROM ${RUNTIME_IMAGE}
RUN mkdir /data
COPY --from=build /build/target/release/minivectordb /usr/local/bin/minivectordb
VOLUME ["/data"]
ENTRYPOINT ["/usr/local/bin/minivectordb"]
