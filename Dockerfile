# Alpine image indexes, including the linux/amd64 and linux/arm64 manifests.
FROM docker.io/library/rust:1.99.1-alpine3.24@sha256:44819b27be4c97b0e4d7203bd2e02c4820f97700211b6a37fb4c246a2de59383 AS build
WORKDIR /src
ENV CARGO_TARGET_DIR=/tmp/rust/target
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY apps apps
COPY crates crates
COPY fixtures fixtures
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked cargo fetch --locked
RUN --network=none \
    --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/tmp/rust/target,sharing=locked \
    cargo build --release --workspace --bins --frozen \
    && mkdir /out \
    && cp /tmp/rust/target/release/rfc-mcp-docker-serve \
          /tmp/rust/target/release/rfc-mcp-docker-sync \
          /tmp/rust/target/release/rfc-mcp-docker-feed /out/

# The fixture driver is excluded from service images.
FROM build AS phase1-build
RUN --network=none \
    --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/tmp/rust/target,sharing=locked \
    cargo build --release --bin rfc-phase1-fixture --features phase1-fixtures --frozen \
    && cp /tmp/rust/target/release/rfc-phase1-fixture /out/

FROM docker.io/library/alpine:3.24@sha256:f59abe656b692d901e184be3445e084786602108599d2a51d229c0f5a48c71bf AS ripgrep
ARG TARGETARCH
RUN set -eu; \
    case "$TARGETARCH" in \
        amd64) arch=x86_64; sha=33e15bcf1624b25cdd2a55813a47a2f95dbe126268203e76aa6a585d1e7b149c ;; \
        arm64) arch=aarch64; sha=800b1e7206afe799dfb5a6901f23147cfaabe0e52210538100f61e86e1740915 ;; \
        *) exit 1 ;; \
    esac; \
    release="ripgrep-15.2.0-$arch-unknown-linux-musl"; \
    wget -q -O /tmp/rg.tar.gz "https://github.com/BurntSushi/ripgrep/releases/download/15.2.0/$release.tar.gz"; \
    echo "$sha  /tmp/rg.tar.gz" | sha256sum -c -; \
    tar -xzf /tmp/rg.tar.gz -C /tmp; \
    cp "/tmp/$release/rg" /tmp/rg

FROM docker.io/library/alpine:3.24@sha256:f59abe656b692d901e184be3445e084786602108599d2a51d229c0f5a48c71bf AS runtime

FROM runtime AS serve
COPY --from=build /out/rfc-mcp-docker-serve /usr/local/bin/
COPY --from=ripgrep /tmp/rg /usr/local/bin/rg
USER 10001:10001
ENTRYPOINT ["/usr/local/bin/rfc-mcp-docker-serve"]

FROM runtime AS sync
RUN apk add --no-cache rsync=3.5.2-r1
COPY --from=build /out/rfc-mcp-docker-sync /usr/local/bin/
USER 10002:10002
ENTRYPOINT ["/usr/local/bin/rfc-mcp-docker-sync"]

# Disposable upstream fixture tools used only by the integration harness.
FROM sync AS phase0-fixtures
USER root
RUN apk add --no-cache openssl python3
USER 10004:10004

FROM runtime AS feed
COPY --from=build /out/rfc-mcp-docker-feed /usr/local/bin/
USER 10003:10003
ENTRYPOINT ["/usr/local/bin/rfc-mcp-docker-feed"]

FROM runtime AS phase1-fixture
COPY --from=phase1-build /out/rfc-phase1-fixture /rfc-phase1-fixture
