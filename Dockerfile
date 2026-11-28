# Alpine image indexes, including the linux/amd64 and linux/arm64 manifests.
FROM docker.io/library/rust:1.100.0-alpine3.24@sha256:8d0e82abfdf9a38071e07cabc5afba4ddbe077631a80e4e742a760c38beadc33 AS build
WORKDIR /src
ARG TARGETARCH
ARG TARGETVARIANT
ENV CARGO_TARGET_DIR=/tmp/rust/target
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY apps apps
COPY crates crates
COPY fixtures fixtures
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked cargo fetch --locked
RUN --network=none \
    --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/tmp/rust/target,sharing=locked \
    if [ "$TARGETARCH/$TARGETVARIANT" = amd64/v3 ]; then export RUSTFLAGS="-C target-cpu=x86-64-v3"; fi \
    && cargo build --release --workspace --bins --frozen \
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

FROM build AS phase3-build
RUN --network=none \
    --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/tmp/rust/target,sharing=locked \
    cargo build --release --bin rfc-phase3-fixture --features phase3-fixtures --frozen \
    && cp /tmp/rust/target/release/rfc-phase3-fixture /out/

FROM build AS phase4-build
RUN --network=none \
    --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/tmp/rust/target,sharing=locked \
    cargo build --release --bin rfc-phase4-fixture --features phase4-fixtures --frozen \
    && cp /tmp/rust/target/release/rfc-phase4-fixture /out/

FROM docker.io/library/rust:1.100.0-alpine3.24@sha256:8d0e82abfdf9a38071e07cabc5afba4ddbe077631a80e4e742a760c38beadc33 AS ripgrep
WORKDIR /ripgrep
ARG TARGETARCH
ARG TARGETVARIANT
RUN apk add --no-cache make=4.4.1-r4
RUN set -eu; \
    commit=669cb4c755db20b26c3af3d923c042ae224c7496; \
    wget -q -O /tmp/rg.tar.gz "https://codeload.github.com/BurntSushi/ripgrep/tar.gz/$commit"; \
    echo '21002cba547651acf2ffb2f1e90010ee9533df3ce8819b171378d55a4f5953fd  /tmp/rg.tar.gz' | sha256sum -c -; \
    tar -xzf /tmp/rg.tar.gz --strip-components=1; \
    rm /tmp/rg.tar.gz
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked cargo fetch --locked
# An explicit target keeps static-link flags out of host procedural macros.
RUN --network=none \
    --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/tmp/rust/target,sharing=locked \
    target="$(rustc -vV | sed -n 's/^host: //p')" \
    && export RUSTFLAGS="-C target-feature=+crt-static" \
    && if [ "$TARGETARCH/$TARGETVARIANT" = amd64/v3 ]; then export RUSTFLAGS="$RUSTFLAGS -C target-cpu=x86-64-v3"; fi \
    && CARGO_PROFILE_RELEASE_STRIP=debuginfo cargo build --target "$target" --target-dir /tmp/rust/target/rg \
      --release --bin rg --no-default-features --frozen \
    && cp "/tmp/rust/target/rg/$target/release/rg" /tmp/rg

# Export the resolved Rust dependency inventories from the same build stages.
FROM build AS app-audit
RUN --network=none \
    --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    mkdir /audit \
    && cargo tree --workspace --frozen --edges normal,build --prefix none \
      --format '{p} | {l}' > /audit/packages.txt \
    && cp Cargo.lock /audit/

FROM ripgrep AS ripgrep-audit
RUN --network=none \
    --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    mkdir /audit \
    && cargo tree --frozen --no-default-features --edges normal,build --prefix none \
      --format '{p} | {l}' > /audit/packages.txt \
    && cp Cargo.lock /audit/

FROM scratch AS audit-inputs
COPY --from=app-audit /audit /application
COPY --from=ripgrep-audit /audit /ripgrep

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

FROM runtime AS phase3-fixture
COPY --from=phase3-build /out/rfc-phase3-fixture /rfc-phase3-fixture

FROM runtime AS phase4-fixture
COPY --from=phase4-build /out/rfc-phase4-fixture /rfc-phase4-fixture

# Phase 6 exercises daemon entrypoints with synthetic upstreams and crash hooks.
FROM build AS phase6-build
RUN --network=none \
    --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/tmp/rust/target,sharing=locked \
    cargo build --release --bin rfc-mcp-docker-sync --bin rfc-mcp-docker-feed \
        --features rfc-mcp-docker/phase6-fixtures,rfc-mcp-docker-sync/phase6-fixtures --frozen \
    && cp /tmp/rust/target/release/rfc-mcp-docker-sync /out/rfc-phase6-sync \
    && cp /tmp/rust/target/release/rfc-mcp-docker-feed /out/rfc-phase6-feed

FROM runtime AS phase6-fixture
COPY --from=phase6-build /out/rfc-phase6-sync /out/rfc-phase6-feed /

FROM docker.io/library/postgres:18.7-alpine3.24@sha256:0115267ab5a509022df9319c77be365cef335adb71556c75f60f5360ee247e71 AS postgres18
# Maintenance runs PostgreSQL tools directly as the postgres user.
RUN rm -f /usr/local/bin/gosu

FROM docker.io/library/postgres:19.0-alpine3.24@sha256:8b0ff4cdf221f0c53d7fcda681d3b461be725c492cd881c5ec9c0ad25023f188 AS maintenance
ARG TARGETARCH
ARG RFC_MAINTENANCE_VERSION=0.0.0
ENV RFC_MAINTENANCE_VERSION=$RFC_MAINTENANCE_VERSION
RUN set -eu; \
    case "$TARGETARCH" in \
        amd64) arch=x86_64; digest=b7d56295eab909875e724992b53ccc242fa792b40d682f303595f958a97b0224 ;; \
        arm64) arch=aarch64; digest=1b955e97abfb07f24a1d6d4b7699be8f4f55fc180fd1f8191408335c36bf603c ;; \
        *) exit 1 ;; \
    esac; \
    package=/tmp/libxml2-2.13.9-r5.apk; \
    wget -q -O "$package" "https://dl-cdn.alpinelinux.org/alpine/v3.24/main/$arch/${package##*/}"; \
    printf '%s  %s\n' "$digest" "$package" | sha256sum -c -; \
    apk --no-logfile add --no-cache python3=3.14.10-r2 b3sum=1.8.5-r0; \
    apk --no-logfile add --no-network --no-cache "$package"; \
    rm "$package"; \
    rm -f /usr/local/bin/gosu
COPY --from=postgres18 /usr/local/ /opt/postgresql18/
RUN /opt/postgresql18/bin/postgres --version
COPY migrations /migrations
COPY scripts/deploy/maintain.py /usr/local/libexec/rfc-maintain.py
USER 70:70
ENTRYPOINT ["python3", "-B", "/usr/local/libexec/rfc-maintain.py"]
