# syntax=docker/dockerfile:1
#
# Build from this repository:
#   docker build -t camera-adapter:dev .
#
# The context is the adapter alone. `edgecommons` is a GIT dependency pinned by REV (Cargo.toml), so
# cargo fetches exactly one library source and the image needs no sibling checkout. The `[patch]` that
# redirects it to a local core tree lives in a gitignored `.cargo/config.toml` and is a DEVELOPER
# convenience -- it is never part of an image, and an image must never be built from an unpinned
# working tree.
#
# This file intentionally provides ONVIF and RTSP targets only. GenICam requires a reviewed,
# architecture-matched Aravis >= 0.8.36 package and is deployed through the native Linux path,
# not by silently relying on a distribution's older Aravis package.
#
# The two base references are reviewed Linux/amd64 digests. Publish a separately reviewed arm64
# image before claiming container support on arm64; a single-architecture digest must not be
# misrepresented as a multi-architecture release.

FROM docker.io/library/rust@sha256:3490aa77d179a59d67e94239cca96dd84030b564470859200f535b942bdffedf AS build

RUN rm -f /etc/apt/sources.list.d/debian.sources \
    && printf '%s\n' \
      "deb [check-valid-until=no] http://snapshot.debian.org/archive/debian/20260623T000000Z/ bookworm main" \
      "deb [check-valid-until=no] http://snapshot.debian.org/archive/debian-security/20260623T000000Z/ bookworm-security main" \
      > /etc/apt/sources.list \
    && apt-get -o Acquire::Check-Valid-Until=false update \
    && apt-get install -y --no-install-recommends \
      ca-certificates \
      pkg-config \
      libgstreamer1.0-dev \
      libgstreamer-plugins-base1.0-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy only source inputs, including the committed `Cargo.lock`.
#
# `Cargo.lock` is committed and records the git source `edgecommons` is pinned to by REV in Cargo.toml
# (it is regenerated on a fresh clone with the local `.cargo` `[patch]` inactive, so it never carries a
# path override). The image build sees no `.cargo` override, so `--locked` resolves the pinned rev
# reproducibly from the lock.
COPY Cargo.toml Cargo.lock /build/camera-adapter/
COPY src /build/camera-adapter/src
COPY native /build/camera-adapter/native

WORKDIR /build/camera-adapter

FROM build AS build-onvif
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/build/target \
    CARGO_TARGET_DIR=/build/target \
    cargo build --release --locked --no-default-features --features standalone,onvif \
    && install -D -m 0755 /build/target/release/camera-adapter /build/artifacts/camera-adapter

FROM build AS build-rtsp
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/build/target \
    CARGO_TARGET_DIR=/build/target \
    cargo build --release --locked --no-default-features --features standalone,onvif,rtsp \
    && install -D -m 0755 /build/target/release/camera-adapter /build/artifacts/camera-adapter

FROM docker.io/library/debian@sha256:1def178129dfb5f24db43afbf2fcac04530012e3264ba4ff81c71184e17a9ee4 AS runtime-base

RUN rm -f /etc/apt/sources.list.d/debian.sources \
    && printf '%s\n' \
      "deb [check-valid-until=no] http://snapshot.debian.org/archive/debian/20260623T000000Z/ bookworm main" \
      "deb [check-valid-until=no] http://snapshot.debian.org/archive/debian-security/20260623T000000Z/ bookworm-security main" \
      > /etc/apt/sources.list \
    && apt-get -o Acquire::Check-Valid-Until=false update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build-onvif /build/artifacts/camera-adapter /usr/local/bin/camera-adapter
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/camera-adapter"]

# Default image: standalone ONVIF snapshot path.
FROM runtime-base AS onvif

FROM docker.io/library/debian@sha256:1def178129dfb5f24db43afbf2fcac04530012e3264ba4ff81c71184e17a9ee4 AS rtsp

RUN rm -f /etc/apt/sources.list.d/debian.sources \
    && printf '%s\n' \
      "deb [check-valid-until=no] http://snapshot.debian.org/archive/debian/20260623T000000Z/ bookworm main" \
      "deb [check-valid-until=no] http://snapshot.debian.org/archive/debian-security/20260623T000000Z/ bookworm-security main" \
      > /etc/apt/sources.list \
    && apt-get -o Acquire::Check-Valid-Until=false update \
    && apt-get install -y --no-install-recommends \
      ca-certificates \
      gstreamer1.0-plugins-base \
      gstreamer1.0-plugins-good \
      gstreamer1.0-plugins-bad \
      gstreamer1.0-libav \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build-rtsp /build/artifacts/camera-adapter /usr/local/bin/camera-adapter
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/camera-adapter"]

# Keep the default Docker target small and deterministic: `docker build ...` yields the ONVIF
# snapshot image. Select `--target rtsp` only when the deployment actually needs RTSP frames.
FROM onvif AS runtime
