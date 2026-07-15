# syntax=docker/dockerfile:1
#
# A RUNNABLE GenICam adapter image for the two-box L2 rig.
#
# The shipped `camera-adapter/Dockerfile` deliberately excludes the `genicam` feature: GenICam needs a
# reviewed Aravis >= 0.8.36 and must not silently link a distribution's older package. This image is
# NOT that shipped artifact -- it is validation infrastructure. It links against the SAME Aravis 0.8.36
# that is built from source in `simulators/aravis_fake/Dockerfile` (supplied as ARAVIS_IMAGE), so the
# adapter under test and the fake camera it discovers share one Aravis, and it never runs on the edge
# device -- it runs on lab-5950x purely to exercise the cross-host L2 GigE path the same-container
# harness cannot reach.
#
# `edgecommons` is fetched from its pinned git rev (public), exactly as the shipped Dockerfile does --
# no `.cargo/config.toml` patch, no core-main.
ARG ARAVIS_IMAGE
FROM docker.io/library/rust@sha256:e51d0265072d2d9d5d320f6a44dde6b9ef13653b035098febd68cce8fa7c0bc4 AS rust

# ---- build: Aravis runtime + rust toolchain + the adapter, with genicam ------------------------------
FROM ${ARAVIS_IMAGE} AS build
USER root
COPY --from=rust /usr/local/cargo /usr/local/cargo
COPY --from=rust /usr/local/rustup /usr/local/rustup
ENV PATH=/usr/local/cargo/bin:$PATH \
    RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PKG_CONFIG_PATH=/opt/aravis/lib/pkgconfig \
    LD_LIBRARY_PATH=/opt/aravis/lib \
    CARGO_NET_GIT_FETCH_WITH_CLI=true
RUN rm -f /etc/apt/sources.list.d/debian.sources \
    && printf '%s\n' \
      "deb [check-valid-until=no] http://snapshot.debian.org/archive/debian/20260623T000000Z/ bookworm main" \
      "deb [check-valid-until=no] http://snapshot.debian.org/archive/debian-security/20260623T000000Z/ bookworm-security main" \
      > /etc/apt/sources.list \
    && apt-get -o Acquire::Check-Valid-Until=false update \
    && apt-get install -y --no-install-recommends \
      build-essential libglib2.0-dev libusb-1.0-0-dev libxml2-dev pkg-config git ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /build/camera-adapter
# `Cargo.lock` is untracked (see the shipped Dockerfile); a normal Docker build context is writable, so
# cargo simply resolves fresh here -- no `--locked`, no lock gymnastics.
COPY Cargo.toml ./
COPY src ./src
COPY native ./native
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/build/camera-adapter/target \
    cargo build --release --no-default-features --features standalone,onvif,genicam \
    && install -D -m0755 target/release/camera-adapter /out/camera-adapter \
    && install -D -m0755 target/release/camera-adapter-genicam-discover /out/camera-adapter-genicam-discover

# ---- runtime: Aravis runtime libs + the two binaries -------------------------------------------------
FROM ${ARAVIS_IMAGE} AS runtime
USER root
COPY --from=build /out/camera-adapter /usr/local/bin/camera-adapter
COPY --from=build /out/camera-adapter-genicam-discover /usr/local/bin/camera-adapter-genicam-discover
ENV LD_LIBRARY_PATH=/opt/aravis/lib
# Default to the discovery probe; the run scripts override for a full adapter run.
ENTRYPOINT ["/usr/local/bin/camera-adapter-genicam-discover"]
