# Ephemeral native validation image. It deliberately starts from the exact fake-camera runtime
# image so cargo links against the same Aravis 0.8.36 installation that provides L2 evidence.
# The invoking runner must supply a freshly tagged, hash-named local image reference after it has
# built the pinned fake-camera Dockerfile; this Dockerfile never pulls an implicit mutable base.
ARG ARAVIS_RUNTIME_IMAGE=scratch
FROM docker.io/library/rust@sha256:e51d0265072d2d9d5d320f6a44dde6b9ef13653b035098febd68cce8fa7c0bc4 AS rust

FROM ${ARAVIS_RUNTIME_IMAGE}

ARG CARGO_LLVM_COV_VERSION=0.8.7
ARG LLVM_COV_TOOLCHAIN=1.87.0
ARG GREENGRASS_TOOLCHAIN=1.90.0

USER root
COPY --from=rust /usr/local/cargo /usr/local/cargo
COPY --from=rust /usr/local/rustup /usr/local/rustup
ENV PATH=/usr/local/cargo/bin:$PATH \
    RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo
RUN rm -f /etc/apt/sources.list.d/debian.sources \
    && printf '%s\n' \
      "deb [check-valid-until=no] http://snapshot.debian.org/archive/debian/20260623T000000Z/ bookworm main" \
      "deb [check-valid-until=no] http://snapshot.debian.org/archive/debian-security/20260623T000000Z/ bookworm-security main" \
      > /etc/apt/sources.list \
    && apt-get -o Acquire::Check-Valid-Until=false update \
    && apt-get install -y --no-install-recommends build-essential libglib2.0-dev libusb-1.0-0-dev libxml2-dev pkg-config \
      git ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && rustup toolchain install "${LLVM_COV_TOOLCHAIN}" --profile minimal --component llvm-tools-preview \
    && rustup toolchain install "${GREENGRASS_TOOLCHAIN}" --profile minimal \
    && cargo +"${LLVM_COV_TOOLCHAIN}" install --locked cargo-llvm-cov --version "${CARGO_LLVM_COV_VERSION}"

ENV PATH=/usr/local/cargo/bin:$PATH \
    RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PKG_CONFIG_PATH=/opt/aravis/lib/pkgconfig \
    LD_LIBRARY_PATH=/opt/aravis/lib
WORKDIR /workspace
ENTRYPOINT ["cargo"]
