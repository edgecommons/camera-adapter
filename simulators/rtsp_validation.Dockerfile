# Ephemeral native RTSP validation image. It compiles the Rust bindings against
# the same Debian snapshot on every run and joins the pinned MediaMTX Compose
# network; it is test infrastructure and is never shipped as an adapter image.
FROM docker.io/library/rust@sha256:e51d0265072d2d9d5d320f6a44dde6b9ef13653b035098febd68cce8fa7c0bc4

RUN rm -f /etc/apt/sources.list.d/debian.sources \
    && printf '%s\n' \
      "deb [check-valid-until=no] http://snapshot.debian.org/archive/debian/20260623T000000Z/ bookworm main" \
      "deb [check-valid-until=no] http://snapshot.debian.org/archive/debian-security/20260623T000000Z/ bookworm-security main" \
      > /etc/apt/sources.list \
    && apt-get -o Acquire::Check-Valid-Until=false update \
    && apt-get install -y --no-install-recommends \
      pkg-config \
      libgstreamer1.0-dev \
      libgstreamer-plugins-base1.0-dev \
      gstreamer1.0-tools \
      gstreamer1.0-plugins-base \
      gstreamer1.0-plugins-good \
      gstreamer1.0-plugins-bad \
      gstreamer1.0-libav \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace
ENTRYPOINT ["cargo"]
