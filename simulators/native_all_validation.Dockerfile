# Combined native-feature validation image. The base is built from the pinned
# Aravis 0.8.36 source/image recipe; this layer adds only the GStreamer ABI
# needed by the Rust RTSP feature. The runner supplies a freshly tagged,
# hash-named local validation image, so this Dockerfile never pulls an implicit
# mutable base image. It is test infrastructure, never a runtime adapter image.
ARG ARAVIS_VALIDATION_IMAGE=scratch
FROM ${ARAVIS_VALIDATION_IMAGE}

USER root
RUN rm -f /etc/apt/sources.list.d/debian.sources \
    && printf '%s\n' \
      "deb [check-valid-until=no] http://snapshot.debian.org/archive/debian/20260623T000000Z/ bookworm main" \
      "deb [check-valid-until=no] http://snapshot.debian.org/archive/debian-security/20260623T000000Z/ bookworm-security main" \
      > /etc/apt/sources.list \
    && apt-get -o Acquire::Check-Valid-Until=false update \
    && apt-get install -y --no-install-recommends \
      libgstreamer1.0-dev \
      libgstreamer-plugins-base1.0-dev \
      libclang-dev \
      gstreamer1.0-plugins-base \
      gstreamer1.0-plugins-good \
      gstreamer1.0-plugins-bad \
      gstreamer1.0-libav \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace
ENTRYPOINT ["cargo"]
