# Pinned, non-production builder for the Linux-only capacity harness. It uses
# the same Rust 1.85.1 digest as the existing native validation images so the
# inner runner can use its default `cargo` without relying on host tooling.
FROM docker.io/library/rust@sha256:e51d0265072d2d9d5d320f6a44dde6b9ef13653b035098febd68cce8fa7c0bc4

ARG DEBIAN_SNAPSHOT=20260623T000000Z

RUN test "$(rustc --version | awk '{print $2}')" = "1.85.1" \
    && rm -f /etc/apt/sources.list.d/debian.sources \
    && printf '%s\n' \
      "deb [check-valid-until=no] http://snapshot.debian.org/archive/debian/${DEBIAN_SNAPSHOT}/ bookworm main" \
      "deb [check-valid-until=no] http://snapshot.debian.org/archive/debian-security/${DEBIAN_SNAPSHOT}/ bookworm-security main" \
      > /etc/apt/sources.list \
    && apt-get -o Acquire::Check-Valid-Until=false update \
    && apt-get install -y --no-install-recommends python3 git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /edgecommons/camera-adapter
