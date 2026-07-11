FROM docker.io/library/debian@sha256:60eac759739651111db372c07be67863818726f754804b8707c90979bda511df AS build

ARG DEBIAN_SNAPSHOT=20260623T000000Z
ARG ARAVIS_COMMIT=97ff5e330919d7a44b871df4eda669feeafdf342

RUN rm -f /etc/apt/sources.list.d/debian.sources \
    && printf '%s\n' \
      "deb [check-valid-until=no] http://snapshot.debian.org/archive/debian/${DEBIAN_SNAPSHOT}/ bookworm main" \
      "deb [check-valid-until=no] http://snapshot.debian.org/archive/debian-security/${DEBIAN_SNAPSHOT}/ bookworm-security main" \
      > /etc/apt/sources.list \
    && apt-get -o Acquire::Check-Valid-Until=false update \
    && apt-get install -y --no-install-recommends \
      build-essential ca-certificates git libglib2.0-dev libusb-1.0-0-dev libxml2-dev \
      meson ninja-build pkg-config \
    && rm -rf /var/lib/apt/lists/*

RUN git clone --filter=blob:none --no-checkout https://github.com/AravisProject/aravis.git /src/aravis \
    && git -C /src/aravis fetch --depth=1 origin "${ARAVIS_COMMIT}" \
    && git -C /src/aravis checkout --detach FETCH_HEAD \
    && test "$(git -C /src/aravis rev-parse HEAD)" = "${ARAVIS_COMMIT}"

RUN meson setup /src/aravis/build /src/aravis \
      --buildtype=release --prefix=/opt/aravis --libdir=lib \
      -Dviewer=disabled -Dgst-plugin=disabled -Dintrospection=disabled \
      -Ddocumentation=disabled -Dtests=true \
    && meson compile -C /src/aravis/build \
    && meson install -C /src/aravis/build \
    && test -x /opt/aravis/bin/arv-fake-gv-camera-0.8

FROM docker.io/library/debian@sha256:60eac759739651111db372c07be67863818726f754804b8707c90979bda511df

ARG DEBIAN_SNAPSHOT=20260623T000000Z
RUN rm -f /etc/apt/sources.list.d/debian.sources \
    && printf '%s\n' \
      "deb [check-valid-until=no] http://snapshot.debian.org/archive/debian/${DEBIAN_SNAPSHOT}/ bookworm main" \
      "deb [check-valid-until=no] http://snapshot.debian.org/archive/debian-security/${DEBIAN_SNAPSHOT}/ bookworm-security main" \
      > /etc/apt/sources.list \
    && apt-get -o Acquire::Check-Valid-Until=false update \
    && apt-get install -y --no-install-recommends ca-certificates libglib2.0-0 libusb-1.0-0 libxml2 zlib1g \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /opt/aravis /opt/aravis
ENV LD_LIBRARY_PATH=/opt/aravis/lib \
    PATH=/opt/aravis/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

USER 65532:65532
ENTRYPOINT ["/opt/aravis/bin/arv-fake-gv-camera-0.8"]
CMD ["--interface=eth0"]
