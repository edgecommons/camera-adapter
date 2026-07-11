FROM docker.io/library/python@sha256:a70519002c49552ea0a853de47599cf40479b001bd7a624f1112eaf44dcaccc7 AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    SIM_FIXTURE=/fixtures/default.json

WORKDIR /opt/onvif-sim
COPY --chmod=0555 server.py /opt/onvif-sim/server.py
COPY --chmod=0555 fixtures /fixtures
RUN find /fixtures -type d -exec chmod 0555 {} + \
    && find /fixtures -type f -exec chmod 0444 {} +

USER 65532:65532

FROM base AS tests
COPY --chown=65532:65532 --chmod=0444 test_server.py /opt/onvif-sim/test_server.py
ENTRYPOINT ["python", "-B", "/opt/onvif-sim/test_server.py", "-v"]

FROM base AS runtime
EXPOSE 8080/tcp 3702/udp
ENTRYPOINT ["python", "-I", "/opt/onvif-sim/server.py"]
