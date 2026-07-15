#!/usr/bin/env bash
#
# Two-box ONVIF/RTSP validation over a real cross-host wire.
#
# Companion to run-genicam-l2.sh. The fleet (an ONVIF simulator that answers SOAP/snapshot/WS-Discovery
# and hands back an RTSP URI, plus a mediamtx serving that RTSP stream, plus an MQTT broker) runs on one
# host; the adapter runs on another and captures through its production ONVIF-RTSP backend over the LAN.
#
# It reaches, at N warm streams, the streaming findings no in-process test can:
#   B3  the RTSP decode gate -- the old 4-permit process-global storm at 12-16 warm streams;
#   D3  blocking pipeline teardown on a worker thread;
#   R1  a pooled (not per-request) reqwest client across many ONVIF cameras;
#   B6  catalog contention (`database is locked`) under N concurrent writers -- and its fix: SQLITE_BUSY
#       retries the capture instead of disconnecting the camera.
#
# NOTE ON WS-DISCOVERY (T1): this script uses an explicit deviceServiceUrl, not discovery. Cross-host
# WS-Discovery needs the LAN to forward multicast 239.255.255.250 to the fleet host; a bridged VM behind
# a switch doing IGMP snooping does not receive it (GVCP broadcast floods and works; WS-Discovery
# multicast is pruned). Run the fleet on a host directly on the physical LAN to exercise T1 cross-host.
#
# Usage:
#   FLEET_HOST=marc@192.168.1.193 SUT_HOST=marc@192.168.1.229 SUT_IF=enp7s0 CAMERAS=32 \
#   simulators/two-box/run-rtsp-onvif-l2.sh
#
set -euo pipefail

FLEET_HOST=${FLEET_HOST:?set FLEET_HOST=user@ip for the ONVIF/RTSP fleet + broker box}
SUT_HOST=${SUT_HOST:?set SUT_HOST=user@ip for the adapter box}
SUT_IF=${SUT_IF:?set SUT_IF to the adapter box LAN interface (only used for eligibleInterfaces)}
FLEET_IP=${FLEET_HOST##*@}
CAMERAS=${CAMERAS:-32}

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
adapter_root=$(cd -- "$script_dir/../.." && pwd)
log() { printf '\n=== %s ===\n' "$*"; }

log "1/6 build the RTSP adapter image (shipped Dockerfile --target rtsp: onvif+rtsp+GStreamer) and the ONVIF sim"
docker build --target rtsp -t camera-adapter:rtsp "$adapter_root"
docker build -f "$adapter_root/simulators/onvif_sim/Dockerfile" -t onvif-sim:twobox "$adapter_root/simulators/onvif_sim"

log "2/6 ship images to the hosts; pull mediamtx (ffmpeg) on the fleet"
docker save onvif-sim:twobox | gzip -1 | ssh "$FLEET_HOST" 'gunzip | docker load'
docker save camera-adapter:rtsp | gzip -1 | ssh "$SUT_HOST" 'gunzip | docker load'
ssh "$FLEET_HOST" 'docker pull -q bluenviron/mediamtx:latest-ffmpeg >/dev/null'

log "3/6 write the fleet config (RTSP URI + ONVIF endpoint repointed at the fleet IP) and start the fleet"
onvif_fixture=$(cat <<JSON
{ "publicBaseUrl": "http://$FLEET_IP:8080", "auth": { "mode": "none" },
  "media": { "media1": true, "media2": true,
    "profile": { "token": "main", "name": "main", "width": 1280, "height": 720 },
    "snapshotPath": "/snapshot/main.png", "rtspUri": "rtsp://$FLEET_IP:8554/camera" },
  "ptz": { "enabled": true, "presets": { "home": "Home" } },
  "fault": { "soap": "normal", "snapshot": "normal", "delayMs": 0 } }
JSON
)
mediamtx_yml='logLevel: warn
paths:
  camera:
    runOnInit: >-
      ffmpeg -hide_banner -loglevel warning -re -f lavfi -i testsrc2=size=1280x720:rate=25
      -vf format=yuv420p -c:v libx264 -preset ultrafast -tune zerolatency
      -g 25 -keyint_min 25 -sc_threshold 0 -f rtsp rtsp://127.0.0.1:8554/camera
    runOnInitRestart: yes'
ssh "$FLEET_HOST" "
  printf '%s' '$onvif_fixture' > /tmp/twobox-onvif.json
  printf '%s' '$mediamtx_yml' > /tmp/mediamtx-twobox.yml
  docker rm -f twobox-broker rtsp-fleet onvif-fleet >/dev/null 2>&1 || true
  docker run -d --name twobox-broker --network host --restart unless-stopped eclipse-mosquitto:2 mosquitto -c /mosquitto-no-auth.conf >/dev/null
  docker run -d --name rtsp-fleet --network host --restart unless-stopped -v /tmp/mediamtx-twobox.yml:/mediamtx.yml:ro bluenviron/mediamtx:latest-ffmpeg >/dev/null
  docker run -d --name onvif-fleet --network host --restart unless-stopped -e SIM_FIXTURE=/fixture.json -v /tmp/twobox-onvif.json:/fixture.json:ro onvif-sim:twobox >/dev/null
  sleep 6; ss -tln | grep -q ':8554' && ss -tln | grep -q ':8080' && echo 'RTSP + ONVIF listening' || { echo 'fleet not up'; exit 1; }
"

log "4/6 write the adapter config ($CAMERAS warm ONVIF-RTSP cameras) and run it on the SUT"
adapter_cfg=$(python3 - "$FLEET_IP" "$SUT_IF" "$CAMERAS" <<'PY'
import json, sys
fleet_ip, sut_if, n = sys.argv[1], sys.argv[2], int(sys.argv[3])
cams = [{
  "id": f"rtsp-cam-{i:02}", "enabled": True, "defaultCaptureProfile": "still",
  "backend": {"type": "onvif-rtsp", "deviceServiceUrl": f"http://{fleet_ip}:8080/onvif/device_service",
              "mediaProfile": "main", "captureMode": "rtsp-frame", "rtspSessionPolicy": "warm", "allowInsecure": True},
  "captureProfiles": {"still": {"output": {"encoding": "png"}}},
  "schedules": [{"id": "every-5s", "cron": "*/5 * * * * *", "timezone": "UTC", "captureProfile": "still"}],
} for i in range(n)]
print(json.dumps({"logging": {"level": "WARN"},
  "messaging": {"local": {"host": fleet_ip, "port": 1883, "clientId": "camera-adapter-rtsp"}},
  "component": {"token": "camera-adapter", "global": {
    "output": {"rootDirectory": "/data/output", "minimumFreeBytes": 1073741824, "writeMetadataSidecar": False, "fileNameTemplate": "{captureId}.{extension}"},
    "state": {"directory": "/data/state"}, "discovery": {"enabled": False}}, "instances": cams}}))
PY
)
ssh "$SUT_HOST" "
  printf '%s' '$adapter_cfg' | sudo tee /tmp/rtsp-config.json >/dev/null
  sudo rm -rf /tmp/rtsp-data; sudo mkdir -p /tmp/rtsp-data/output /tmp/rtsp-data/state
  sudo chown -R root:root /tmp/rtsp-data; sudo chmod 700 /tmp/rtsp-data/output /tmp/rtsp-data/state
  docker rm -f twobox-rtsp >/dev/null 2>&1 || true
  docker run -d --name twobox-rtsp --user 0:0 --network host \
    -v /tmp/rtsp-config.json:/config.json:ro -v /tmp/rtsp-data:/data \
    --entrypoint /usr/local/bin/camera-adapter camera-adapter:rtsp \
    --platform HOST --transport MQTT /config.json -c FILE /config.json -t lab-rtsp
"

log "5/6 let $CAMERAS warm streams run for 55s"
sleep 55

log "6/6 B3/B6 signals"
ssh "$SUT_HOST" '
  echo "  captures:               $(sudo find /tmp/rtsp-data/output -name "*.png" | wc -l)"
  echo "  session restarts (B3):  $(docker logs twobox-rtsp 2>&1 | grep -icE "restart|reconnect|rebuild|backoff|session.*torn")"
  echo "  sqlite-locked (B6):     $(docker logs twobox-rtsp 2>&1 | grep -ic "database is locked")"
  echo "  camera disconnects:     $(docker logs twobox-rtsp 2>&1 | grep -icE "disconnect|BACKOFF|went offline")   (B6 cascade -- must be 0)"
  echo "  status:                 $(docker ps --filter name=twobox-rtsp --format "{{.Status}}")"
'
echo
echo "PASS if restarts==0 and disconnects==0: the decode gate (B3) and the SQLITE_BUSY-retry fix (B6) hold under $CAMERAS warm cross-host streams."
echo "Tear down: ssh $FLEET_HOST 'docker rm -f rtsp-fleet onvif-fleet twobox-broker'; ssh $SUT_HOST 'docker rm -f twobox-rtsp'"
