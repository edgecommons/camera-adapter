#!/usr/bin/env bash
#
# Two-box GenICam validation over REAL cross-host L2 GigE Vision.
#
# This closes the gap the acceptance matrix names explicitly: the same-container genicam harness
# (`run-genicam-native-coverage.sh`) proves discovery+capture with the fake camera BESIDE the adapter,
# but never L2, never cross-host. Here the fake GigE camera runs on one machine and the adapter on
# another, and the adapter discovers and captures it over GVCP/GVSP multicast on the shared LAN --
# exercising the production genicam backend's discovery, bounded connect (D4), and buffer acquisition
# against a camera that is genuinely a different host at the far end of a wire.
#
# THE TOPOLOGY (three roles; the fleet and adapter MUST be different physical hosts):
#
#   build host   -- has Docker + this repo. Builds the aravis-from-source images. Never the edge.
#   FLEET host   -- runs `arv-fake-gv-camera` (Aravis 0.8.36, built from source) with host networking,
#                   broadcasting GVCP on its LAN interface, plus an MQTT broker (kept OFF the adapter
#                   box per review X6). A Linux Docker host on the same L2 as the adapter.
#   SUT host     -- runs the genicam adapter image (Aravis baked in from source). This is where the
#                   component ships; it never builds Aravis itself. Discovers the fleet over L2.
#
# Aravis is built FROM SOURCE at >= 0.8.36 in the images (Ubuntu/Debian package Aravis is older than the
# 0.8.36 floor `native/aravis-scoped/build.rs` enforces); nothing links a distribution's Aravis.
#
# Usage:
#   FLEET_HOST=marc@192.168.1.193 FLEET_IF=ens33 \
#   SUT_HOST=marc@192.168.1.229   SUT_IF=enp7s0  \
#   simulators/two-box/run-genicam-l2.sh
#
set -euo pipefail

FLEET_HOST=${FLEET_HOST:?set FLEET_HOST=user@ip for the fake-camera + broker box}
SUT_HOST=${SUT_HOST:?set SUT_HOST=user@ip for the adapter box}
FLEET_IF=${FLEET_IF:?set FLEET_IF to the fleet box LAN interface (e.g. ens33)}
SUT_IF=${SUT_IF:?set SUT_IF to the adapter box LAN interface (e.g. enp7s0)}
FLEET_IP=${FLEET_HOST##*@}
CAPTURES=${CAPTURES:-5}

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
adapter_root=$(cd -- "$script_dir/../.." && pwd)
compose_file="$adapter_root/simulators/compose.yaml"

log() { printf '\n=== %s ===\n' "$*"; }

log "1/6 build the aravis-from-source fake-camera image (on the build host)"
docker compose -f "$compose_file" --profile linux-l2 build aravis-fake

log "2/6 build the RUNNABLE genicam adapter image (aravis baked in, genicam feature)"
docker build -f "$adapter_root/simulators/two-box/genicam-adapter.Dockerfile" \
  --build-arg ARAVIS_IMAGE=camera-adapter-simulators-aravis-fake \
  -t genicam-adapter:twobox "$adapter_root"

log "3/6 ship the fake-camera image to the FLEET host and the adapter image to the SUT host"
docker save camera-adapter-simulators-aravis-fake:latest | gzip -1 | ssh "$FLEET_HOST" 'gunzip | docker load'
docker save genicam-adapter:twobox | gzip -1 | ssh "$SUT_HOST" 'gunzip | docker load'

log "4/6 start the fake GigE camera + broker on the FLEET host (host networking, LAN interface)"
ssh "$FLEET_HOST" "
  docker rm -f gige-fleet twobox-broker >/dev/null 2>&1 || true
  docker run -d --name gige-fleet --network host --restart unless-stopped \
    camera-adapter-simulators-aravis-fake:latest --interface=$FLEET_IF
  docker run -d --name twobox-broker --network host --restart unless-stopped \
    eclipse-mosquitto:2 mosquitto -c /mosquitto-no-auth.conf
  sleep 3
  ss -uln | grep -q ':3956' && echo 'GVCP bound on the LAN' || { echo 'GVCP not bound'; exit 1; }
"

log "5/6 write the adapter config and run it on the SUT host, capturing the fleet camera over L2"
config=$(cat <<JSON
{
  "logging": { "level": "INFO" },
  "messaging": { "local": { "host": "$FLEET_IP", "port": 1883, "clientId": "camera-adapter-twobox" } },
  "component": {
    "token": "camera-adapter",
    "global": {
      "output": { "rootDirectory": "/data/output", "minimumFreeBytes": 1073741824, "writeMetadataSidecar": true, "fileNameTemplate": "{captureId}.{extension}" },
      "state": { "directory": "/data/state" },
      "discovery": { "enabled": false, "eligibleInterfaces": ["$SUT_IF"] }
    },
    "instances": [{
      "id": "gige-cam-01", "enabled": true, "defaultCaptureProfile": "still",
      "backend": { "type": "genicam-aravis", "selector": { "serial": "GV01" }, "transport": "gige-vision", "interface": "$SUT_IF" },
      "captureProfiles": { "still": { "output": { "encoding": "png" } } },
      "schedules": [{ "id": "every-5s", "cron": "*/5 * * * * *", "timezone": "UTC", "captureProfile": "still" }]
    }]
  }
}
JSON
)
ssh "$SUT_HOST" "
  set -e
  printf '%s' '$config' | sudo tee /tmp/twobox-genicam.json >/dev/null
  sudo rm -rf /tmp/twobox-data
  sudo mkdir -p /tmp/twobox-data/output /tmp/twobox-data/state
  sudo chown -R root:root /tmp/twobox-data
  sudo chmod 700 /tmp/twobox-data/output /tmp/twobox-data/state   # storage hardening: no group/other write
  docker rm -f twobox-adapter >/dev/null 2>&1 || true
  docker run -d --name twobox-adapter --user 0:0 --network host \
    -v /tmp/twobox-genicam.json:/config.json:ro -v /tmp/twobox-data:/data \
    --entrypoint /usr/local/bin/camera-adapter genicam-adapter:twobox \
    --platform HOST --transport MQTT /config.json -c FILE /config.json -t lab-genicam
"

log "6/6 verify captures accumulate (real frames from a camera on another host, over L2)"
deadline=$(( $(date +%s) + 60 ))
while :; do
  count=$(ssh "$SUT_HOST" 'sudo find /tmp/twobox-data/output -name "*.png" 2>/dev/null | wc -l')
  echo "  captured frames: $count"
  [[ $count -ge $CAPTURES ]] && break
  [[ $(date +%s) -lt $deadline ]] || { echo "  did not reach $CAPTURES captures in time"; exit 1; }
  sleep 5
done
ssh "$SUT_HOST" 'f=$(sudo find /tmp/twobox-data/output -name "*.png.json" | head -1); echo "  camera provenance:"; sudo python3 -c "import json,sys; d=json.load(open(sys.argv[1])); c=d[\"camera\"]; print(f\"    backend={c[\"backend\"]} serial={c[\"serial\"]} vendor={c[\"vendor\"]}\")" "$f"'
echo
echo "PASS: the adapter on $SUT_HOST captured >= $CAPTURES frames from the GigE camera on $FLEET_HOST over cross-host L2."
echo "Tear down with: ssh $FLEET_HOST 'docker rm -f gige-fleet twobox-broker'; ssh $SUT_HOST 'docker rm -f twobox-adapter'"
