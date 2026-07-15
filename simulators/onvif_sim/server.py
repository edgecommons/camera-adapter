#!/usr/bin/env python3
"""Deterministic ONVIF, snapshot HTTP, and WS-Discovery test service.

The service intentionally has no third-party Python dependencies.  A fixture selects
one behavior for a container, which keeps fault runs reproducible and prevents a test
client from weakening the fixture while it is under test.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
import signal
import socket
import ssl
import struct
import threading
import time
import urllib.parse
import zlib
from dataclasses import dataclass, field
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET


SOAP = "http://www.w3.org/2003/05/soap-envelope"
TDS = "http://www.onvif.org/ver10/device/wsdl"
TRT = "http://www.onvif.org/ver10/media/wsdl"
TR2 = "http://www.onvif.org/ver20/media/wsdl"
TPTZ = "http://www.onvif.org/ver20/ptz/wsdl"
TT = "http://www.onvif.org/ver10/schema"
WSA = "http://www.w3.org/2005/08/addressing"
WSD = "http://schemas.xmlsoap.org/ws/2005/04/discovery"
WSSE = (
    "http://docs.oasis-open.org/wss/2004/01/"
    "oasis-200401-wss-wssecurity-secext-1.0.xsd"
)
WSU = (
    "http://docs.oasis-open.org/wss/2004/01/"
    "oasis-200401-wss-wssecurity-utility-1.0.xsd"
)

for prefix, namespace in {
    "s": SOAP,
    "tds": TDS,
    "trt": TRT,
    "tr2": TR2,
    "tptz": TPTZ,
    "tt": TT,
    "wsa": WSA,
    "wsd": WSD,
}.items():
    ET.register_namespace(prefix, namespace)


DEFAULT_FIXTURE: dict[str, Any] = {
    "endpointReference": "urn:uuid:edgecommons-onvif-simulator",
    "publicBaseUrl": "http://onvif-sim:8080",
    "auth": {
        "mode": "none",
        "algorithm": "MD5",
        "username": "operator",
        "password": "camera-secret",
    },
    "device": {
        "manufacturer": "EdgeCommons",
        "model": "Deterministic ONVIF Simulator",
        "firmwareVersion": "1.0.0",
        "serialNumber": "SIM-0001",
        "hardwareId": "EC-SIM",
    },
    "media": {
        "media1": True,
        "media2": True,
        "profile": {"token": "main", "name": "main", "width": 64, "height": 48},
        "snapshotPath": "/snapshot/main.png",
        "rtspUri": "rtsp://mediamtx:8554/camera",
    },
    "ptz": {"enabled": True, "presets": {"home": "Home"}},
    "fault": {"soap": "normal", "snapshot": "normal", "delayMs": 0},
}


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    result = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def load_fixture(path: str | os.PathLike[str] | None) -> dict[str, Any]:
    if path is None:
        fixture = _deep_merge({}, DEFAULT_FIXTURE)
    else:
        parsed = json.loads(Path(path).read_text(encoding="utf-8"))
        if not isinstance(parsed, dict):
            raise ValueError("fixture root must be an object")
        fixture = _deep_merge(DEFAULT_FIXTURE, parsed)
    profile = fixture["media"]["profile"]
    if not 1 <= int(profile["width"]) <= 4096 or not 1 <= int(profile["height"]) <= 4096:
        raise ValueError("fixture dimensions must be between 1 and 4096")
    delay_ms = int(fixture["fault"].get("delayMs", 0))
    if not 0 <= delay_ms <= 300_000:
        raise ValueError("fixture delayMs must be between 0 and 300000")
    oversize = int(fixture["fault"].get("oversizeBytes", 70 * 1024 * 1024))
    if not 1 <= oversize <= 128 * 1024 * 1024:
        raise ValueError("fixture oversizeBytes must be between 1 and 128 MiB")
    return fixture


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _png(width: int, height: int, ordinal: int = 1) -> bytes:
    """Produce a deterministic RGB PNG without an image library."""

    def chunk(kind: bytes, payload: bytes) -> bytes:
        return (
            struct.pack(">I", len(payload))
            + kind
            + payload
            + struct.pack(">I", zlib.crc32(kind + payload) & 0xFFFFFFFF)
        )

    rows = bytearray()
    for y in range(height):
        rows.append(0)
        for x in range(width):
            rows.extend(((x + ordinal) % 256, (y * 3) % 256, (x ^ y) % 256))
    header = struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)
    return b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", header) + chunk(
        b"IDAT", zlib.compress(bytes(rows), 9)
    ) + chunk(b"IEND", b"")


def _envelope(body: ET.Element) -> bytes:
    root = ET.Element(f"{{{SOAP}}}Envelope")
    ET.SubElement(root, f"{{{SOAP}}}Header")
    ET.SubElement(root, f"{{{SOAP}}}Body").append(body)
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


def _discovery_envelope(body: ET.Element, relates_to: str) -> bytes:
    root = ET.Element(f"{{{SOAP}}}Envelope")
    root.set("xmlns:dn", "http://www.onvif.org/ver10/network/wsdl")
    header = ET.SubElement(root, f"{{{SOAP}}}Header")
    action = "ProbeMatches" if _local_name(body.tag) == "ProbeMatches" else "ResolveMatches"
    ET.SubElement(header, f"{{{WSA}}}Action").text = f"{WSD}/{action}"
    stable = hashlib.sha256((relates_to + action).encode()).hexdigest()[:32]
    ET.SubElement(header, f"{{{WSA}}}MessageID").text = (
        f"urn:uuid:{stable[:8]}-{stable[8:12]}-{stable[12:16]}-"
        f"{stable[16:20]}-{stable[20:]}"
    )
    ET.SubElement(header, f"{{{WSA}}}RelatesTo").text = relates_to
    ET.SubElement(header, f"{{{WSA}}}To").text = (
        "http://www.w3.org/2005/08/addressing/anonymous"
    )
    ET.SubElement(root, f"{{{SOAP}}}Body").append(body)
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


def _soap_fault(reason: str, subcode: str = "ter:ActionNotSupported") -> bytes:
    fault = ET.Element(f"{{{SOAP}}}Fault")
    code = ET.SubElement(fault, f"{{{SOAP}}}Code")
    ET.SubElement(code, f"{{{SOAP}}}Value").text = "s:Sender"
    sub = ET.SubElement(code, f"{{{SOAP}}}Subcode")
    ET.SubElement(sub, f"{{{SOAP}}}Value").text = subcode
    reason_node = ET.SubElement(fault, f"{{{SOAP}}}Reason")
    ET.SubElement(reason_node, f"{{{SOAP}}}Text").text = reason
    return _envelope(fault)


def _parse_digest_header(value: str) -> dict[str, str]:
    if not value.startswith("Digest "):
        return {}
    fields: dict[str, str] = {}
    for item in value[7:].split(","):
        key, separator, raw = item.strip().partition("=")
        if separator:
            fields[key] = raw.strip().strip('"')
    return fields


@dataclass
class SimulatorState:
    fixture: dict[str, Any]
    nonce: str = field(default_factory=lambda: secrets.token_hex(16))
    capture_ordinal: int = 0
    moving: bool = False
    pan: float = 0.0
    tilt: float = 0.0
    zoom: float = 0.0
    lock: threading.Lock = field(default_factory=threading.Lock)

    @property
    def base_url(self) -> str:
        return str(self.fixture["publicBaseUrl"]).rstrip("/")

    def next_snapshot(self) -> bytes:
        with self.lock:
            self.capture_ordinal += 1
            profile = self.fixture["media"]["profile"]
            return _png(int(profile["width"]), int(profile["height"]), self.capture_ordinal)

    def snapshot_state(self) -> dict[str, Any]:
        with self.lock:
            return {
                "captureOrdinal": self.capture_ordinal,
                "moving": self.moving,
                "pan": self.pan,
                "tilt": self.tilt,
                "zoom": self.zoom,
            }


class SimulatorHandler(BaseHTTPRequestHandler):
    server_version = "EdgeCommonsOnvifSimulator/1"
    protocol_version = "HTTP/1.1"
    state: SimulatorState

    def setup(self) -> None:
        super().setup()
        self.connection.settimeout(10)

    def log_message(self, fmt: str, *args: object) -> None:
        print(json.dumps({"client": self.client_address[0], "message": fmt % args}))

    def _send(self, status: int, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _challenge(self) -> None:
        auth = self.state.fixture["auth"]
        if auth["mode"] == "basic":
            challenge = 'Basic realm="EdgeCommons camera simulator"'
        else:
            algorithm = str(auth.get("algorithm", "MD5")).upper()
            if algorithm not in {"MD5", "SHA-256"}:
                raise ValueError("digest algorithm must be MD5 or SHA-256")
            challenge = (
                'Digest realm="EdgeCommons camera simulator", '
                f'nonce="{self.state.nonce}", algorithm={algorithm}, qop="auth"'
            )
        self.send_response(HTTPStatus.UNAUTHORIZED)
        self.send_header("WWW-Authenticate", challenge)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _http_authorized(self) -> bool:
        auth = self.state.fixture["auth"]
        mode = auth["mode"]
        if mode in {"none", "wsse-digest"}:
            return True
        header = self.headers.get("Authorization", "")
        if mode == "basic":
            wanted = base64.b64encode(
                f'{auth["username"]}:{auth["password"]}'.encode()
            ).decode()
            return hmac.compare_digest(header, f"Basic {wanted}")
        fields = _parse_digest_header(header)
        required = {"username", "realm", "nonce", "uri", "response"}
        if not required.issubset(fields) or fields["nonce"] != self.state.nonce:
            return False
        if fields["username"] != auth["username"] or fields["uri"] != self.path:
            return False
        algorithm = fields.get("algorithm", "MD5").upper()
        if algorithm != str(auth.get("algorithm", "MD5")).upper():
            return False
        digest = hashlib.sha256 if algorithm == "SHA-256" else hashlib.md5
        ha1 = digest(
            f'{auth["username"]}:{fields["realm"]}:{auth["password"]}'.encode()
        ).hexdigest()
        ha2 = digest(f"{self.command}:{fields['uri']}".encode()).hexdigest()
        if fields.get("qop"):
            source = (
                f"{ha1}:{fields['nonce']}:{fields.get('nc', '')}:"
                f"{fields.get('cnonce', '')}:{fields['qop']}:{ha2}"
            )
        else:
            source = f"{ha1}:{fields['nonce']}:{ha2}"
        return hmac.compare_digest(digest(source.encode()).hexdigest(), fields["response"])

    def _wsse_authorized(self, raw: bytes) -> bool:
        auth = self.state.fixture["auth"]
        if auth["mode"] != "wsse-digest":
            return True
        try:
            root = ET.fromstring(raw)
            token = root.find(f".//{{{WSSE}}}UsernameToken")
            if token is None:
                return False
            username = token.findtext(f"{{{WSSE}}}Username", "")
            password = token.findtext(f"{{{WSSE}}}Password", "")
            nonce_text = token.findtext(f"{{{WSSE}}}Nonce", "")
            created = token.findtext(f"{{{WSU}}}Created", "")
            nonce = base64.b64decode(nonce_text, validate=True)
            wanted = base64.b64encode(
                hashlib.sha1(nonce + created.encode() + auth["password"].encode()).digest()
            ).decode()
            return username == auth["username"] and hmac.compare_digest(password, wanted)
        except (ET.ParseError, ValueError):
            return False

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/healthz":
            self._send(HTTPStatus.OK, b'{"status":"ok"}', "application/json")
            return
        if parsed.path == "/admin/state":
            body = json.dumps(self.state.snapshot_state(), sort_keys=True).encode()
            self._send(HTTPStatus.OK, body, "application/json")
            return
        if parsed.path.startswith("/snapshot/"):
            if not self._http_authorized():
                self._challenge()
                return
            self._snapshot()
            return
        self._send(HTTPStatus.NOT_FOUND, b"not found", "text/plain")

    def _snapshot(self) -> None:
        fault = self.state.fixture["fault"]
        mode = fault.get("snapshot", "normal")
        delay = int(fault.get("delayMs", 0)) / 1000
        if delay:
            time.sleep(delay)
        if mode == "redirect-safe":
            self.send_response(HTTPStatus.FOUND)
            self.send_header("Location", self.state.base_url + "/snapshot/main.png")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if mode == "redirect-unsafe":
            self.send_response(HTTPStatus.FOUND)
            self.send_header("Location", "http://169.254.169.254/latest/meta-data/")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if mode == "disconnect":
            self.connection.shutdown(socket.SHUT_RDWR)
            self.connection.close()
            return
        body = self.state.next_snapshot()
        content_type = "image/png"
        if mode == "truncate":
            body = body[: max(1, len(body) // 2)]
        elif mode == "oversize":
            body = b"X" * int(fault.get("oversizeBytes", 70 * 1024 * 1024))
        elif mode == "wrong-type":
            content_type = "text/html"
        self._send(HTTPStatus.OK, body, content_type)

    def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        if not self._http_authorized():
            self._challenge()
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            self._send(HTTPStatus.BAD_REQUEST, b"invalid length", "text/plain")
            return
        if length < 0 or length > 2 * 1024 * 1024:
            self._send(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, b"too large", "text/plain")
            return
        raw = self.rfile.read(length)
        if not self._wsse_authorized(raw):
            self._send(HTTPStatus.UNAUTHORIZED, _soap_fault("NotAuthorized"), "application/soap+xml")
            return
        self._soap(raw)

    def _soap(self, raw: bytes) -> None:
        fault = self.state.fixture["fault"]
        delay = int(fault.get("delayMs", 0)) / 1000
        if delay:
            time.sleep(delay)
        if fault.get("soap") == "malformed":
            self._send(HTTPStatus.OK, b"<not-closed", "application/soap+xml")
            return
        if fault.get("soap") == "dtd":
            body = b'<!DOCTYPE x [<!ENTITY e "boom">]><x>&e;</x>'
            self._send(HTTPStatus.OK, body, "application/soap+xml")
            return
        if fault.get("soap") == "oversize":
            body = b"<x>" + b"X" * int(fault.get("oversizeBytes", 2 * 1024 * 1024)) + b"</x>"
            self._send(HTTPStatus.OK, body, "application/soap+xml")
            return
        if b"<!DOCTYPE" in raw.upper() or b"<!ENTITY" in raw.upper():
            self._send(HTTPStatus.BAD_REQUEST, _soap_fault("DTD forbidden"), "application/soap+xml")
            return
        try:
            root = ET.fromstring(raw)
            body = root.find(f"{{{SOAP}}}Body")
            operation = next(iter(body)) if body is not None else None
            if operation is None:
                raise ET.ParseError("missing SOAP body operation")
        except (ET.ParseError, StopIteration):
            self._send(HTTPStatus.BAD_REQUEST, _soap_fault("Malformed SOAP"), "application/soap+xml")
            return
        name = _local_name(operation.tag)
        forced_action = fault.get("action")
        if fault.get("soap") == "fault" and (not forced_action or forced_action == name):
            self._send(HTTPStatus.INTERNAL_SERVER_ERROR, _soap_fault("Injected fault"), "application/soap+xml")
            return
        response = self._dispatch(name, operation)
        if response is None:
            self._send(HTTPStatus.INTERNAL_SERVER_ERROR, _soap_fault(name), "application/soap+xml")
        else:
            self._send(HTTPStatus.OK, _envelope(response), "application/soap+xml")

    def _dispatch(self, name: str, operation: ET.Element) -> ET.Element | None:
        fixture = self.state.fixture
        media = fixture["media"]
        profile = media["profile"]
        operation_namespace = operation.tag[1:].split("}", 1)[0] if operation.tag.startswith("{") else ""
        media_namespace = TR2 if operation_namespace == TR2 else TRT
        if name == "GetDeviceInformation":
            response = ET.Element(f"{{{TDS}}}GetDeviceInformationResponse")
            labels = {
                "Manufacturer": "manufacturer",
                "Model": "model",
                "FirmwareVersion": "firmwareVersion",
                "SerialNumber": "serialNumber",
                "HardwareId": "hardwareId",
            }
            for label, key in labels.items():
                ET.SubElement(response, f"{{{TDS}}}{label}").text = fixture["device"][key]
            return response
        if name == "GetCapabilities":
            response = ET.Element(f"{{{TDS}}}GetCapabilitiesResponse")
            capabilities = ET.SubElement(response, f"{{{TDS}}}Capabilities")
            for label, path in {
                "Device": "/onvif/device_service",
                "Media": "/onvif/media_service",
                "Media2": "/onvif/media2_service",
                "PTZ": "/onvif/ptz_service",
            }.items():
                node = ET.SubElement(capabilities, f"{{{TT}}}{label}")
                ET.SubElement(node, f"{{{TT}}}XAddr").text = self.state.base_url + path
            return response
        if name == "GetServices":
            response = ET.Element(f"{{{TDS}}}GetServicesResponse")
            services = [(TDS, "/onvif/device_service", 2, 6)]
            if media["media1"]:
                services.append((TRT, "/onvif/media_service", 2, 6))
            if media["media2"]:
                services.append((TR2, "/onvif/media2_service", 2, 0))
            if fixture["ptz"]["enabled"]:
                services.append((TPTZ, "/onvif/ptz_service", 2, 6))
            for namespace, path, major, minor in services:
                service = ET.SubElement(response, f"{{{TDS}}}Service")
                ET.SubElement(service, f"{{{TDS}}}Namespace").text = namespace
                ET.SubElement(service, f"{{{TDS}}}XAddr").text = self.state.base_url + path
                version = ET.SubElement(service, f"{{{TDS}}}Version")
                ET.SubElement(version, f"{{{TT}}}Major").text = str(major)
                ET.SubElement(version, f"{{{TT}}}Minor").text = str(minor)
            return response
        if name == "GetServiceCapabilities":
            response = ET.Element(f"{{{operation_namespace}}}GetServiceCapabilitiesResponse")
            ET.SubElement(response, f"{{{operation_namespace}}}Capabilities")
            return response
        if name == "GetProfiles":
            namespace = media_namespace
            if (namespace == TR2 and not media["media2"]) or (namespace == TRT and not media["media1"]):
                return None
            response = ET.Element(f"{{{namespace}}}GetProfilesResponse")
            item = ET.SubElement(response, f"{{{namespace}}}Profiles", token=profile["token"])
            ET.SubElement(item, f"{{{TT}}}Name").text = profile["name"]
            video = ET.SubElement(item, f"{{{TT}}}VideoEncoderConfiguration")
            resolution = ET.SubElement(video, f"{{{TT}}}Resolution")
            ET.SubElement(resolution, f"{{{TT}}}Width").text = str(profile["width"])
            ET.SubElement(resolution, f"{{{TT}}}Height").text = str(profile["height"])
            return response
        if name == "GetSnapshotUri":
            response = ET.Element(f"{{{media_namespace}}}GetSnapshotUriResponse")
            uri = ET.SubElement(response, f"{{{media_namespace}}}MediaUri")
            returned = self.state.base_url + media["snapshotPath"]
            if fixture["fault"].get("snapshot") == "hostile-uri":
                returned = "http://169.254.169.254/latest/meta-data/iam/security-credentials/"
            ET.SubElement(uri, f"{{{TT}}}Uri").text = returned
            ET.SubElement(uri, f"{{{TT}}}InvalidAfterConnect").text = "false"
            ET.SubElement(uri, f"{{{TT}}}InvalidAfterReboot").text = "false"
            ET.SubElement(uri, f"{{{TT}}}Timeout").text = "PT60S"
            return response
        if name == "GetStreamUri":
            response = ET.Element(f"{{{media_namespace}}}GetStreamUriResponse")
            uri = ET.SubElement(response, f"{{{media_namespace}}}MediaUri")
            ET.SubElement(uri, f"{{{TT}}}Uri").text = media["rtspUri"]
            return response
        if name in {"GetNodes", "GetConfigurations", "GetConfigurationOptions"}:
            if not fixture["ptz"]["enabled"]:
                return None
            response = ET.Element(f"{{{TPTZ}}}{name}Response")
            if name == "GetNodes":
                node = ET.SubElement(response, f"{{{TPTZ}}}PTZNode", token="ptz-node")
                ET.SubElement(node, f"{{{TT}}}Name").text = "Simulator PTZ"
                ET.SubElement(node, f"{{{TT}}}MaximumNumberOfPresets").text = "16"
                ET.SubElement(node, f"{{{TT}}}HomeSupported").text = "true"
            elif name == "GetConfigurations":
                config = ET.SubElement(response, f"{{{TPTZ}}}PTZConfiguration", token="ptz-main")
                ET.SubElement(config, f"{{{TT}}}Name").text = "Simulator PTZ"
                ET.SubElement(config, f"{{{TT}}}NodeToken").text = "ptz-node"
            else:
                spaces = ET.SubElement(response, f"{{{TPTZ}}}PTZConfigurationOptions")
                space_container = ET.SubElement(spaces, f"{{{TT}}}Spaces")
                for label, uri, minimum, maximum in (
                    ("AbsolutePanTiltPositionSpace", "urn:edgecommons:ptz:pan-tilt", -2.0, 2.0),
                    ("AbsoluteZoomPositionSpace", "urn:edgecommons:ptz:zoom", 0.0, 4.0),
                    ("RelativePanTiltTranslationSpace", "urn:edgecommons:ptz:pan-tilt-relative", -0.5, 0.5),
                    ("RelativeZoomTranslationSpace", "urn:edgecommons:ptz:zoom-relative", -1.0, 1.0),
                    ("ContinuousPanTiltVelocitySpace", "urn:edgecommons:ptz:pan-tilt-velocity", -3.0, 3.0),
                    ("ContinuousZoomVelocitySpace", "urn:edgecommons:ptz:zoom-velocity", -2.0, 2.0),
                ):
                    space = ET.SubElement(space_container, f"{{{TT}}}{label}")
                    ET.SubElement(space, f"{{{TT}}}URI").text = uri
                    for axis in ("XRange", "YRange") if "PanTilt" in label else ("XRange",):
                        range_node = ET.SubElement(space, f"{{{TT}}}{axis}")
                        ET.SubElement(range_node, f"{{{TT}}}Min").text = str(minimum)
                        ET.SubElement(range_node, f"{{{TT}}}Max").text = str(maximum)
            return response
        if name == "GetStatus":
            if not fixture["ptz"]["enabled"]:
                return None
            response = ET.Element(f"{{{TPTZ}}}GetStatusResponse")
            status = ET.SubElement(response, f"{{{TPTZ}}}PTZStatus")
            position = ET.SubElement(status, f"{{{TT}}}Position")
            with self.state.lock:
                ET.SubElement(position, f"{{{TT}}}PanTilt", x=str(self.state.pan), y=str(self.state.tilt))
                ET.SubElement(position, f"{{{TT}}}Zoom", x=str(self.state.zoom))
                move_status = ET.SubElement(status, f"{{{TT}}}MoveStatus")
                value = "MOVING" if self.state.moving else "IDLE"
                ET.SubElement(move_status, f"{{{TT}}}PanTilt").text = value
                ET.SubElement(move_status, f"{{{TT}}}Zoom").text = value
            return response
        if name == "GetPresets":
            if not fixture["ptz"]["enabled"]:
                return None
            response = ET.Element(f"{{{TPTZ}}}GetPresetsResponse")
            for token, preset_name in fixture["ptz"]["presets"].items():
                preset = ET.SubElement(response, f"{{{TPTZ}}}Preset", token=token)
                ET.SubElement(preset, f"{{{TT}}}Name").text = preset_name
            return response
        if name in {"ContinuousMove", "AbsoluteMove", "RelativeMove", "GotoPreset", "GotoHomePosition"}:
            if not fixture["ptz"]["enabled"]:
                return None
            with self.state.lock:
                self.state.moving = name == "ContinuousMove"
                if name in {"GotoPreset", "GotoHomePosition"}:
                    self.state.pan = self.state.tilt = self.state.zoom = 0.0
                elif name in {"AbsoluteMove", "RelativeMove"}:
                    pan_tilt = operation.find(f".//{{{TT}}}PanTilt")
                    zoom = operation.find(f".//{{{TT}}}Zoom")
                    if pan_tilt is not None:
                        x = float(pan_tilt.get("x", "0"))
                        y = float(pan_tilt.get("y", "0"))
                        if name == "AbsoluteMove":
                            self.state.pan, self.state.tilt = x, y
                        else:
                            self.state.pan += x
                            self.state.tilt += y
                    if zoom is not None:
                        value = float(zoom.get("x", "0"))
                        self.state.zoom = value if name == "AbsoluteMove" else self.state.zoom + value
            return ET.Element(f"{{{TPTZ}}}{name}Response")
        if name == "Stop":
            if not fixture["ptz"]["enabled"]:
                return None
            with self.state.lock:
                self.state.moving = False
            return ET.Element(f"{{{TPTZ}}}StopResponse")
        return None


def make_handler(state: SimulatorState) -> type[SimulatorHandler]:
    class BoundHandler(SimulatorHandler):
        pass

    BoundHandler.state = state
    return BoundHandler


class BoundedThreadingHTTPServer(ThreadingHTTPServer):
    """Thread-per-request server with a hard in-flight connection ceiling.

    The ceiling defaults to 64 -- enough for the functional fixtures -- and is raised via
    ``SIM_MAX_CONNECTIONS`` for fleet/load runs where many cameras establish sessions at once
    (each warm RTSP session first does an ONVIF handshake, so N cameras need N concurrent slots).
    """

    daemon_threads = True
    block_on_close = False
    _MAX_CONNECTIONS = max(8, int(os.environ.get("SIM_MAX_CONNECTIONS", "64")))
    request_queue_size = _MAX_CONNECTIONS

    def __init__(self, address: tuple[str, int], handler: type[BaseHTTPRequestHandler]) -> None:
        self._request_slots = threading.BoundedSemaphore(self._MAX_CONNECTIONS)
        super().__init__(address, handler)

    def process_request(self, request: socket.socket, client_address: tuple[str, int]) -> None:
        if not self._request_slots.acquire(blocking=False):
            self.close_request(request)
            return
        super().process_request(request, client_address)

    def process_request_thread(
        self, request: socket.socket, client_address: tuple[str, int]
    ) -> None:
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._request_slots.release()


class DiscoveryResponder(threading.Thread):
    def __init__(self, state: SimulatorState, host: str, port: int, multicast: bool) -> None:
        super().__init__(name="ws-discovery", daemon=True)
        self.state = state
        self.host = host
        self.port = port
        self.multicast = multicast
        self.stop_event = threading.Event()
        self.ready = threading.Event()
        self.sock: socket.socket | None = None

    def run(self) -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock = sock
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self.host, self.port))
        self.port = int(sock.getsockname()[1])
        if self.multicast:
            membership = socket.inet_aton("239.255.255.250") + socket.inet_aton("0.0.0.0")
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, membership)
        sock.settimeout(0.25)
        self.ready.set()
        while not self.stop_event.is_set():
            try:
                payload, address = sock.recvfrom(65_535)
            except TimeoutError:
                continue
            except OSError:
                break
            if b"Probe" not in payload and b"Resolve" not in payload:
                continue
            try:
                request = ET.fromstring(payload)
                message_id = request.findtext(f".//{{{WSA}}}MessageID", "urn:uuid:unknown")
                is_resolve = request.find(f".//{{{WSD}}}Resolve") is not None
                requested_epr = request.findtext(
                    f".//{{{WSD}}}Resolve/{{{WSA}}}EndpointReference/{{{WSA}}}Address"
                )
            except ET.ParseError:
                continue
            if is_resolve and requested_epr != self.state.fixture["endpointReference"]:
                continue
            container_name = "ResolveMatches" if is_resolve else "ProbeMatches"
            match_name = "ResolveMatch" if is_resolve else "ProbeMatch"
            body = ET.Element(f"{{{WSD}}}{container_name}")
            match = ET.SubElement(body, f"{{{WSD}}}{match_name}")
            endpoint = ET.SubElement(match, f"{{{WSA}}}EndpointReference")
            ET.SubElement(endpoint, f"{{{WSA}}}Address").text = self.state.fixture["endpointReference"]
            ET.SubElement(match, f"{{{WSD}}}Types").text = "dn:NetworkVideoTransmitter"
            xaddr = self.state.base_url + "/onvif/device_service"
            if self.state.fixture["fault"].get("discovery") == "hostile-xaddr":
                xaddr = "http://169.254.169.254/onvif/device_service"
            ET.SubElement(match, f"{{{WSD}}}XAddrs").text = xaddr
            ET.SubElement(match, f"{{{WSD}}}MetadataVersion").text = "1"
            copies = max(1, min(4, int(self.state.fixture["fault"].get("discoveryCopies", 1))))
            response = _discovery_envelope(body, message_id)
            for _ in range(copies):
                sock.sendto(response, address)
        sock.close()

    def stop(self) -> None:
        self.stop_event.set()
        if self.sock is not None:
            self.sock.close()


def serve() -> None:
    fixture = load_fixture(os.environ.get("SIM_FIXTURE"))
    state = SimulatorState(fixture)
    host = os.environ.get("SIM_HTTP_HOST", "0.0.0.0")
    port = int(os.environ.get("SIM_HTTP_PORT", "8080"))
    server = BoundedThreadingHTTPServer((host, port), make_handler(state))
    cert = os.environ.get("SIM_TLS_CERT")
    key = os.environ.get("SIM_TLS_KEY")
    if cert or key:
        if not cert or not key:
            raise ValueError("SIM_TLS_CERT and SIM_TLS_KEY must be set together")
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.load_cert_chain(cert, key)
        server.socket = context.wrap_socket(server.socket, server_side=True)
    discovery = DiscoveryResponder(
        state,
        os.environ.get("SIM_DISCOVERY_HOST", "0.0.0.0"),
        int(os.environ.get("SIM_DISCOVERY_PORT", "3702")),
        os.environ.get("SIM_DISCOVERY_MULTICAST", "true").lower() == "true",
    )
    discovery.start()

    def stop(_signum: int, _frame: object) -> None:
        discovery.stop()
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    print(json.dumps({"event": "ready", "http": f"{host}:{port}"}), flush=True)
    try:
        server.serve_forever(poll_interval=0.25)
    finally:
        discovery.stop()
        discovery.join(timeout=2)
        server.server_close()


if __name__ == "__main__":
    serve()
