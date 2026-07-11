from __future__ import annotations

import base64
import contextlib
import hashlib
import json
import socket
import threading
import unittest
import urllib.error
import urllib.request
from xml.etree import ElementTree as ET

import server


@contextlib.contextmanager
def running(fixture_override: dict | None = None):
    fixture = server._deep_merge(server.DEFAULT_FIXTURE, fixture_override or {})
    state = server.SimulatorState(fixture)
    httpd = server.BoundedThreadingHTTPServer(("127.0.0.1", 0), server.make_handler(state))
    fixture["publicBaseUrl"] = f"http://127.0.0.1:{httpd.server_port}"
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        yield state, fixture["publicBaseUrl"]
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=2)


def soap(operation: ET.Element) -> bytes:
    return server._envelope(operation)


def post(url: str, body: bytes, headers: dict[str, str] | None = None):
    request = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/soap+xml", **(headers or {})},
    )
    return urllib.request.urlopen(request, timeout=2)


class SimulatorTests(unittest.TestCase):
    def test_device_media_snapshot_and_state_are_deterministic(self):
        with running() as (_state, base):
            operation = ET.Element(f"{{{server.TDS}}}GetDeviceInformation")
            with post(base + "/onvif/device_service", soap(operation)) as response:
                payload = response.read()
            self.assertIn(b"Deterministic ONVIF Simulator", payload)

            operation = ET.Element(f"{{{server.TRT}}}GetSnapshotUri")
            with post(base + "/onvif/media_service", soap(operation)) as response:
                payload = response.read()
            self.assertIn((base + "/snapshot/main.png").encode(), payload)

            with urllib.request.urlopen(base + "/snapshot/main.png", timeout=2) as response:
                first = response.read()
            with urllib.request.urlopen(base + "/snapshot/main.png", timeout=2) as response:
                second = response.read()
            self.assertTrue(first.startswith(b"\x89PNG\r\n\x1a\n"))
            self.assertNotEqual(hashlib.sha256(first).digest(), hashlib.sha256(second).digest())

            with urllib.request.urlopen(base + "/admin/state", timeout=2) as response:
                state = json.load(response)
            self.assertEqual(state["captureOrdinal"], 2)

    def test_basic_auth_challenge_and_success(self):
        override = {"auth": {"mode": "basic", "username": "u", "password": "p"}}
        with running(override) as (_state, base):
            operation = soap(ET.Element(f"{{{server.TDS}}}GetDeviceInformation"))
            with self.assertRaises(urllib.error.HTTPError) as raised:
                post(base + "/onvif/device_service", operation)
            self.assertEqual(raised.exception.code, 401)
            self.assertTrue(raised.exception.headers["WWW-Authenticate"].startswith("Basic "))
            raised.exception.close()

            authorization = "Basic " + base64.b64encode(b"u:p").decode()
            with post(
                base + "/onvif/device_service",
                operation,
                {"Authorization": authorization},
            ) as response:
                self.assertEqual(response.status, 200)

    def test_http_digest_challenge_and_success(self):
        override = {
            "auth": {
                "mode": "digest",
                "algorithm": "MD5",
                "username": "u",
                "password": "p",
            }
        }
        with running(override) as (_state, base):
            passwords = urllib.request.HTTPPasswordMgrWithDefaultRealm()
            passwords.add_password(None, base, "u", "p")
            opener = urllib.request.build_opener(urllib.request.HTTPDigestAuthHandler(passwords))
            with opener.open(base + "/snapshot/main.png", timeout=2) as response:
                payload = response.read()
            self.assertEqual(response.status, 200)
            self.assertTrue(payload.startswith(b"\x89PNG\r\n\x1a\n"))

    def test_wsse_password_digest_is_checked(self):
        override = {"auth": {"mode": "wsse-digest", "username": "u", "password": "p"}}
        with running(override) as (_state, base):
            operation = ET.Element(f"{{{server.TDS}}}GetDeviceInformation")
            envelope = ET.fromstring(soap(operation))
            header = envelope.find(f"{{{server.SOAP}}}Header")
            security = ET.SubElement(header, f"{{{server.WSSE}}}Security")
            token = ET.SubElement(security, f"{{{server.WSSE}}}UsernameToken")
            ET.SubElement(token, f"{{{server.WSSE}}}Username").text = "u"
            nonce = b"0123456789abcdef"
            created = "2026-07-10T12:00:00Z"
            digest = base64.b64encode(hashlib.sha1(nonce + created.encode() + b"p").digest()).decode()
            ET.SubElement(token, f"{{{server.WSSE}}}Password").text = digest
            ET.SubElement(token, f"{{{server.WSSE}}}Nonce").text = base64.b64encode(nonce).decode()
            ET.SubElement(token, f"{{{server.WSU}}}Created").text = created
            with post(
                base + "/onvif/device_service",
                ET.tostring(envelope, encoding="utf-8", xml_declaration=True),
            ) as response:
                self.assertEqual(response.status, 200)

    def test_media2_uses_media2_response_namespace_and_services_are_enumerated(self):
        with running() as (_state, base):
            operation = ET.Element(f"{{{server.TR2}}}GetProfiles")
            with post(base + "/onvif/media2_service", soap(operation)) as response:
                payload = response.read()
            root = ET.fromstring(payload)
            self.assertIsNotNone(root.find(f".//{{{server.TR2}}}GetProfilesResponse"))
            self.assertIsNone(root.find(f".//{{{server.TRT}}}GetProfilesResponse"))

            operation = ET.Element(f"{{{server.TDS}}}GetServices")
            with post(base + "/onvif/device_service", soap(operation)) as response:
                payload = response.read()
            self.assertIn(server.TR2.encode(), payload)
            self.assertIn(server.TPTZ.encode(), payload)

    def test_hostile_snapshot_uri_and_truncated_payload_are_reproducible(self):
        override = {"fault": {"snapshot": "hostile-uri"}}
        with running(override) as (_state, base):
            operation = ET.Element(f"{{{server.TRT}}}GetSnapshotUri")
            with post(base + "/onvif/media_service", soap(operation)) as response:
                self.assertIn(b"169.254.169.254", response.read())

        override = {"fault": {"snapshot": "truncate"}}
        with running(override) as (_state, base):
            with urllib.request.urlopen(base + "/snapshot/main.png", timeout=2) as response:
                payload = response.read()
            self.assertTrue(payload.startswith(b"\x89PNG"))
            self.assertNotIn(b"IEND", payload)

    def test_dtd_request_is_rejected(self):
        payload = b'<!DOCTYPE x [<!ENTITY e "boom">]><x>&e;</x>'
        with running() as (_state, base):
            with self.assertRaises(urllib.error.HTTPError) as raised:
                post(base + "/onvif/device_service", payload)
            self.assertEqual(raised.exception.code, 400)
            raised.exception.close()

    def test_direct_ws_discovery_correlates_probe_response(self):
        fixture = server._deep_merge(server.DEFAULT_FIXTURE, {"publicBaseUrl": "http://camera.test:8080"})
        state = server.SimulatorState(fixture)
        responder = server.DiscoveryResponder(state, "127.0.0.1", 0, False)
        responder.start()
        self.assertTrue(responder.ready.wait(timeout=2))
        message_id = "urn:uuid:11111111-2222-3333-4444-555555555555"
        root = ET.Element(f"{{{server.SOAP}}}Envelope")
        header = ET.SubElement(root, f"{{{server.SOAP}}}Header")
        ET.SubElement(header, f"{{{server.WSA}}}MessageID").text = message_id
        body = ET.SubElement(root, f"{{{server.SOAP}}}Body")
        ET.SubElement(body, f"{{{server.WSD}}}Probe")
        client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        client.settimeout(2)
        try:
            client.sendto(ET.tostring(root), ("127.0.0.1", responder.port))
            response, _address = client.recvfrom(65_535)
        finally:
            client.close()
            responder.stop()
            responder.join(timeout=2)
        parsed = ET.fromstring(response)
        self.assertEqual(parsed.findtext(f".//{{{server.WSA}}}RelatesTo"), message_id)
        self.assertEqual(
            parsed.findtext(f".//{{{server.WSD}}}XAddrs"),
            "http://camera.test:8080/onvif/device_service",
        )


if __name__ == "__main__":
    unittest.main()
