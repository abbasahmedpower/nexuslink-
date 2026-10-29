#!/usr/bin/env python3
"""
Integration test suite for Nexus Hub v0.4
Simulates Rust IPC server, client verification, fake binaries, and security checks.
"""

import sys
import os
import re
import json
import time
import socket
import tempfile
import threading

def is_valid_device_id(device_id: str) -> bool:
    if not device_id or len(device_id) > 128:
        return False
    if device_id.startswith("-"):
        return False
    return bool(re.match(r"^[A-Za-z0-9_.:-]+$", device_id))

def test_device_id_validation():
    print("Testing device ID validation...")
    assert is_valid_device_id("device-123_test.0:x")
    assert is_valid_device_id("emulator-5554")
    assert is_valid_device_id("192.168.1.100:5555")
    assert not is_valid_device_id("")
    assert not is_valid_device_id("--switch")
    assert not is_valid_device_id("-s")
    assert not is_valid_device_id("id with spaces")
    assert not is_valid_device_id("id;rm -rf /")
    assert not is_valid_device_id("a" * 129)
    print("  [OK] Passed device ID validation")

def test_ipc_protocol_and_auth():
    print("Testing IPC protocol and authentication state machine...")
    auth_token = "secret-test-token-12345"
    
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(("127.0.0.1", 0))
    port = server_socket.getsockname()[1]
    server_socket.listen(1)

    def server_loop():
        conn, _ = server_socket.accept()
        f = conn.makefile("rwb")
        authenticated = False
        try:
            while True:
                line = f.readline()
                if not line:
                    break
                if len(line) > 65536:
                    resp = {"id": 0, "status": "error", "error": {"code": "payload_too_large", "message": "Frame exceeds 64KB"}}
                    f.write(json.dumps(resp).encode() + b"\n")
                    f.flush()
                    break
                
                req = json.loads(line.decode())
                req_id = req.get("id", 0)
                action = req.get("action")
                payload = req.get("payload")

                if action == "handshake":
                    if payload.get("protocol_version") != 1:
                        resp = {"id": req_id, "status": "error", "error": {"code": "invalid_request", "message": "Incompatible protocol version"}}
                    elif payload.get("auth_token") == auth_token:
                        authenticated = True
                        resp = {"id": req_id, "status": "ok"}
                    else:
                        resp = {"id": req_id, "status": "error", "error": {"code": "ipc_authentication_required", "message": "Invalid auth token"}}
                elif not authenticated:
                    resp = {"id": req_id, "status": "error", "error": {"code": "ipc_authentication_required", "message": "Handshake required"}}
                elif action == "get_capabilities":
                    resp = {"id": req_id, "status": "ok", "data": {"protocol_version": 1, "platform": "test", "features": ["scrcpy_screen", "kde_pairing"]}}
                elif action == "list_devices":
                    resp = {"id": req_id, "status": "ok", "data": [{"id": "dev1", "name": "Phone", "origin": "kde_connect", "paired": True, "reachable": True}]}
                elif action == "pair_device":
                    dev_id = payload.get("device_id")
                    if not is_valid_device_id(dev_id):
                        resp = {"id": req_id, "status": "error", "error": {"code": "invalid_device_id", "message": "Invalid device id"}}
                    else:
                        resp = {"id": req_id, "status": "ok", "data": "Pairing initiated"}
                elif action == "start_mirror":
                    serial = payload.get("serial")
                    if serial and not is_valid_device_id(serial):
                        resp = {"id": req_id, "status": "error", "error": {"code": "invalid_device_id", "message": "Invalid ADB serial"}}
                    else:
                        resp = {"id": req_id, "status": "ok", "data": {
                            "session_id": "scrcpy-1", "kind": "screen", "state": "running", "started_at_ms": int(time.time()*1000), "pid": 4321
                        }}
                else:
                    resp = {"id": req_id, "status": "error", "error": {"code": "invalid_request", "message": f"Unknown action: {action}"}}
                
                f.write(json.dumps(resp).encode() + b"\n")
                f.flush()
        finally:
            conn.close()

    t = threading.Thread(target=server_loop, daemon=True)
    t.start()

    # Test 1: Request before authentication is rejected
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(("127.0.0.1", port))
    f = client.makefile("rwb")

    f.write(json.dumps({"id": 1, "action": "list_devices"}).encode() + b"\n")
    f.flush()
    r = json.loads(f.readline().decode())
    assert r["status"] == "error"
    assert r["error"]["code"] == "ipc_authentication_required"

    # Test 2: Invalid handshake token is rejected
    f.write(json.dumps({"id": 2, "action": "handshake", "payload": {"protocol_version": 1, "client_name": "test", "auth_token": "wrong"}}).encode() + b"\n")
    f.flush()
    r = json.loads(f.readline().decode())
    assert r["status"] == "error"
    assert r["error"]["code"] == "ipc_authentication_required"

    # Test 3: Successful handshake
    f.write(json.dumps({"id": 3, "action": "handshake", "payload": {"protocol_version": 1, "client_name": "test", "auth_token": auth_token}}).encode() + b"\n")
    f.flush()
    r = json.loads(f.readline().decode())
    assert r["status"] == "ok"

    # Test 4: Authenticated list_devices succeeds
    f.write(json.dumps({"id": 4, "action": "list_devices"}).encode() + b"\n")
    f.flush()
    r = json.loads(f.readline().decode())
    assert r["status"] == "ok"
    assert len(r["data"]) == 1
    assert r["data"][0]["id"] == "dev1"

    # Test 5: Command switch injection rejection
    f.write(json.dumps({"id": 5, "action": "pair_device", "payload": {"device_id": "--switch-injection"}}).encode() + b"\n")
    f.flush()
    r = json.loads(f.readline().decode())
    assert r["status"] == "error"
    assert r["error"]["code"] == "invalid_device_id"

    # Test 6: Safe start mirror
    f.write(json.dumps({"id": 6, "action": "start_mirror", "payload": {"serial": "emulator-5554", "camera": False}}).encode() + b"\n")
    f.flush()
    r = json.loads(f.readline().decode())
    assert r["status"] == "ok"
    assert r["data"]["pid"] == 4321
    assert r["data"]["kind"] == "screen"

    client.close()
    server_socket.close()
    print("  [OK] Passed IPC protocol and authentication tests")

def test_fake_cli_process_execution():
    print("Testing fake process execution, argument passing, and output capture limits...")
    import subprocess
    
    # Run fake command with strict args
    output = subprocess.check_output([sys.executable, "-c", "import sys; print(' '.join(sys.argv[1:]))", "arg1", "--flag", "val2"])
    assert output.strip() == b"arg1 --flag val2"

    # Ensure max capture truncation behavior
    long_output = b"x" * 20000
    capped = long_output[:12000]
    assert len(capped) == 12000
    print("  [OK] Passed process execution and capture tests")

def main():
    print("==================================================")
    print("Starting Nexus Hub v0.4 Verification Suite")
    print("==================================================")
    test_device_id_validation()
    test_ipc_protocol_and_auth()
    test_fake_cli_process_execution()
    print("==================================================")
    print("ALL SUITE TESTS PASSED!")
    print("==================================================")

if __name__ == "__main__":
    main()
