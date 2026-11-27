#!/usr/bin/env python3
"""Create disposable loopback development credentials/configuration, using OpenSSL."""
import base64
import json
import os
from pathlib import Path
import secrets
import subprocess
import time

os.umask(0o077)
root = Path(".rescrow-http-dev")
root.mkdir(mode=0o700)  # Never overwrite existing credentials.
private = root / "signing.pem"
subprocess.run(
    ["openssl", "genpkey", "-algorithm", "ED25519", "-out", str(private)], check=True
)
public = subprocess.check_output(
    ["openssl", "pkey", "-in", str(private), "-pubout"], text=True
)
issuer = "https://dev.rescrow.invalid"
config = {
    "listen": "127.0.0.1:8080",
    "backend": {"type": "file", "path": ".rescrow"},
    "api": {
        "serving_region": "local",
        "deployment_cell": "development",
        "required_profile": "local",
        "cursor_key": secrets.token_hex(32),
    },
    "issuers": [
        {
            "issuer": issuer,
            "audience": "rescrow",
            "keys": [
                {"kid": "development", "algorithm": "EdDSA", "public_key_pem": public}
            ],
        }
    ],
    "tls": None,
}
(root / "config.json").write_text(json.dumps(config, indent=2) + "\n")


def segment(value):
    return base64.urlsafe_b64encode(
        json.dumps(value, separators=(",", ":")).encode()
    ).rstrip(b"=")


header = {"alg": "EdDSA", "typ": "JWT", "kid": "development"}
claims = {
    "iss": issuer,
    "aud": "rescrow",
    "sub": "development-admin",
    "exp": int(time.time()) + 3600,
    "tenant": "local",
    "namespace": None,
    "credential_id": "development-admin",
    "scopes": [
        "resource:create",
        "transaction:prepare",
        "transaction:commit",
        "transaction:abort",
        "read",
    ],
    "resources": [
        {
            "resource_prefix": "*",
            "debit": True,
            "credit": True,
            "max_amount": str(2**127 - 1),
        }
    ],
    "max_hold_ms": None,
}
message = segment(header) + b"." + segment(claims)
message_file = root / "message"
message_file.write_bytes(message)
signature = subprocess.check_output(
    [
        "openssl",
        "pkeyutl",
        "-sign",
        "-rawin",
        "-inkey",
        str(private),
        "-in",
        str(message_file),
    ]
)
message_file.unlink()
(root / "token").write_bytes(
    message + b"." + base64.urlsafe_b64encode(signature).rstrip(b"=") + b"\n"
)
print("Created .rescrow-http-dev/config.json and a one-hour development token.")
