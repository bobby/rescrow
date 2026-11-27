"""Bounded standard-library transport. No redirects or inferred endpoint discovery."""

import http.client
import ipaddress
import json
import secrets
import ssl
import time
import urllib.parse
import uuid


def new_id():
    """Generate once, then persist alongside the request before any attempt."""
    millis = time.time_ns() // 1_000_000
    return str(
        uuid.UUID(
            int=(millis << 80)
            | (7 << 76)
            | (secrets.randbits(12) << 64)
            | (2 << 62)
            | secrets.randbits(62)
        )
    )


def path_id(value):
    if (
        not isinstance(value, str)
        or not 1 <= len(value) <= 128
        or any(
            c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"
            for c in value
        )
    ):
        raise ValueError("invalid object ID")
    return {".": "~Lg", "..": "~Li4"}.get(value, value)


class ProblemError(Exception):
    def __init__(self, problem):
        self.problem = problem
        super().__init__(problem.get("detail", "remote request failed"))


def _object(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate JSON key")
        value[key] = item
    return value


def _generation(value):
    try:
        parsed = uuid.UUID(value)
        return str(parsed) == value and parsed.version == 7
    except (ValueError, TypeError, AttributeError):
        return False


def _unknown(key):
    return ProblemError(
        {
            "type": "urn:rescrow:problem:deadline_exceeded",
            "category": "unknown_outcome",
            "code": "deadline_exceeded",
            "status": 503,
            "detail": "Outcome unconfirmed; inspect or retry only the identical identity and request.",
            "request_id": "client",
            "transaction_id": key,
            "retryable": True,
        }
    )


class Transport:
    def __init__(
        self,
        endpoints,
        token,
        *,
        authority_generation=None,
        require_profile="simulation",
        attempt_timeout=5.0,
        overall_timeout=30.0,
        max_attempts=8,
    ):
        if isinstance(endpoints, str):
            endpoints = [endpoints]
        if (
            not 1 <= len(endpoints) <= 8
            or not token
            or len(token) > 16384
            or any(ord(c) < 32 or ord(c) == 127 for c in token)
        ):
            raise ValueError("invalid endpoints or bearer")
        if authority_generation is not None and not _generation(authority_generation):
            raise ValueError("invalid authority generation")
        if len(endpoints) > 1 and authority_generation is None:
            raise ValueError(
                "alternate endpoints require a pinned authority generation"
            )
        self.endpoints = []
        for endpoint in endpoints:
            url = urllib.parse.urlsplit(endpoint)
            _ = url.port  # Validate the configured port before any business request.
            try:
                loopback = ipaddress.ip_address(url.hostname).is_loopback
            except ValueError:
                loopback = url.hostname == "localhost"
            if url.scheme != "https" and not (url.scheme == "http" and loopback):
                raise ValueError("HTTPS or loopback HTTP required")
            if (
                not url.hostname
                or url.username
                or url.password
                or url.path not in ("", "/")
                or url.query
                or url.fragment
            ):
                raise ValueError("endpoint must be an origin")
            self.endpoints.append(url)
        profiles = ["simulation", "local", "regional", "enterprise-mr"]
        if (
            require_profile not in profiles
            or not 0 < attempt_timeout <= overall_timeout <= 300
            or not 1 <= max_attempts <= 32
        ):
            raise ValueError("invalid profile or retry bounds")
        self.token = token
        self.generation = authority_generation
        self.profile = require_profile
        self.attempt_timeout = attempt_timeout
        self.overall_timeout = overall_timeout
        self.max_attempts = max_attempts
        self._tls = ssl.create_default_context()

    def request(
        self, method, path, body=None, key=None, query=None, transaction_id=None
    ):
        identity = key or transaction_id
        if key is not None and not _generation(key):
            raise ValueError("Idempotency-Key must be a canonical UUIDv7")
        # Capture bytes exactly once, before the first attempt.
        payload = (
            None
            if body is None
            else json.dumps(
                body, ensure_ascii=False, separators=(",", ":"), allow_nan=False
            ).encode()
        )
        if query:
            path += "?" + urllib.parse.urlencode(query)
        deadline = time.monotonic() + self.overall_timeout
        last = _unknown(identity)
        for attempt in range(self.max_attempts):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            url = self.endpoints[attempt % len(self.endpoints)]
            timeout = min(self.attempt_timeout, remaining)
            connection = (
                http.client.HTTPSConnection(
                    url.hostname, url.port, timeout=timeout, context=self._tls
                )
                if url.scheme == "https"
                else http.client.HTTPConnection(url.hostname, url.port, timeout=timeout)
            )
            delay = 0
            try:
                headers = {
                    "Authorization": "Bearer " + self.token,
                    "Require-Durability-Profile": self.profile,
                    "Accept": "application/json",
                }
                if self.generation:
                    headers["Require-Authority-Generation"] = self.generation
                if payload is not None:
                    headers["Content-Type"] = "application/json"
                if key:
                    headers["Idempotency-Key"] = key
                attempt_end = min(deadline, time.monotonic() + self.attempt_timeout)
                connection.request(method, path, payload, headers)
                response = connection.getresponse()
                generation = response.getheader("Authority-Generation")
                if not _generation(generation):
                    raise _unknown(identity)
                if self.generation and self.generation != generation:
                    raise ProblemError(
                        {
                            "code": "conflict",
                            "detail": "Endpoint belongs to a different authority generation.",
                            "retryable": False,
                            "transaction_id": identity,
                        }
                    )
                self.generation = generation
                chunks = []
                size = 0
                while True:
                    remaining = attempt_end - time.monotonic()
                    if remaining <= 0:
                        raise TimeoutError()
                    if connection.sock:
                        connection.sock.settimeout(remaining)
                    chunk = response.read1(65536)
                    if not chunk:
                        break
                    size += len(chunk)
                    if size > 16 * 1024 * 1024:
                        raise _unknown(identity)
                    chunks.append(chunk)
                value = json.loads(b"".join(chunks), object_pairs_hook=_object)
                if response.status in (200, 201, 202):
                    profiles = ["simulation", "local", "regional", "enterprise-mr"]
                    meta = value["metadata"]
                    if meta["authority_generation"] != generation or profiles.index(
                        meta["durability_profile"]
                    ) < profiles.index(self.profile):
                        raise _unknown(identity)
                    if response.status != 202:
                        return value
                    last = _unknown(identity)
                else:
                    if value.get("status") != response.status:
                        raise _unknown(identity)
                    if value.get("transaction_id") is None and identity is not None:
                        value["transaction_id"] = identity
                    last = ProblemError(value)
                    if response.status not in (429, 503) or not value.get("retryable"):
                        raise last
                after = response.getheader("Retry-After", "0")
                delay = int(after) if after.isascii() and after.isdigit() else 0
            except ProblemError as error:
                if error.problem.get("code") not in (
                    "deadline_exceeded",
                    "unknown_outcome",
                ):
                    raise
                last = error
            except (
                OSError,
                http.client.HTTPException,
                ValueError,
                KeyError,
                TypeError,
            ):
                last = _unknown(identity)
            finally:
                connection.close()
            if attempt + 1 == self.max_attempts:
                break
            delay = max(delay, (50 + secrets.randbelow(50 << min(attempt, 4))) / 1000)
            if delay >= deadline - time.monotonic():
                break
            time.sleep(delay)
        raise last
