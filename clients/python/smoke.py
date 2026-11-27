"""Real-server lifecycle and replay; also the Python usage example."""

import os
from rescrow_client import Client, ProblemError, new_id

client = Client(
    os.environ["RESCROW_ENDPOINT"],
    os.environ["RESCROW_TOKEN"],
    authority_generation=os.environ["RESCROW_GENERATION"],
)
resource = "python-credits"
spec = {"lower": "0", "upper": "100", "initial": "60", "unit": "credits", "scale": 0}
client.create_resource(resource, spec)
key = new_id()  # Persist key and intent before I/O. Never replace on timeout.
request = {
    "operations": [{"operation_id": "op", "resource_id": resource, "delta": "-40"}],
    "hold_ms": None,
    "context": {},
}
first = client.prepare(request, idempotency_key=key)
assert client.prepare(request, idempotency_key=key)["data"] == first["data"]
actuals = {"actuals": [{"operation_id": "op", "delta": "-25"}]}
client.commit(key, actuals)
client.commit(key, actuals)
assert client.get_resource(resource)["data"]["observation"]["quantity"] == "35"
try:
    client.commit(key, {"actuals": [{"operation_id": "op", "delta": "-24"}]})
    raise AssertionError("changed actuals were accepted")
except ProblemError as error:
    assert error.problem["code"] == "conflict"
assert client.list_transactions()["data"]["items"]
print("Python lifecycle, stable key, partial settlement and conflict passed")
