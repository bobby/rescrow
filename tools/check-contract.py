#!/usr/bin/env python3
"""Validate OpenAPI and captured responses from the real memory HTTP harness."""
import json
import sys
from pathlib import Path
from jsonschema import Draft202012Validator
from openapi_spec_validator import validate

root = Path(__file__).resolve().parents[1]
spec = json.loads((root / "openapi/rescrow-v1.json").read_text())
validate(spec)
operations = {
    op["operationId"]: op for path in spec["paths"].values() for op in path.values()
}


def check(value, schema):
    Draft202012Validator(
        {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "components": spec["components"],
            **schema,
        }
    ).validate(value)


for filename in [root / "openapi/fixtures/http.json", *map(Path, sys.argv[1:])]:
    cases = json.loads(filename.read_text())
    for case in cases:
        op = operations[case["operation"]]
        status = str(case["status"])
        assert status in op["responses"], case
        schema = op["responses"][status]
        if "$ref" in schema:
            schema = spec["components"]["responses"][schema["$ref"].split("/")[-1]]
        media = "application/json" if int(status) < 400 else "application/problem+json"
        assert case["headers"]["content-type"].split(";")[0] == media, case
        check(case["response"], schema["content"][media]["schema"])
        for name in ["x-request-id", "durability-profile", "authority-generation"]:
            assert name in case["headers"], case
        if case["request"] is not None:
            check(
                case["request"],
                op["requestBody"]["content"]["application/json"]["schema"],
            )
    print(f"{filename}: {len(cases)} real HTTP cases conform")
