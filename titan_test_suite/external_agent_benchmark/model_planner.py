"""Small dependency-free model client used by external benchmark adapters."""
from __future__ import annotations

import json
import urllib.request


class GeminiFunctionPlanner:
    def __init__(self, api_key: str, model: str = "gemini-2.5-flash"):
        if not api_key:
            raise ValueError("Gemini API key is required")
        self._api_key = api_key
        self.model = model.removeprefix("models/")

    @staticmethod
    def validate_calls(value: object, functions: list[dict]) -> list[dict]:
        if not isinstance(value, dict) or not isinstance(value.get("calls"), list):
            raise ValueError("planner response must be an object containing a calls array")
        schemas = {item["name"]: item for item in functions}
        calls = []
        for raw in value["calls"]:
            if not isinstance(raw, dict) or raw.get("name") not in schemas:
                raise ValueError("planner selected an unknown function")
            arguments = raw.get("arguments")
            if not isinstance(arguments, dict):
                raise ValueError("each function call must contain an arguments object")
            expected = schemas[raw["name"]].get("parameters", {})
            unknown = set(arguments) - set(expected)
            if unknown:
                raise ValueError("unknown arguments: " + ", ".join(sorted(unknown)))
            missing = set(expected) - set(arguments)
            if missing:
                raise ValueError("missing arguments: " + ", ".join(sorted(missing)))
            calls.append({"name": raw["name"], "arguments": arguments})
        if not calls:
            raise ValueError("planner returned no function calls")
        return calls

    def plan(self, question: str, functions: list[dict]) -> list[dict]:
        # Gemini's structured-output decoder will otherwise treat an unconstrained
        # OBJECT as empty.  BFCL parallel cases use one function signature per
        # question, so describe that signature explicitly in the response schema.
        parameter_types = {"string": "STRING", "integer": "INTEGER",
                           "float": "NUMBER", "number": "NUMBER",
                           "boolean": "BOOLEAN"}
        argument_properties: dict[str, dict] = {}
        for function in functions:
            for name, declared_type in function.get("parameters", {}).items():
                argument_properties[name] = {
                    "type": parameter_types.get(str(declared_type).lower(), "STRING")
                }
        required_arguments = (list(argument_properties)
                              if len({function["name"] for function in functions}) == 1
                              else [])
        arguments_schema: dict = {
            "type": "OBJECT",
            "properties": argument_properties,
        }
        if required_arguments:
            arguments_schema["required"] = required_arguments
        prompt = (
            "Select every function call required to satisfy the user request. Calls that do not "
            "depend on each other should all be returned in one calls array so they may run in parallel. "
            "Use only the supplied functions and preserve argument types. Return JSON only.\n\n"
            + "Functions:\n" + json.dumps(functions, indent=2)
            + "\n\nUser request:\n" + question
        )
        body = json.dumps({
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": 0,
                "maxOutputTokens": 1024,
                "responseMimeType": "application/json",
                "responseSchema": {
                    "type": "OBJECT",
                    "properties": {"calls": {"type": "ARRAY", "items": {
                        "type": "OBJECT",
                        "properties": {
                            "name": {"type": "STRING", "enum": [
                                function["name"] for function in functions
                            ]},
                            "arguments": arguments_schema,
                        },
                        "required": ["name", "arguments"],
                    }}},
                    "required": ["calls"],
                },
            },
        }).encode("utf-8")
        request = urllib.request.Request(
            f"https://generativelanguage.googleapis.com/v1beta/models/{self.model}:generateContent",
            data=body,
            headers={"x-goog-api-key": self._api_key, "Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=60) as response:
            result = json.load(response)
        candidate = (result.get("candidates") or [{}])[0]
        parts = (candidate.get("content") or {}).get("parts") or []
        if not parts or not isinstance(parts[0].get("text"), str):
            raise RuntimeError("Gemini returned no structured planner response")
        return self.validate_calls(json.loads(parts[0]["text"]), functions)
