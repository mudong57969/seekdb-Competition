#!/usr/bin/env python3
"""Deterministic Ollama-compatible HTTP mock for the AI function mysqltests.

It serves the native Ollama interfaces used by the AI_COMPLETE and AI_EMBED
functions:

  * POST /api/generate  -> {"model": ..., "prompt": ..., "response": ...}
  * POST /api/embed     -> {"model": ..., "input": [...], "embeddings": [[...]]}

The mock never contacts a real model server, every answer is derived from the
request so that the tests stay deterministic and free of charge.
"""

import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse


def state_dir(port):
    return "/tmp/seekdb_ai_ollama_mock_%d" % port


def state_path(port, name):
    return os.path.join(state_dir(port), name)


def read_events(port):
    events = []
    path = state_path(port, "events.jsonl")
    if not os.path.exists(path):
        return events
    with open(path, "r", encoding="utf-8") as stream:
        for line in stream:
            line = line.strip()
            if line:
                events.append(json.loads(line))
    return events


def stop_server(port):
    pid_path = state_path(port, "server.pid")
    if not os.path.exists(pid_path):
        return
    try:
        with open(pid_path, "r", encoding="ascii") as stream:
            pid = int(stream.read().strip())
        os.kill(pid, signal.SIGTERM)
        for _ in range(50):
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                break
            time.sleep(0.02)
    except (ValueError, ProcessLookupError):
        pass
    try:
        os.unlink(pid_path)
    except FileNotFoundError:
        pass


def start_server(port):
    os.makedirs(state_dir(port), exist_ok=True)
    stop_server(port)
    for name in ("events.jsonl", "server.out"):
        with open(state_path(port, name), "w", encoding="utf-8"):
            pass
    output = open(state_path(port, "server.out"), "a", encoding="utf-8")
    process = subprocess.Popen(
        [sys.executable, os.path.abspath(__file__), "serve", str(port)],
        stdin=subprocess.DEVNULL,
        stdout=output,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    with open(state_path(port, "server.pid"), "w", encoding="ascii") as stream:
        stream.write(str(process.pid))
    for _ in range(100):
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.05):
                return
        except OSError:
            if process.poll() is not None:
                break
            time.sleep(0.02)
    raise RuntimeError("AI ollama mock did not start on port %d" % port)


def serve(port):
    lock = threading.Lock()
    attempts = {}
    log_path = state_path(port, "events.jsonl")

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, fmt, *args):
            return

        def do_POST(self):
            size = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(size)
            try:
                body = json.loads(raw.decode("utf-8"))
            except Exception:
                body = {}
            parsed = urlparse(self.path)

            if parsed.path.endswith("/api/embed"):
                values = body.get("input", [])
                if isinstance(values, str):
                    values = [values]
                if not isinstance(values, list):
                    values = []

                def vector(value, index):
                    text = str(value)
                    if "vec-zero" in text:
                        return [0.0, 0.0, 0.0, 0.0]
                    if "vec-ten" in text:
                        return [10.0, 0.0, 0.0, 0.0]
                    if "vec-twenty" in text:
                        return [20.0, 0.0, 0.0, 0.0]
                    return [float(index + 1), 0.25, 0.5, 0.75]

                status = 200
                joined = " ".join(str(value) for value in values)
                if "embed-not-array" in joined:
                    # The embeddings member is not an array at all.
                    response = {"model": body.get("model", ""), "embeddings": "oops"}
                elif "embed-element-not-array" in joined:
                    # An embedding is not an array of numbers.
                    response = {"model": body.get("model", ""), "embeddings": [1, 2]}
                elif "embed-non-numeric" in joined:
                    # An embedding holds a value which is not a number.
                    response = {
                        "model": body.get("model", ""),
                        "embeddings": [[1.0, "x", 0.5, 0.75]],
                    }
                elif "embed-count-mismatch" in joined:
                    # More embeddings than inputs are answered.
                    response = {
                        "model": body.get("model", ""),
                        "embeddings": [[1.0, 0.0, 0.0, 0.0], [2.0, 0.0, 0.0, 0.0]],
                    }
                elif "embed-missing" in joined:
                    # The embeddings member is missing at all.
                    response = {"model": body.get("model", "")}
                else:
                    response = {
                        "model": body.get("model", ""),
                        "embeddings": [
                            vector(value, index) for index, value in enumerate(values)
                        ],
                    }
                event = {
                    "path": parsed.path,
                    "status": status,
                    "model": body.get("model", ""),
                    "input": values,
                    "options": body.get("options", {}),
                    "dimensions": body.get("dimensions", None),
                    "body_keys": sorted(body.keys()),
                    "authorization": self.headers.get("Authorization", ""),
                    "content_type": self.headers.get("Content-Type", ""),
                }
            else:
                prompt = str(body.get("prompt", ""))
                key = parsed.path + "|" + prompt
                with lock:
                    attempts[key] = attempts.get(key, 0) + 1
                    attempt = attempts[key]

                if "boom" in prompt:
                    status = 500
                    response = {"error": "injected failure"}
                elif "not-done" in prompt:
                    status = 200
                    response = {
                        "model": body.get("model", ""),
                        "response": "partial",
                        "done": False,
                    }
                elif "no-response" in prompt:
                    status = 200
                    response = {"model": body.get("model", ""), "done": True}
                elif "response-not-string" in prompt:
                    # The generated text is not a string.
                    status = 200
                    response = {
                        "model": body.get("model", ""),
                        "response": 123,
                        "done": True,
                    }
                elif "server-error" in prompt:
                    # The model server reports a failure in the error member.
                    status = 200
                    response = {
                        "model": body.get("model", ""),
                        "error": "model not found",
                        "done": True,
                    }
                else:
                    status = 200
                    response = {
                        "model": body.get("model", ""),
                        "response": "mock:" + prompt,
                        "done": True,
                    }
                event = {
                    "path": parsed.path,
                    "status": status,
                    "model": body.get("model", ""),
                    "prompt": prompt,
                    "stream": body.get("stream", None),
                    "options": body.get("options", {}),
                    "authorization": self.headers.get("Authorization", ""),
                    "content_type": self.headers.get("Content-Type", ""),
                    "attempt": attempt,
                }

            with lock:
                with open(log_path, "a", encoding="utf-8") as stream:
                    stream.write(json.dumps(event, ensure_ascii=False) + "\n")
            payload = json.dumps(response, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()


def assert_events(port, assertion):
    events = read_events(port)
    if assertion == "no-requests":
        ok = not events
    elif assertion == "no-authorization":
        ok = bool(events) and all(
            event["authorization"] == "" for event in events
        )
    elif assertion == "content-type-json":
        ok = bool(events) and all(
            event["content_type"] == "application/json" for event in events
        )
    elif assertion == "generate":
        ok = any(event["path"].endswith("/api/generate") for event in events)
    elif assertion == "embed":
        ok = any(event["path"].endswith("/api/embed") for event in events)
    elif assertion == "stream-disabled":
        ok = any(
            event["path"].endswith("/api/generate") and event["stream"] is False
            for event in events
        )
    elif assertion == "options-merged":
        ok = any(
            event["path"].endswith("/api/generate")
            and isinstance(event["options"], dict)
            and event["options"].get("temperature") == 0.2
            for event in events
        )
    elif assertion == "embed-inputs":
        seen = set()
        for event in events:
            if event["path"].endswith("/api/embed"):
                for value in event["input"]:
                    seen.add(str(value))
        ok = seen.issuperset({"vec-zero", "vec-ten", "vec-twenty"})
    elif assertion == "embed-dimensions":
        # The dimension is a member of the request body itself, it is not a
        # generation option.
        ok = any(
            event["path"].endswith("/api/embed")
            and event.get("dimensions") == 4
            and event.get("options") == {}
            and event.get("body_keys") == ["dimensions", "input", "model"]
            for event in events
        )
    elif assertion == "embed-reserved-keys":
        # The input member belongs to the provider, the user config must not
        # overwrite it.
        ok = any(
            event["path"].endswith("/api/embed")
            and event["input"] == ["reserved"]
            and event.get("body_keys") == ["input", "model"]
            for event in events
        )
    elif assertion == "complete-reserved-keys":
        # The model, the prompt and the stream members belong to the provider,
        # the user config must not overwrite them.
        ok = any(
            event["path"].endswith("/api/generate")
            and event["model"] == "qwen2.5"
            and event["prompt"] == "reserved"
            and event["stream"] is False
            for event in events
        )
    else:
        raise ValueError("unknown assertion: " + assertion)
    if not ok:
        raise AssertionError("assertion %s failed: %r" % (assertion, events))
    print("mock_%s=ok" % assertion.replace("-", "_"))


def main():
    if len(sys.argv) < 3:
        raise SystemExit("usage: ai_ollama_mock.py MODE PORT [ASSERTION]")
    mode = sys.argv[1]
    port = int(sys.argv[2])
    if mode == "start":
        start_server(port)
    elif mode == "stop":
        stop_server(port)
    elif mode == "serve":
        serve(port)
    elif mode == "reset":
        with open(state_path(port, "events.jsonl"), "w", encoding="utf-8"):
            pass
    elif mode == "assert":
        assert_events(port, sys.argv[3])
    else:
        raise SystemExit("unknown mode: " + mode)


if __name__ == "__main__":
    main()
