#!/usr/bin/env python3
"""Deterministic OpenAI compatible embedding mock for the AI_EMBED mysqltests.

It serves the embeddings interface used by the AI_EMBED function:

  * POST /v1/embeddings -> {"model": ..., "input": [...], "data": [...]}

Every answer is derived from the request, the first element of an embedding
holds the length of the input it belongs to. A row which is answered with the
embedding of another row is therefore visible in the result of the test.

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
    return "/tmp/seekdb_ai_embed_batch_mock_%d" % port


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
    if os.path.exists(pid_path):
        try:
            with open(pid_path, "r", encoding="ascii") as stream:
                pid = int(stream.read().strip())
            os.kill(pid, signal.SIGTERM)
        except (ValueError, ProcessLookupError):
            pass
        try:
            os.unlink(pid_path)
        except FileNotFoundError:
            pass
    # The port must be free before a new mock is started on it.
    for _ in range(250):
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.05):
                pass
        except OSError:
            return
        time.sleep(0.02)
    raise RuntimeError("AI embed batch mock did not stop on port %d" % port)


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
    raise RuntimeError("AI embed batch mock did not start on port %d" % port)


def serve(port):
    lock = threading.Lock()
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

            values = body.get("input", [])
            if isinstance(values, str):
                values = [values]
            if not isinstance(values, list):
                values = []

            joined = " ".join(str(value) for value in values)
            if "count-mismatch" in joined:
                # One answer less than the number of the inputs, the client
                # must not shift the answers of the rows.
                answered = values[:-1]
            else:
                answered = values
            dimension = body.get("dimensions")
            if not isinstance(dimension, int) or dimension < 2:
                dimension = 2
            data = [
                {"object": "embedding", "index": index,
                 "embedding": [float(len(str(value))), 0.25]
                             + [0.5] * (dimension - 2)}
                for index, value in enumerate(answered)
            ]
            response = {
                "object": "list",
                "model": body.get("model", ""),
                "data": data,
            }
            event = {
                "path": parsed.path,
                "status": 200,
                "model": body.get("model", ""),
                "input": [str(value) for value in values],
                "dimensions": body.get("dimensions", None),
                "body_keys": sorted(body.keys()),
                "authorization": self.headers.get("Authorization", ""),
                "content_type": self.headers.get("Content-Type", ""),
            }

            with lock:
                with open(log_path, "a", encoding="utf-8") as stream:
                    stream.write(json.dumps(event, ensure_ascii=False) + "\n")
            payload = json.dumps(response, ensure_ascii=False).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()


def embedding_events(events):
    return [event for event in events if event["path"].endswith("/v1/embeddings")]


def assert_events(port, assertion, expected):
    events = read_events(port)
    requests = embedding_events(events)
    if assertion == "no-requests":
        ok = not requests
    elif assertion == "single-request":
        # The rows of one batch are merged into a single model request.
        ok = len(requests) == 1
    elif assertion == "request-count":
        ok = len(requests) == int(expected[0])
    elif assertion == "inputs":
        # The request carries the contents of the rows, the null rows are not
        # sent to the model service.
        sent = []
        for event in requests:
            sent.extend(event["input"])
        ok = sent == list(expected)
    elif assertion == "dimensions":
        ok = any(event.get("dimensions") == int(expected[0]) for event in requests)
    else:
        raise ValueError("unknown assertion: " + assertion)
    if not ok:
        raise AssertionError("assertion %s failed: %r" % (assertion, events))
    print("mock_%s=ok" % assertion.replace("-", "_"))


def main():
    if len(sys.argv) < 3:
        raise SystemExit(
            "usage: ai_embed_batch_mock.py MODE PORT [ASSERTION [EXPECTED]]")
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
        expected = sys.argv[4].split(",") if len(sys.argv) > 4 else []
        assert_events(port, sys.argv[3], expected)
    else:
        raise SystemExit("unknown mode: " + mode)


if __name__ == "__main__":
    main()
