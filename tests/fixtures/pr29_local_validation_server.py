from __future__ import annotations

import json
import os
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


HOST = "127.0.0.1"
PORT = int(os.environ["PR29_OFFLINE_PORT"])
SANDBOX_DATABASE = os.environ["PR29_OFFLINE_SANDBOX_DB"]


class Handler(BaseHTTPRequestHandler):
    server_version = "PR29OfflineHarness/1.0"

    def _send_json(self, status: HTTPStatus, payload: dict[str, object]) -> None:
        encoded = (
            json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n"
        ).encode("utf-8")
        self.send_response(status.value)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/health":
            self._send_json(HTTPStatus.OK, {"status": "healthy"})
            return
        if self.path == "/controls":
            self._send_json(
                HTTPStatus.OK,
                {
                    "database_path": SANDBOX_DATABASE,
                    "external_provider_network_enabled": False,
                    "fixture_mode": True,
                },
            )
            return
        self._send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})

    def do_POST(self) -> None:  # noqa: N802
        if self.path != "/mock/acquire":
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})
            return
        raw_ids = [
            "fixture:alpha-vantage:1",
            "fixture:gdelt:1",
            "fixture:rss:1",
        ]
        self._send_json(
            HTTPStatus.OK,
            {
                "provider_network_calls": 0,
                "raw_in_scope_ids": raw_ids,
                "parsed_ids": raw_ids,
                "technically_rejected_ids": [],
                "persisted_ids": raw_ids,
                "persistence_rejected_ids": [],
                "explicit_outside_scope_ids": [],
                "delivered_ids": raw_ids,
                "quarantined_ids": [],
                "withheld_ids": [],
            },
        )

    def log_message(self, format: str, *args: object) -> None:
        return


def main() -> None:
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print(
        json.dumps(
            {
                "event": "offline_server_started",
                "host": HOST,
                "port": PORT,
            },
            sort_keys=True,
        ),
        flush=True,
    )
    server.serve_forever(poll_interval=0.1)


if __name__ == "__main__":
    main()
