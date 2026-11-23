"""Idle connections to the worker health server must be closed.

aiohttp 3.11.14, the version pinned in poetry.lock, does not arm its
keepalive timeout until a request has been fully handled. A peer that
opens a connection and sends no request, or stalls after a partial
request, keeps that connection until the peer closes it.

These tests start the SDK health servers the same way an enabled worker
does and require those connections to be closed within the server's
keepalive timeout. They fail on the pinned aiohttp and pass once that
timeout is enforced from the moment the connection is accepted.
"""

from __future__ import annotations

import socket
import subprocess
import sys
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest
from aiohttp.web_runner import BaseRunner
from prometheus_client import CollectorRegistry, Gauge

from hatchet_sdk.config import HealthcheckConfig
from hatchet_sdk.worker.worker import Worker, WorkerStatus
from tests.healthcheck_idle_timeout_support import (
    PARTIAL_REQUEST,
    bound_port,
    compress_keepalive_timeout,
    idle_connections_are_closed,
)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _gauge(name: str) -> Gauge:
    return Gauge(name, "test health server", registry=CollectorRegistry())


@pytest.fixture
def short_keepalive(monkeypatch: pytest.MonkeyPatch) -> None:
    compress_keepalive_timeout(monkeypatch)


async def _start_current_worker(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[BaseRunner, int]:
    monkeypatch.setenv("HATCHET_CLIENT_WORKER_HEALTHCHECK_ENABLED", "True")
    monkeypatch.setenv("HATCHET_CLIENT_WORKER_HEALTHCHECK_PORT", str(_free_port()))
    health = HealthcheckConfig()
    assert health.enabled is True

    worker = Worker.__new__(Worker)
    worker.config = SimpleNamespace(healthcheck=health)
    worker.name = "idle-probe"
    worker.slots = 1
    worker.labels = {}
    worker.action_registry = {}
    worker._status = WorkerStatus.HEALTHY
    worker.worker_status_gauge = _gauge(f"hatchet_worker_status_{uuid.uuid4().hex}")

    captured: list[BaseRunner] = []
    from aiohttp import web

    original_init = web.AppRunner.__init__

    def init(self: web.AppRunner, *args: object, **kwargs: object) -> None:
        original_init(self, *args, **kwargs)
        captured.append(self)

    monkeypatch.setattr(web.AppRunner, "__init__", init)

    # Same branch Worker._aio_start uses once the healthcheck setting is on.
    assert worker.config.healthcheck.enabled
    await worker._start_health_server()
    assert captured, "health server was not started"
    runner = captured[-1]
    return runner, await bound_port(runner)


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [b"", PARTIAL_REQUEST])
async def test_current_worker_closes_prerequest_idle_connections(
    short_keepalive: None,
    monkeypatch: pytest.MonkeyPatch,
    payload: bytes,
) -> None:
    runner, port = await _start_current_worker(monkeypatch)
    await idle_connections_are_closed(runner, port, payload, "idle-probe")


def test_v0_worker_closes_prerequest_idle_connections() -> None:
    """The legacy worker starts the same server. Run it in a subprocess.

    Importing hatchet_sdk.worker and hatchet_sdk.v0 in one process raises
    a duplicate protobuf descriptor error.
    """

    script = Path(__file__).with_name("run_v0_healthcheck_idle_timeout.py")
    completed = subprocess.run(
        [sys.executable, str(script)],
        cwd=Path(__file__).resolve().parents[1],
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        pytest.fail(
            "v0 health server left pre-request connections open\n"
            f"{completed.stdout}\n{completed.stderr}"
        )
