"""Exercise the legacy worker health server. Not collected by pytest."""

from __future__ import annotations

import asyncio
import pathlib
import socket
import sys
import types
import uuid

from aiohttp.web_runner import BaseRunner
from prometheus_client import CollectorRegistry, Gauge

# hatchet_sdk/__init__.py and hatchet_sdk/v0/__init__.py both register
# dispatcher.proto. Load the legacy package without running the parent init.
_sdk = types.ModuleType("hatchet_sdk")
_sdk.__path__ = [
    str(pathlib.Path(__file__).resolve().parents[1] / "hatchet_sdk")
]
_sdk.__package__ = "hatchet_sdk"
sys.modules["hatchet_sdk"] = _sdk

from hatchet_sdk.v0.loader import ClientConfig
from hatchet_sdk.v0.worker.worker import Worker, WorkerStatus
from tests.healthcheck_idle_timeout_support import (
    PARTIAL_REQUEST,
    bound_port,
    compress_keepalive_timeout,
    idle_connections_are_closed,
)


class _Patch:
    def __init__(self) -> None:
        self._undo: list[tuple[object, str, object]] = []

    def setattr(self, obj: object, name: str, value: object) -> None:
        self._undo.append((obj, name, getattr(obj, name)))
        setattr(obj, name, value)

    def undo(self) -> None:
        for obj, name, value in reversed(self._undo):
            setattr(obj, name, value)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


async def _start(patch: _Patch) -> tuple[BaseRunner, int]:
    port = _free_port()
    config = ClientConfig(
        worker_healthcheck_enabled=True,
        worker_healthcheck_port=port,
    )
    assert config.worker_healthcheck_enabled is True

    worker = Worker.__new__(Worker)
    worker.config = config
    worker._status = WorkerStatus.HEALTHY
    worker.worker_status_gauge = Gauge(
        f"hatchet_worker_status_{uuid.uuid4().hex}",
        "test health server",
        registry=CollectorRegistry(),
    )

    captured: list[BaseRunner] = []
    from aiohttp import web

    original_init = web.AppRunner.__init__

    def init(self: web.AppRunner, *args: object, **kwargs: object) -> None:
        original_init(self, *args, **kwargs)
        captured.append(self)

    patch.setattr(web.AppRunner, "__init__", init)
    compress_keepalive_timeout(patch)  # type: ignore[arg-type]

    # Same branch Worker._async_start uses once the healthcheck setting is on.
    assert worker.config.worker_healthcheck_enabled
    await worker.start_health_server()
    assert captured, "v0 health server was not started"
    runner = captured[-1]
    return runner, await bound_port(runner)


async def _main() -> None:
    failures: list[str] = []
    for payload in (b"", PARTIAL_REQUEST):
        patch = _Patch()
        try:
            runner, port = await _start(patch)
            await idle_connections_are_closed(runner, port, payload, None)
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            failures.append(f"payload={payload!r}: {exc}")
        finally:
            patch.undo()
    if failures:
        raise SystemExit("\n".join(failures))


if __name__ == "__main__":
    asyncio.run(_main())
