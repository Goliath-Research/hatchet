"""Shared checks for the worker health-server idle-connection tests.

This module does not import the SDK. The current and legacy workers both
register a protobuf file named dispatcher.proto, so one process cannot
import both.
"""

from __future__ import annotations

import asyncio

import pytest
from aiohttp.web_protocol import RequestHandler
from aiohttp.web_runner import BaseRunner

# Shorter than the library default (3630s) so the deadline can be observed
# here. The vulnerable server never schedules this value for a connection
# that has not finished a request, so shrinking it does not make that
# server close the socket.
KEEPALIVE_TIMEOUT = 0.4
WAIT_FOR_CLOSE = KEEPALIVE_TIMEOUT * 4

# An unfinished request. No terminating blank line, so the server has not
# received a complete request.
PARTIAL_REQUEST = b"GET /health HTTP/1.1\r\nHost: localhost\r\n"


def compress_keepalive_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    original = RequestHandler.__init__

    def __init__(
        self: RequestHandler, manager: object, *args: object, **kwargs: object
    ) -> None:
        kwargs["keepalive_timeout"] = KEEPALIVE_TIMEOUT
        original(self, manager, *args, **kwargs)

    monkeypatch.setattr(RequestHandler, "__init__", __init__)


async def bound_port(runner: BaseRunner) -> int:
    for _ in range(50):
        for address in runner.addresses:
            if isinstance(address, tuple) and len(address) >= 2:
                host = address[0]
                assert host in {"0.0.0.0", "::"}, address
                return int(address[1])
        await asyncio.sleep(0.01)
    raise AssertionError("health server did not bind a socket")


async def health_names_worker(port: int, worker_name: str | None) -> None:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
    await writer.drain()
    body = await asyncio.wait_for(reader.read(), timeout=2)
    writer.close()
    await writer.wait_closed()
    status_line = body.split(b"\r\n", 1)[0]
    assert status_line.startswith(b"HTTP/1.1 200"), body
    if worker_name is not None:
        assert worker_name.encode() in body


async def open_peer(
    port: int, payload: bytes
) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    if payload:
        writer.write(payload)
        await writer.drain()
    return reader, writer


async def assert_server_closed(reader: asyncio.StreamReader) -> None:
    try:
        data = await asyncio.wait_for(reader.read(64), timeout=0.5)
    except (ConnectionResetError, BrokenPipeError):
        return
    except TimeoutError:
        pytest.fail(
            "health server left a connection open past keepalive timeout "
            "before a complete request was received"
        )
    assert data == b"", data


async def idle_connections_are_closed(
    runner: BaseRunner, port: int, payload: bytes, worker_name: str | None
) -> None:
    peers: list[tuple[asyncio.StreamReader, asyncio.StreamWriter]] = []
    try:
        await health_names_worker(port, worker_name)
        peers = [await open_peer(port, payload) for _ in range(4)]
        await asyncio.sleep(0)
        assert runner.server is not None
        assert len(runner.server.connections) >= len(peers)
        await asyncio.sleep(WAIT_FOR_CLOSE)
        for reader, _writer in peers:
            await assert_server_closed(reader)
    finally:
        for _reader, writer in peers:
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass
        await runner.cleanup()
