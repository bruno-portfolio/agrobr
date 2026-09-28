from __future__ import annotations

import asyncio
import socket
from threading import Thread

import pytest
import pytest_socket

from tests import helpers


@pytest.mark.parametrize("family", [socket.AF_INET, socket.AF_INET6])
@pytest.mark.parametrize("kind", [socket.SOCK_STREAM, socket.SOCK_DGRAM])
def test_unit_tests_block_network_sockets(family, kind):
    with pytest.raises(pytest_socket.SocketBlockedError):
        socket.socket(family, kind)


@pytest.mark.parametrize(
    "resolve,args",
    [("getaddrinfo", ("example.invalid", 443)), ("gethostbyname", ("example.invalid",))],
)
def test_unit_tests_block_dns(resolve, args):
    with pytest.raises(pytest_socket.SocketBlockedError):
        getattr(socket, resolve)(*args)


@pytest.mark.asyncio
async def test_async_loop_thread_wakeup_keeps_network_blocked():
    loop = asyncio.get_running_loop()
    future = loop.create_future()
    thread = Thread(target=loop.call_soon_threadsafe, args=(future.set_result, "awake"))
    thread.start()
    try:
        assert await asyncio.wait_for(future, timeout=5) == "awake"
    finally:
        thread.join(timeout=5)
    assert not thread.is_alive()
    with pytest.raises(pytest_socket.SocketBlockedError):
        socket.socket(socket.AF_INET, socket.SOCK_STREAM)


def test_event_loop_repeated_creation_and_close():
    for _ in range(5):
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(asyncio.sleep(0))
        finally:
            loop.close()
        assert loop.is_closed()
    with pytest.raises(pytest_socket.SocketBlockedError):
        socket.socket(socket.AF_INET, socket.SOCK_DGRAM)


@pytest.mark.parametrize("family", [None, socket.AF_INET, socket.AF_INET6])
def test_local_socketpair_transfers_only_between_loopback_peers(family):
    first, second = helpers.local_socketpair(family, socket.SOCK_STREAM, 0)
    with first, second:
        assert first.getsockname() == second.getpeername()
        assert second.getsockname() == first.getpeername()
        assert first.getsockname()[0] in ("127.0.0.1", "::1")
        first.sendall(b"self-pipe")
        assert second.recv(32) == b"self-pipe"
    assert first.fileno() == second.fileno() == -1


@pytest.mark.parametrize(
    "args",
    [
        (socket.AF_UNSPEC, socket.SOCK_STREAM, 0),
        (socket.AF_INET, socket.SOCK_DGRAM, 0),
        (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP),
    ],
)
def test_local_socketpair_rejects_unsupported_arguments(args):
    with pytest.raises(ValueError, match="Only local TCP"):
        helpers.local_socketpair(*args)


def test_local_socketpair_closes_sockets_when_connection_fails(monkeypatch):
    real_socket = helpers._SOCKET_TYPE
    created = []

    def tracked_socket(*args, **kwargs):
        instance = real_socket(*args, **kwargs)
        created.append(instance)
        return instance

    def failing_connect(*_args):
        raise OSError("controlled connection failure")

    monkeypatch.setattr(helpers, "_SOCKET_TYPE", tracked_socket)
    monkeypatch.setattr(helpers, "_SOCKET_CONNECT", failing_connect)
    with pytest.raises(OSError, match="controlled connection failure"):
        helpers.local_socketpair()
    assert len(created) == 2
    assert all(instance.fileno() == -1 for instance in created)


def test_local_socketpair_closes_sockets_when_peer_mismatches(monkeypatch):
    created = []

    class UnexpectedPeerSocket(helpers._SOCKET_TYPE):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            created.append(self)

        def getpeername(self):
            return ("127.0.0.1", 0)

    monkeypatch.setattr(helpers, "_SOCKET_TYPE", UnexpectedPeerSocket)
    with pytest.raises(ConnectionError, match="Unexpected socket-pair endpoint"):
        helpers.local_socketpair()
    assert len(created) == 3
    assert all(instance.fileno() == -1 for instance in created)
