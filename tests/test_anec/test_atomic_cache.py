from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from agrobr.anec import client
from tests.helpers import collect_failures


def test_failed_atomic_write_preserves_target_and_cleans_owned_temporary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    def fail_replace(_source: Path, _target: Path) -> None:
        raise PermissionError("replace failed")

    monkeypatch.setattr(client.os, "replace", fail_replace)
    with collect_failures() as check:
        for nome, escrever, conteudo in [
            ("bytes", client._atomic_write_bytes, b"updated"),
            ("texto", client._atomic_write_text, "updated"),
        ]:
            with check(nome):
                target = tmp_path / nome / "data"
                target.parent.mkdir()
                target.write_bytes(b"original")
                foreign = target.parent / "foreign.tmp"
                foreign.write_bytes(b"active")
                with pytest.raises(PermissionError, match="replace failed"):
                    escrever(target, conteudo)
                assert target.read_bytes() == b"original"
                assert foreign.read_bytes() == b"active"
                assert set(target.parent.iterdir()) == {target, foreign}


def test_trava_de_download_e_por_loop_de_eventos():
    def outro_loop() -> str:
        async def tentar() -> None:
            lock = await client._get_fetch_lock("mesma-edicao")
            await asyncio.wait_for(lock.acquire(), timeout=1)
            lock.release()

        try:
            asyncio.run(tentar())
        except (TimeoutError, RuntimeError) as error:
            return type(error).__name__
        return "adquiriu"

    async def principal() -> str:
        lock = await client._get_fetch_lock("mesma-edicao")
        async with lock:
            return await asyncio.to_thread(outro_loop)

    assert asyncio.run(principal()) == "adquiriu"
