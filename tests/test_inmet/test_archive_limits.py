from __future__ import annotations

import hashlib
import io
import zipfile
from datetime import UTC, datetime

import pytest

from agrobr import constants
from agrobr.exceptions import ResourceLimitError
from agrobr.inmet import client


def archive(parts: list[bytes]) -> client.HistoricoArquivo:
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w", compression=zipfile.ZIP_DEFLATED) as zipped:
        for index, part in enumerate(parts):
            zipped.writestr(f"INMET_CO_DF_A00{index}_2025.csv", part)
    content = stream.getvalue()
    return client.HistoricoArquivo(
        2025,
        content,
        "https://example.test/2025.zip",
        hashlib.sha256(content).hexdigest(),
        datetime.now(UTC),
        0,
    )


@pytest.mark.parametrize(
    "parts,member_limit,total_limit", [([b"x" * 4096], 1024, 8192), ([b"x" * 1024] * 2, 1024, 1024)]
)
def test_zip_expansao_recusada_antes_de_abrir_csv(monkeypatch, parts, member_limit, total_limit):
    acquired = archive(parts)
    monkeypatch.setattr(constants, "INMET_HISTORICO_MAX_MEMBER_BYTES", member_limit)
    monkeypatch.setattr(constants, "INMET_HISTORICO_MAX_EXPANDED_BYTES", total_limit)
    monkeypatch.setattr(
        zipfile.ZipFile, "open", lambda *_a, **_kw: pytest.fail("CSV foi descomprimido")
    )
    with pytest.raises(ResourceLimitError, match="Expansão"):
        client.historico_membros(acquired, uf="DF")


def test_zip_orcamento_considera_apenas_membros_selecionados(monkeypatch):
    acquired = archive([b"selected", b"x" * 4096])
    monkeypatch.setattr(constants, "INMET_HISTORICO_MAX_MEMBER_BYTES", 10)
    monkeypatch.setattr(constants, "INMET_HISTORICO_MAX_EXPANDED_BYTES", 10)
    assert client.historico_membros(acquired, codigo="A000")[0][3] == b"selected"
