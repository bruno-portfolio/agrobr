from __future__ import annotations

import ssl
from unittest import mock

import pytest

from agrobr.funai import _tls

pytest.importorskip("shapely")
reconciliacao = pytest.importorskip("scripts.reconciliar_funai")


def test_reconciliacao_usa_contexto_tls_da_biblioteca(monkeypatch, tmp_path):
    contexto = ssl.create_default_context()
    construir = mock.Mock(return_value=contexto)
    cliente = mock.MagicMock()
    monkeypatch.setattr(_tls, "build_context", construir)
    monkeypatch.setattr(reconciliacao.httpx, "Client", cliente)
    monkeypatch.setattr(reconciliacao, "CASOS", [])

    assert reconciliacao.run(tmp_path / "funai.json") == 0
    assert cliente.call_args.kwargs.get("verify") is contexto
    construir.assert_called_once_with()
    assert contexto.verify_mode == ssl.CERT_REQUIRED
    assert contexto.check_hostname
