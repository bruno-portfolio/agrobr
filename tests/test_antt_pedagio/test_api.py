from __future__ import annotations

from datetime import datetime
from unittest.mock import Mock

import pandas as pd
import pytest

from agrobr import deterministic
from agrobr.alt.antt_pedagio import api
from agrobr.alt.antt_pedagio.api import fluxo_pedagio, pracas_pedagio
from agrobr.exceptions import InvalidParameterError
from tests.helpers import install_anttpedagio_source

TRAFEGO_HEADER = "concessionaria;mes_ano;sentido;praca;tipo_cobranca;categoria_eixo;tipo_de_veiculo;volume_total\n"
DAILY_CSV = (
    TRAFEGO_HEADER + "CCR (Sul);14/01/2025;Crescente;P1;Manual;2;Comercial;11,00\n"
    "CCR (Sul);15/01/2025;Crescente;P1;Manual;2;Comercial;12,00\n"
    "CCR (Sul);15/01/2025;Crescente;P1;Automática;2;Comercial;13,00\n"
).encode()
PRACAS_CSV = (
    b"concessionaria;praca_de_pedagio;rodovia;uf;km_m;municipio;lat;lon;situacao\n"
    b"CCR AutoBAn;Campinas;SP-348;SP;87+500;Campinas;-22.9;-47.0;Ativa\n"
    b"Arteris;Jacarezinho;BR-153;PR;10+000;Jacarezinho;-23.1;-49.9;Ativa\n"
)


async def test_fluxo_diario_preserva_dia_e_filtra_intervalo_inclusivo(monkeypatch):
    install_anttpedagio_source(monkeypatch, {"volume-2025_diario.csv": DAILY_CSV})
    frame = await fluxo_pedagio(
        ano=2025,
        frequencia="diaria",
        enriquecer=False,
        data_inicio="2025-01-15",
        data_fim="2025-01-15",
        tipo_cobranca="Manual",
    )
    assert frame["data"].tolist() == [pd.Timestamp("2025-01-15")]
    assert frame["volume"].tolist() == [12]
    assert frame["frequencia"].tolist() == ["diaria"]


@pytest.mark.parametrize(
    "options",
    [
        {"ano": True},
        {"ano": 2025.0},
        {"ano": 2009},
        {"ano": 2025, "ano_inicio": 2024},
        {"ano_inicio": 2025, "ano_fim": 2024},
        {"frequencia": "anual"},
        {"frequencia": pd.NA},
        {"concessionaria": " "},
        {"uf": "XX"},
        {"as_polars": 1},
        {"return_meta": "sim"},
        {"apenas_pesados": 1},
        {"enriquecer": 0},
        {"max_linhas": False},
        {"max_memoria_bytes": 0},
        {"ano": 2025, "data_inicio": "2025-02-30"},
        {"ano": 2025, "data_inicio": "2025-01-02"},
        {"ano": 2025, "data_inicio": datetime(2025, 1, 1)},
        {"ano": 2025, "data_inicio": "2024-01-01"},
        {"ano": 2024, "data_inicio": "2025-01-01"},
        {
            "ano": 2025,
            "frequencia": "diaria",
            "data_inicio": "2025-01-02",
            "data_fim": "2025-01-01",
        },
        {"enriquecer": False, "uf": "SP"},
    ],
)
async def test_fluxo_parametros_invalidos_antes_de_http(monkeypatch, options):
    calls, _ = install_anttpedagio_source(monkeypatch, {})
    with pytest.raises(Exception) as caught:
        await fluxo_pedagio(**options)
    assert caught.type is InvalidParameterError, caught.value
    assert calls == []


@pytest.mark.parametrize("function", [fluxo_pedagio, pracas_pedagio])
async def test_deterministic_recusado_antes_de_http(monkeypatch, function):
    calls, _ = install_anttpedagio_source(monkeypatch, {})
    async with deterministic("2026-09-07"):
        with pytest.raises(Exception) as caught:
            await function()
    assert caught.type is InvalidParameterError, caught.value
    assert "deterministic" in str(caught.value)
    assert calls == []


@pytest.mark.parametrize("function", [fluxo_pedagio, pracas_pedagio])
async def test_polars_ausente_antes_de_http(monkeypatch, function):
    calls, _ = install_anttpedagio_source(monkeypatch, {})

    def missing(name):
        raise ImportError(name)

    monkeypatch.setattr(api.importlib, "import_module", missing)
    with pytest.raises(Exception) as caught:
        await function(as_polars=True)
    assert caught.type is ImportError, caught.value
    assert "pip install agrobr[polars]" in str(caught.value)
    assert calls == []


async def test_fluxo_valida_ano_antes_de_consultar_opcional(monkeypatch):
    calls, _ = install_anttpedagio_source(monkeypatch, {})
    loader = Mock(return_value=None)
    monkeypatch.setattr(api.importlib, "import_module", loader)
    with pytest.raises(InvalidParameterError, match="ano"):
        await fluxo_pedagio(ano=True, as_polars=True)
    assert loader.call_count == 0
    assert calls == []


class TestPracasPedagio:
    @pytest.mark.parametrize("empty", [False, True])
    async def test_polars_preserva_texto_nulos_e_coordenadas(self, monkeypatch, empty):
        polars = pytest.importorskip("polars")
        body = PRACAS_CSV.replace(b"-23.1;-49.9;Ativa", b";;")
        calls, _ = install_anttpedagio_source(monkeypatch, {}, plazas=body)
        frame, meta = await pracas_pedagio(
            uf="AC" if empty else None, as_polars=True, return_meta=True
        )
        assert frame.shape == (0 if empty else 2, 9)
        assert frame.schema["lat"] == frame.schema["lon"] == polars.Float64
        assert frame.schema["concessionaria"] == frame.schema["situacao"] == polars.Utf8
        assert frame["lat"].to_list() == ([] if empty else [-22.9, None])
        assert frame["situacao"].to_list() == ([] if empty else ["Ativa", ""])
        assert meta.records_count == len(frame) and len(calls) == 2
