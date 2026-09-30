from __future__ import annotations

from datetime import date, datetime
from unittest.mock import Mock

import pandas as pd
import pytest

from agrobr import deterministic
from agrobr.alt.antt_pedagio import api, query
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
    b"CCR AutoBAn;Campinas;SP-348;SP;87.5;Campinas;-22.9;-47.0;Ativa\n"
    b"Arteris;Jacarezinho;BR-153;PR;10.0;Jacarezinho;-23.1;-49.9;Ativa\n"
)


async def test_fluxo_diario_preserva_dia_e_filtra_intervalo_inclusivo(monkeypatch):
    install_anttpedagio_source(monkeypatch, {"volume-2025_diario.csv": DAILY_CSV})
    frame = await fluxo_pedagio(
        ano=2025,
        frequencia="diaria",
        enriquecer=False,
        inicio="2025-01-15",
        fim=datetime(2025, 1, 15, 23, 59),
        tipo_cobranca="manual",
        tipo_veiculo="COMERCIAL",
    )
    assert frame["data"].tolist() == [pd.Timestamp("2025-01-15")]
    assert frame["volume"].tolist() == [12]
    assert frame["frequencia"].tolist() == ["diaria"]


async def test_fluxo_filtro_de_texto_sem_registros_avisa(monkeypatch):
    install_anttpedagio_source(monkeypatch, {"volume-2025_diario.csv": DAILY_CSV})
    with pytest.warns(UserWarning, match="Nenhum registro da ANTT casou"):
        frame, meta = await fluxo_pedagio(
            ano=2025, frequencia="diaria", enriquecer=False, concessionaria="xx", return_meta=True
        )
    assert frame.empty
    assert any("Nenhum registro da ANTT casou" in aviso for aviso in meta.validation_warnings)


def test_fluxo_teto_do_ano_usa_o_dia_de_brasilia(monkeypatch):
    monkeypatch.setattr(query.time_utils, "hoje", lambda: date(2031, 1, 1))
    assert query._years(2031, None, None) == (2031,)


@pytest.mark.parametrize(
    "call",
    [
        lambda: fluxo_pedagio(2025, None, None, None, None, None, None, None, False, True),
        lambda: pracas_pedagio(None, None, None, True),
    ],
)
async def test_flags_somente_nomeadas(call):
    with pytest.raises(TypeError):
        await call()


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
        {"ano": 2025, "inicio": "2025-02-30"},
        {"ano": 2025, "inicio": "2025-01-02"},
        {"ano": 2025, "inicio": "2025/01/01"},
        {"ano": 2025, "inicio": "2024-01-01"},
        {"ano": 2024, "inicio": "2025-01-01"},
        {"tipo_veiculo": "caminhao"},
        {
            "ano": 2025,
            "frequencia": "diaria",
            "inicio": "2025-01-02",
            "fim": "2025-01-01",
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
    @pytest.mark.parametrize("rodovia", ["BR 153", "br153", "BR-0153"])
    async def test_rodovia_sem_espaco_hifen_e_zero_a_esquerda(self, monkeypatch, rodovia):
        install_anttpedagio_source(monkeypatch, {}, plazas=PRACAS_CSV)
        frame = await pracas_pedagio(rodovia=rodovia)
        assert frame["praca_de_pedagio"].tolist() == ["Jacarezinho"]

    async def test_filtro_sem_pracas_avisa_os_valores_publicados(self, monkeypatch):
        install_anttpedagio_source(monkeypatch, {}, plazas=PRACAS_CSV)
        with pytest.warns(UserWarning, match="Ativa"):
            frame, meta = await pracas_pedagio(situacao="xx", return_meta=True)
        assert frame.empty
        assert "valores publicados" in meta.validation_warnings[0]

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
