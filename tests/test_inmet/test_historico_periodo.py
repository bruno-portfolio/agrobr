from __future__ import annotations

import asyncio
import hashlib
import io
import json
import zipfile
from dataclasses import replace
from datetime import date, datetime
from pathlib import Path
from unittest.mock import AsyncMock

import httpx
import pandas as pd
import pytest

from agrobr import inmet
from agrobr.exceptions import InvalidParameterError, ParseError, SourceUnavailableError
from agrobr.inmet import client, parser
from tests.helpers import conferir_corpo, levanta_exatamente

GOLDEN = Path(__file__).parents[1] / "golden_data" / "inmet" / "selecao_20260906"
LEGACY = GOLDEN.parent / "historico_a701_sample.csv"
ORACLES = json.loads((GOLDEN / "independent_oracles.json").read_text(encoding="utf-8"))


@pytest.fixture(autouse=True)
def clear_cache():
    client._historico_zip_cache = None
    yield
    client._historico_zip_cache = None


def install_http(monkeypatch, files):
    calls = []

    async def get(_self, url, **_kwargs):
        calls.append(url)
        year = int(url.rsplit("/", 1)[-1].removesuffix(".zip"))
        body = files[year]
        if isinstance(body, Exception):
            raise body
        return httpx.Response(200, content=body, request=httpx.Request("GET", url))

    monkeypatch.setattr(httpx.AsyncClient, "get", get)
    return calls


def synthetic_zip(members):
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_STORED) as zipped:
        for name, raw in members.items():
            zipped.writestr(name, raw)
        zipped.writestr("padding.bin", b"0" * client.MIN_HISTORICO_ZIP)
    return output.getvalue()


@pytest.mark.asyncio
async def test_periodo_cruza_ano_oraculos_celulas(monkeypatch):
    files = {year: (GOLDEN / f"{year}.zip").read_bytes() for year in (2000, 2001)}
    calls = install_http(monkeypatch, files)
    data, meta = await inmet.historico_periodo(
        "a001", "2000-12-30", "2001-01-02", "diario", return_meta=True
    )
    assert len(data) == 4
    assert str(data["data"].dtype) == "datetime64[ns]"
    for expected in ORACLES["boundary_a001_daily"]:
        row = data.loc[data.data.eq(pd.Timestamp(expected["data"]))].iloc[0]
        for column in (
            "precipitacao_mm",
            "temp_media",
            "temp_max",
            "temp_min",
            "umidade_media",
            "radiacao_total_kj_m2",
        ):
            assert row[column] == pytest.approx(expected[column])
    assert len(calls) == 2
    assert meta.parser_version == 2
    assert meta.source_details["time_basis"] == "UTC"
    assert meta.source_details["coverage"]["observed_hours"] == 96
    assert meta.source_details["coverage"]["complete_calendar"]
    assert len(meta.validation_warnings) == 1
    for resource, year in zip(meta.source_details["resources"], (2000, 2001), strict=True):
        assert resource["sha256"] == hashlib.sha256(files[year]).hexdigest()
        assert len(resource["members"]) == 1
    assert (meta.raw_content_hash, meta.raw_content_size) == (None, 0)


@pytest.mark.asyncio
async def test_um_ano_traz_o_hash_e_o_tamanho_do_zip(monkeypatch):
    corpo = (GOLDEN / "2001.zip").read_bytes()
    install_http(monkeypatch, {2001: corpo})

    _, meta = await inmet.historico_periodo(
        "A001", "2001-01-01", "2001-01-02", "diario", return_meta=True
    )

    conferir_corpo(meta, corpo)
    assert [r["bytes"] for r in meta.source_details["resources"]] == [len(corpo)]


@pytest.mark.asyncio
async def test_ausencia_membro_ano_e_explicita(monkeypatch):
    install_http(
        monkeypatch,
        {2000: (GOLDEN / "2000.zip").read_bytes(), 2001: (GOLDEN / "2001.zip").read_bytes()},
    )
    data, meta = await inmet.historico_periodo("A003", "2000-12-30", "2001-12-31", return_meta=True)
    assert not data.empty
    assert meta.source_details["coverage"]["missing_station_years"] == [2000]
    assert not meta.source_details["coverage"]["complete_calendar"]
    assert any("Ausência de membro" in aviso for aviso in meta.validation_warnings)
    assert any("Cobertura inferior" in aviso for aviso in meta.validation_warnings)


@pytest.mark.asyncio
@pytest.mark.parametrize("aggregation", ["horario", "diario"])
async def test_estacao_ausente_vazio_tipado_periodo(monkeypatch, aggregation):
    install_http(monkeypatch, {2000: (GOLDEN / "2000.zip").read_bytes()})
    data, meta = await inmet.historico_periodo(
        "Z999", "2000-01-01", "2000-12-31", aggregation, return_meta=True
    )
    assert data.empty
    assert pd.api.types.is_datetime64_dtype(data.data)
    assert pd.api.types.is_float_dtype(data.precipitacao_mm)
    assert meta.source_details["coverage"]["missing_station_years"] == [2000]


@pytest.mark.asyncio
async def test_estacao_ausente_anual_continua_erro(monkeypatch):
    install_http(monkeypatch, {2000: (GOLDEN / "2000.zip").read_bytes()})
    with pytest.raises(SourceUnavailableError, match="Z999"):
        await inmet.historico("Z999", 2000)


@pytest.mark.asyncio
async def test_uf_ausente_vazio_mensal_tipado(monkeypatch):
    install_http(monkeypatch, {2000: (GOLDEN / "2000.zip").read_bytes()})
    data, meta = await inmet.historico_uf("AC", 2000, return_meta=True)
    assert data.empty
    assert pd.api.types.is_datetime64_dtype(data.mes)
    assert pd.api.types.is_float_dtype(data.precip_acum_mm)
    assert meta.source_details["coverage"]["missing_uf_years"] == [2000]


@pytest.mark.asyncio
async def test_antes_periodo_publicado_sem_inventar_zeros(monkeypatch):
    install_http(monkeypatch, {2000: (GOLDEN / "2000.zip").read_bytes()})
    data, meta = await inmet.historico_periodo("A001", "2000-01-01", "2000-05-06", return_meta=True)
    assert data.empty
    assert meta.source_details["coverage"]["missing_station_years"] == []
    assert not meta.source_details["coverage"]["complete_calendar"]
    assert meta.source_details["stations"][0]["fundacao"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "codigo,inicio,fim,agregacao",
    [
        (True, "2000-01-01", "2000-01-02", "horario"),
        ("../A001", "2000-01-01", "2000-01-02", "horario"),
        ("A001", "2000-02-30", "2000-03-01", "horario"),
        ("A001", "20000101", "2000-01-02", "horario"),
        ("A001", datetime(2000, 1, 1), "2000-01-02", "horario"),
        ("A001", True, "2000-01-02", "horario"),
        ("A001", "2000-01-02", "2000-01-01", "horario"),
        ("A001", "1999-12-31", "2000-01-02", "horario"),
        ("A001", "2000-01-01", "2999-01-02", "horario"),
        ("A001", "2000-01-01", "2000-01-02", "mensal"),
        ("A001", "2000-01-01", "2000-01-02", None),
    ],
)
async def test_filtros_periodo_invalidos_antes_http(monkeypatch, codigo, inicio, fim, agregacao):
    fetch = AsyncMock()
    monkeypatch.setattr(client, "fetch_historico_arquivo", fetch)
    with pytest.raises(InvalidParameterError):
        await inmet.historico_periodo(codigo, inicio, fim, agregacao)
    fetch.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "uf,ano",
    [
        (True, 2000),
        (None, 2000),
        ("XX", 2000),
        ("", 2000),
        ("SP", True),
        ("SP", 2000.0),
        ("SP", "2000"),
        ("SP", 1999),
        ("SP", 2999),
    ],
)
async def test_filtros_uf_invalidos_antes_http(monkeypatch, uf, ano):
    fetch = AsyncMock()
    monkeypatch.setattr(client, "fetch_historico_arquivo", fetch)
    with pytest.raises(InvalidParameterError):
        await inmet.historico_uf(uf, ano)
    fetch.assert_not_awaited()


def test_parser_moderno_preserva_unidades_e_identidade():
    raw = (GOLDEN / "2026_A001.csv").read_bytes()
    metadata = parser.parse_historico_metadata(raw)
    data = parser.parse_historico_csv(raw, "A001")
    assert metadata.altitude == pytest.approx(1160.96)
    assert data.shape == (5832, 17)
    assert data.hora_utc.str.fullmatch(r"\d{4}").all()
    assert data.radiacao_kj_m2.isna().any()
    assert data.uf.eq("DF").all()


@pytest.mark.parametrize(
    "old,new,reason",
    [
        (b"CODIGO (WMO):;A701", b"CODIGO (WMO):;A702", "cabeçalho"),
        (b"0000 UTC", b"9999 UTC", "hora"),
        (b"2025/01/01", b"2025/02/30", "Linha"),
        (b"UF:;SP", b"UF:;XX", "Metadata"),
    ],
)
def test_parser_rejeita_identidade_e_tempo_invalidos(old, new, reason):
    raw = LEGACY.read_bytes()
    assert old in raw
    with pytest.raises(ParseError, match=reason):
        parser.parse_historico_csv(raw.replace(old, new), "A701")


@pytest.mark.asyncio
async def test_todos_membros_selecionados_e_duplicatas_identicas(monkeypatch):
    raw = LEGACY.read_bytes()
    zipped = synthetic_zip({"INMET_SE_SP_A701_PARTE1.CSV": raw, "INMET_SE_SP_A701_PARTE2.CSV": raw})
    install_http(monkeypatch, {2025: zipped})
    data, meta = await inmet.historico_periodo("A701", "2025-01-01", "2025-01-02", return_meta=True)
    assert len(data) == 48
    assert len(meta.source_details["resources"][0]["members"]) == 2
    assert meta.source_details["coverage"]["identical_duplicates_removed"] == 48
    assert any("contadas apenas uma vez" in aviso for aviso in meta.validation_warnings)


@pytest.mark.asyncio
async def test_membros_conflitantes_abortam(monkeypatch):
    raw = LEGACY.read_bytes()
    lines = raw.splitlines()
    cells = lines[9].split(b";")
    cells[2] = b"999,0"
    lines[9] = b";".join(cells)
    zipped = synthetic_zip(
        {"INMET_SE_SP_A701_PARTE1.CSV": raw, "INMET_SE_SP_A701_PARTE2.CSV": b"\n".join(lines)}
    )
    install_http(monkeypatch, {2025: zipped})
    with pytest.raises(ParseError, match="conflitantes"):
        await inmet.historico_periodo("A701", "2025-01-01", "2025-01-02")
    assert 2025 not in client._historico_zip_cache


@pytest.mark.asyncio
async def test_crc_invalido_aborta_e_invalida_cache(monkeypatch):
    raw = LEGACY.read_bytes()
    zipped = synthetic_zip({"INMET_SE_SP_A701_PARTE1.CSV": raw})
    index = zipped.index(raw)
    corrupt = zipped[:index] + bytes([zipped[index] ^ 1]) + zipped[index + 1 :]
    install_http(monkeypatch, {2025: corrupt})
    with pytest.raises(ParseError, match="CRC"):
        await inmet.historico_periodo("A701", "2025-01-01", "2025-01-02")
    assert 2025 not in client._historico_zip_cache


@pytest.mark.asyncio
async def test_cache_compartilha_download_concorrente_e_proveniencia(monkeypatch):
    calls = install_http(monkeypatch, {2001: (GOLDEN / "2001.zip").read_bytes()})
    results = await asyncio.gather(
        *(
            inmet.historico_periodo("A001", "2001-01-01", "2001-01-02", return_meta=True)
            for _ in range(2)
        )
    )
    assert len(calls) == 1
    first, second = results[0][1], results[1][1]
    assert not first.from_cache and second.from_cache
    assert first.fetched_at == second.fetched_at
    assert first.fetch_timestamp == second.fetch_timestamp == first.fetched_at
    assert (
        first.source_details["resources"][0]["sha256"]
        == second.source_details["resources"][0]["sha256"]
    )


@pytest.mark.asyncio
async def test_cache_expirado_revalida_sem_servir_stale(monkeypatch):
    calls = install_http(monkeypatch, {2000: (GOLDEN / "2000.zip").read_bytes()})
    await client.fetch_historico_arquivo(2000)
    client._historico_zip_cache[2000] = replace(client._historico_zip_cache[2000], expires_at=-1)
    archive = await client.fetch_historico_arquivo(2000)
    assert len(calls) == 2
    assert not archive.from_cache


@pytest.mark.asyncio
async def test_cache_guarda_mais_de_um_ano(monkeypatch):
    calls = install_http(
        monkeypatch,
        {year: (GOLDEN / f"{year}.zip").read_bytes() for year in (2000, 2001)},
    )
    await client.fetch_historico_arquivo(2000)
    await client.fetch_historico_arquivo(2001)
    archive = await client.fetch_historico_arquivo(2000)
    assert len(calls) == 2
    assert archive.from_cache


@pytest.mark.asyncio
async def test_api_estacao_rejeita_mensal_antes_io(monkeypatch):
    fetch = AsyncMock()
    monkeypatch.setattr(client, "fetch_dados_estacao", fetch)
    with pytest.raises(InvalidParameterError, match="agregacao"):
        await inmet.estacao("A001", date(2000, 1, 1), date(2000, 1, 2), "mensal")
    fetch.assert_not_awaited()


def test_coluna_sem_nenhuma_medicao_continua_numerica():
    lines = LEGACY.read_bytes().splitlines()
    for index in range(9, len(lines)):
        cells = lines[index].split(b";")
        cells[2] = b""
        lines[index] = b";".join(cells)
    data = parser.parse_historico_csv(b"\n".join(lines), "A701")
    assert data.precipitacao_mm.isna().all()
    assert data.precipitacao_mm.dtype == "float64"


@pytest.mark.asyncio
async def test_ano_do_csv_diferente_do_zip_rejeitado(monkeypatch):
    zipped = synthetic_zip({"INMET_SE_SP_A701_PARTE1.CSV": LEGACY.read_bytes()})
    install_http(monkeypatch, {2000: zipped})
    with pytest.raises(ParseError, match="fora do ano"):
        await inmet.historico_periodo("A701", "2000-01-01", "2000-01-02")
    assert 2000 not in client._historico_zip_cache


@pytest.mark.asyncio
async def test_uf_do_nome_diferente_cabecalho_rejeitada(monkeypatch):
    zipped = synthetic_zip({"INMET_CO_DF_A701_PARTE1.CSV": LEGACY.read_bytes()})
    install_http(monkeypatch, {2025: zipped})
    with pytest.raises(ParseError, match="Identidade"):
        await inmet.historico_uf("DF", 2025)


@pytest.mark.asyncio
async def test_estacao_sem_linhas_no_periodo_nao_some_da_cobertura_uf(monkeypatch):
    raw = LEGACY.read_bytes()
    empty_station = b"\n".join(raw.replace(b"A701", b"A702").splitlines()[:9])
    zipped = synthetic_zip({"INMET_SE_SP_A701_X.CSV": raw, "INMET_SE_SP_A702_Y.CSV": empty_station})
    install_http(monkeypatch, {2025: zipped})
    _, meta = await inmet.historico_uf("SP", 2025, return_meta=True)
    coverage = meta.source_details["coverage"]
    empty = next(station for station in coverage["stations"] if station["codigo"] == "A702")
    assert empty["observed_hours"] == 0
    assert empty["first_observation"] is None
    assert not coverage["complete_calendar"]
    assert not coverage["complete_measurements"]


@pytest.mark.asyncio
async def test_resposta_que_nao_e_zip_recusada(monkeypatch):
    install_http(monkeypatch, {2000: b"<html>manutencao</html>" * 10_000})
    with levanta_exatamente(SourceUnavailableError, "não é um ZIP"):
        await client.fetch_historico_arquivo(2000)


@pytest.mark.asyncio
async def test_zip_menor_que_o_minimo_recusado(monkeypatch):
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as zipped:
        zipped.writestr("INMET_SE_SP_A701_X.CSV", LEGACY.read_bytes()[:200])
    install_http(monkeypatch, {2000: output.getvalue()})
    with levanta_exatamente(SourceUnavailableError, "truncamento"):
        await client.fetch_historico_arquivo(2000)


@pytest.mark.asyncio
async def test_zip_sem_csv_recusado(monkeypatch):
    install_http(monkeypatch, {2025: synthetic_zip({})})
    with levanta_exatamente(ParseError, "sem CSVs"):
        await inmet.historico_periodo("A701", "2025-01-01", "2025-01-02")


@pytest.mark.asyncio
async def test_membro_com_nome_fora_do_padrao_recusado(monkeypatch):
    install_http(monkeypatch, {2025: synthetic_zip({"dados.csv": LEGACY.read_bytes()})})
    with levanta_exatamente(ParseError, "não reconhecido"):
        await inmet.historico_periodo("A701", "2025-01-01", "2025-01-02")
    assert 2025 not in client._historico_zip_cache


def test_duas_colunas_para_a_mesma_medida_recusadas():
    lines = LEGACY.read_bytes().splitlines()
    cells = lines[8].split(b";")
    cells[6] = cells[2] + b" (repetida)"
    lines[8] = b";".join(cells)
    with levanta_exatamente(ParseError, "duplicadas"):
        parser.parse_historico_csv(b"\n".join(lines), "A701")


def test_linha_truncada_recusada():
    lines = LEGACY.read_bytes().splitlines()
    lines[9] = lines[9].rsplit(b";", 1)[0]
    with levanta_exatamente(ParseError, "truncada"):
        parser.parse_historico_csv(b"\n".join(lines), "A701")
