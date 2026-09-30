from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from agrobr import datasets
from agrobr.alt.anp_diesel import api, client, models
from agrobr.exceptions import InvalidParameterError, SourceUnavailableError


@pytest.fixture(scope="module")
def estados_publicados():
    arquivo = Path(__file__).parents[1] / "golden_data/anp_diesel/precos_sample/response.xlsx"
    return client.PrecosResource(
        content=arquivo.read_bytes(),
        requested_url=models.PRECOS_ESTADOS_URL,
        url=models.PRECOS_ESTADOS_URL,
        fetched_at=datetime(2026, 2, 21, 8, 25, 13, tzinfo=UTC),
    )


@pytest.mark.parametrize("consulta", [api.precos_diesel, datasets.precos_diesel])
@pytest.mark.parametrize("agregacao", ["semanal", "mensal"])
async def test_repeticao_oficial_preserva_valor_e_media_sem_repesar_semana(
    consulta, agregacao, estados_publicados, monkeypatch
):
    monkeypatch.setattr(client, "fetch_precos_resource", AsyncMock(return_value=estados_publicados))
    aviso = "ANP: linhas semanais idênticas removidas na seleção: 1."
    with pytest.warns(UserWarning, match="linhas semanais idênticas"):
        frame, meta = await consulta(
            uf="SP",
            nivel="uf",
            produto="DIESEL S10",
            inicio="2025-08-24",
            fim="2025-08-31",
            agregacao=agregacao,
            return_meta=True,
        )

    assert frame["uf"].unique().tolist() == ["SP"]
    assert frame["produto"].unique().tolist() == ["DIESEL S10"]
    assert frame["nivel"].unique().tolist() == ["uf"]
    assert frame["unidade"].unique().tolist() == ["BRL/litro"]
    if agregacao == "semanal":
        assert frame["data"].tolist() == [pd.Timestamp("2025-08-24"), pd.Timestamp("2025-08-31")]
        assert frame["preco_venda"].tolist() == [6.15, 6.13]
        assert frame["n_postos"].tolist() == [745, 701]
    else:
        assert frame["data"].tolist() == [pd.Timestamp("2025-08-01")]
        assert frame["preco_venda"].tolist() == pytest.approx([6.14])
        assert frame["n_postos_media"].tolist() == [723.0]
        assert frame["n_postos"].isna().all()
        assert frame["n_semanas"].tolist() == [2]
    assert meta.validation_warnings.count(aviso) == 1
    assert meta.source_details["duplicate_weekly_rows_before_date_filter"] == 4
    assert meta.validation_passed
    assert str(frame["preco_venda"].dtype) == "float64"
    assert str(frame["data"].dtype) == "datetime64[ns]"
    assert frame["uf"].dtype == pd.Series(["SP"]).dtype


@pytest.mark.parametrize("consulta", [api.precos_diesel, datasets.precos_diesel])
async def test_vazio_tem_o_dtype_textual_nativo_e_os_tipos_numericos(
    consulta, estados_publicados, monkeypatch
):
    monkeypatch.setattr(client, "fetch_precos_resource", AsyncMock(return_value=estados_publicados))
    frame = await consulta(nivel="uf", uf="SP", inicio="2030-01-01")
    assert frame.empty
    assert frame["uf"].dtype == pd.Series(["SP"]).dtype
    assert str(frame["preco_venda"].dtype) == "float64"
    assert str(frame["data"].dtype) == "datetime64[ns]"
    assert str(frame["n_postos"].dtype) == "Int64"


@pytest.mark.parametrize(
    ("consulta", "argumentos"),
    [
        (api.precos_diesel, (None, None, "DIESEL S10", None, None, "semanal", "brasil", False)),
        (api.vendas_diesel, (None, None, None, False)),
        (datasets.precos_diesel, ("DIESEL S10", False)),
    ],
)
async def test_flags_por_posicao_recusadas_antes_do_download(consulta, argumentos, monkeypatch):
    precos, vendas = AsyncMock(), AsyncMock()
    monkeypatch.setattr(client, "fetch_precos_resource", precos)
    monkeypatch.setattr(client, "fetch_vendas_m3", vendas)
    with pytest.raises(TypeError, match="positional"):
        await consulta(*argumentos)
    precos.assert_not_awaited()
    vendas.assert_not_awaited()


async def test_fetch_do_registry_recusa_return_meta_por_posicao(monkeypatch):
    download = AsyncMock()
    monkeypatch.setattr(client, "fetch_precos_resource", download)
    with pytest.raises(TypeError, match="positional"):
        await datasets.get_dataset("precos_diesel").fetch("DIESEL S10", True)
    download.assert_not_awaited()


@pytest.mark.parametrize("opcoes", [{"as_polars": "yes"}, {"return_meta": 1}])
async def test_flags_de_vendas_invalidas_recusadas_antes_do_download(opcoes, monkeypatch):
    download = AsyncMock()
    monkeypatch.setattr(client, "fetch_vendas_m3", download)
    with pytest.raises(InvalidParameterError, match="booleanos"):
        await api.vendas_diesel(**opcoes)
    download.assert_not_awaited()


async def test_virada_do_ano_usa_o_dia_civil_de_brasilia(monkeypatch):
    monkeypatch.setattr(api.time_utils, "utcnow", lambda: datetime(2027, 1, 1, 1, tzinfo=UTC))
    monkeypatch.setattr(api.time_utils, "hoje", lambda: date(2026, 12, 31))
    catalogo, download = AsyncMock(), AsyncMock()
    monkeypatch.setattr(client, "fetch_precos_catalog", catalogo)
    monkeypatch.setattr(client, "fetch_precos_resource", download)
    with pytest.raises(SourceUnavailableError, match="2022 a 2026"):
        await api.precos_diesel(inicio="2027-01-01")
    catalogo.assert_not_awaited()
    download.assert_not_awaited()
