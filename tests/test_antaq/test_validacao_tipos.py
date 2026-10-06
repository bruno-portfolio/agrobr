from __future__ import annotations

import pandas as pd
import pytest

from agrobr import antaq, datasets
from agrobr.exceptions import InvalidParameterError, ParseError
from tests.test_antaq.test_reconciliacao import _corpo, install_replay_antaq


@pytest.mark.parametrize("consulta", [antaq.movimentacao, datasets.movimentacao_portuaria])
@pytest.mark.parametrize(
    "opcoes",
    [
        {"sentido": "XX"},
        {"sentido": []},
        {"sentido": ""},
        {"tipo_navegacao": "XX"},
        {"tipo_navegacao": 7},
        {"natureza_carga": "XX"},
        {"natureza_carga": []},
        {"ano": True},
    ],
)
async def test_filtro_invalido_recusado_antes_dos_zips(monkeypatch, consulta, opcoes):
    rede = install_replay_antaq(monkeypatch)
    with pytest.raises(InvalidParameterError):
        await consulta(**{"ano": 2024, **opcoes})
    assert rede["served"] == []


@pytest.mark.parametrize("consulta", [antaq.movimentacao, datasets.movimentacao_portuaria])
@pytest.mark.parametrize("polars", [False, True])
async def test_movimentacao_cheio_oficial_e_vazio_com_mesmos_tipos(monkeypatch, consulta, polars):
    if polars:
        pytest.importorskip("polars")
    install_replay_antaq(monkeypatch)
    cheio = await consulta(ano=2024, as_polars=polars)
    vazio = await consulta(ano=2024, mercadoria="ausente neste recorte", as_polars=polars)
    if polars:
        assert vazio.is_empty()
        assert cheio.schema == vazio.schema
    else:
        pd.testing.assert_frame_equal(cheio.iloc[:0], vazio)
        assert str(cheio["data_atracacao"].dtype) == "datetime64[ns]"
        assert cheio["data_atracacao"].min() == pd.Timestamp("2023-12-22 17:32:00")
        assert str(cheio["qt_carga"].dtype) == "float64"
        assert str(cheio["teu"].dtype) == "Int64"


async def test_alias_sentido_e_navegacao_usam_rotulos_publicados(monkeypatch):
    install_replay_antaq(monkeypatch)
    frame = await antaq.movimentacao(
        2024, sentido=" DESEMBARCADOS ", tipo_navegacao=" Longo Curso "
    )
    assert frame["sentido"].tolist() == ["Desembarcados", "Desembarcados"]
    assert frame["peso_bruto_ton"].sum() == pytest.approx(37007.98)


async def test_data_digitada_errado_gera_nat_com_aviso_no_meta(monkeypatch):
    corpo = _corpo("atracacao.txt").replace(b"01/01/2024 11:13:00", b"31/02/2024 11:13:00")
    install_replay_antaq(monkeypatch, atracacao=corpo)
    with pytest.warns(UserWarning, match="data_atracacao.*NaT"):
        frame, meta = await antaq.movimentacao(2024, return_meta=True)
    assert frame["data_atracacao"].isna().sum() == 1
    assert any("data_atracacao" in aviso for aviso in meta.validation_warnings)


async def test_texto_no_campo_de_data_recusado_com_parse_error(monkeypatch):
    corpo = _corpo("atracacao.txt").replace(b"01/01/2024 11:13:00", b"DATA ALTERADA")
    install_replay_antaq(monkeypatch, atracacao=corpo)
    with pytest.raises(ParseError, match="data_atracacao"):
        await antaq.movimentacao(2024)
