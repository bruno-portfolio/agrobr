import io
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from agrobr.antaq import client
from agrobr.datasets.movimentacao_portuaria import (
    MovimentacaoPortuariaDataset,
    movimentacao_portuaria,
)
from agrobr.exceptions import ParseError
from tests import helpers
from tests.helpers import collect_failures, isolated_dataset_case, levanta_exatamente

from .conftest import make_source


def _make_df(**overrides):
    row = {
        "ano": 2024,
        "mes": 6,
        "data_atracacao": pd.Timestamp("2024-06-15"),
        "tipo_navegacao": "Longo Curso",
        "tipo_operacao": "Embarque",
        "natureza_carga": "Granel Sólido",
        "sentido": "Embarcados",
        "porto": "Santos",
        "complexo_portuario": "Santos",
        "terminal": "Terminal de Granéis",
        "municipio": "Santos",
        "uf": "SP",
        "regiao": "Sudeste",
        "cd_mercadoria": "1201",
        "mercadoria": "Soja",
        "grupo_mercadoria": "Grãos",
        "origem": "Brasil",
        "destino": "China",
        "peso_bruto_ton": 65000.0,
        "qt_carga": 64500.0,
        "teu": 0,
    }
    row.update(overrides)
    return pd.DataFrame([row])


class TestMovimentacaoPortuariaAgregacao:
    @pytest.mark.parametrize(
        "scenario,parameters",
        [
            ("test_preserva_soma_com_codigo_mercadoria_ausente", {}),
            ("test_movimentacao_portuaria_agregacao_casos_1", {}),
        ],
        ids=[
            "preserva_soma_com_codigo_mercadoria_ausente-0",
            "movimentacao_portuaria_agregacao_casos_1-0",
        ],
    )
    async def test_agregacao_portuaria_com_chaves_e_contexto(
        self, scenario: str, parameters: dict[str, Any]
    ):
        with (
            helpers.collect_failures() as check,
            check((scenario, parameters)),
            helpers.isolated_dataset_case((scenario, parameters)),
        ):
            if scenario == "test_preserva_soma_com_codigo_mercadoria_ausente":
                frame = pd.concat(
                    [
                        _make_df(cd_mercadoria=None, peso_bruto_ton=4.0),
                        _make_df(cd_mercadoria=None, peso_bruto_ton=6.0),
                    ],
                    ignore_index=True,
                )
                frame["cd_mercadoria"] = frame["cd_mercadoria"].astype("string")
                dataset = MovimentacaoPortuariaDataset()
                dataset.info.sources[0].fetch_fn = make_source(frame)
                result = await dataset.fetch(ano=2024)
                assert len(result) == 1
                assert result["cd_mercadoria"].isna().all()
                assert result["peso_bruto_ton"].tolist() == [10.0]
            elif scenario == "test_movimentacao_portuaria_agregacao_casos_1":
                with collect_failures() as check:
                    case = "test_agrega_por_pk_do_contrato"
                    with check(case), isolated_dataset_case(case):
                        df_detalhe = pd.concat(
                            [
                                _make_df(peso_bruto_ton=60000.0, qt_carga=59000.0, teu=2),
                                _make_df(
                                    peso_bruto_ton=5000.0,
                                    qt_carga=5500.0,
                                    teu=1,
                                    terminal="Outro Terminal",
                                ),
                            ],
                            ignore_index=True,
                        )
                        dataset = MovimentacaoPortuariaDataset()
                        dataset.info.sources[0].fetch_fn = make_source(df_detalhe)
                        df = await dataset.fetch(ano=2024)
                        assert len(df) == 1
                        assert df["peso_bruto_ton"].iloc[0] == 65000.0
                        assert df["qt_carga"].iloc[0] == 64500.0
                        assert df["teu"].iloc[0] == 3
                    case = "test_detalhe_unico_preservado_misto_vira_none"
                    with check(case), isolated_dataset_case(case):
                        df_detalhe = pd.concat(
                            [
                                _make_df(terminal="Terminal A", origem="Brasil"),
                                _make_df(terminal="Terminal B", origem="Brasil"),
                            ],
                            ignore_index=True,
                        )
                        dataset = MovimentacaoPortuariaDataset()
                        dataset.info.sources[0].fetch_fn = make_source(df_detalhe)
                        df = await dataset.fetch(ano=2024)
                        assert len(df) == 1
                        assert df["origem"].iloc[0] == "Brasil"
                        assert df["terminal"].iloc[0] is None
                        assert "data_atracacao" in df.columns
                        assert "natureza_carga" in df.columns


async def test_movimentacao_portuaria_soma_com_medida_nula_sai_nula():
    frame = pd.concat(
        [
            _make_df(peso_bruto_ton=4.0, qt_carga=float("nan")),
            _make_df(peso_bruto_ton=float("nan"), qt_carga=float("nan")),
            _make_df(porto="Paranaguá", peso_bruto_ton=6.0, qt_carga=2.0),
            _make_df(porto="Paranaguá", peso_bruto_ton=1.0, qt_carga=3.0),
        ],
        ignore_index=True,
    ).astype({"teu": "Int64"})
    with isolated_dataset_case("soma_com_medida_nula"):
        dataset = MovimentacaoPortuariaDataset()
        dataset.info.sources[0].fetch_fn = make_source(frame)
        df = await dataset.fetch(ano=2024)
    santos = df[df["porto"] == "Santos"].iloc[0]
    paranagua = df[df["porto"] == "Paranaguá"].iloc[0]
    assert pd.isna(santos["peso_bruto_ton"]) and pd.isna(santos["qt_carga"])
    assert santos["teu"] == 0
    assert (paranagua["peso_bruto_ton"], paranagua["qt_carga"]) == (7.0, 5.0)
    assert {str(df[c].dtype) for c in ("peso_bruto_ton", "qt_carga")} == {"float64"}
    assert str(df["teu"].dtype) == "Int64"


async def test_movimentacao_portuaria_carga_sem_coluna_da_chave_levanta_parse_error():
    base = Path(__file__).parents[1] / "golden_data" / "antaq" / "movimentacao_sample"
    textos = {
        nome: (base / f"{nome}.txt").read_text(encoding="utf-8")
        for nome in ("atracacao", "carga", "mercadoria")
    }
    carga = pd.read_csv(io.StringIO(textos["carga"]), sep=";", dtype=str, keep_default_na=False)
    textos["carga"] = carga.drop(columns="Sentido").to_csv(sep=";", index=False)
    with isolated_dataset_case("carga_sem_sentido") as monkeypatch:
        monkeypatch.setattr(client, "fetch_ano_zip", AsyncMock(return_value=b"ano"))
        monkeypatch.setattr(client, "fetch_mercadoria_zip", AsyncMock(return_value=b"merc"))
        for nome in textos:
            monkeypatch.setattr(client, f"extract_{nome}", lambda *_, t=textos[nome]: t)
        with levanta_exatamente(ParseError, match="Sentido"):
            await movimentacao_portuaria(ano=2024, tipo_navegacao="cabotagem")
