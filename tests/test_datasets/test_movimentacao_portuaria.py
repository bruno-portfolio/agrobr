from typing import Any

import pandas as pd
import pytest

from agrobr.datasets.movimentacao_portuaria import (
    MovimentacaoPortuariaDataset,
)
from tests import helpers
from tests.helpers import collect_failures, isolated_dataset_case

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
