from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from agrobr.models import MetaInfo
from tests.helpers import isolate_dataset_info
from tests.test_bcb.focus_replay import annual_row as annual_row
from tests.test_bcb.focus_replay import focus_captures as focus_captures
from tests.test_bcb.focus_replay import focus_http as focus_http
from tests.test_bcb.ptax_replay import ptax_captures as ptax_captures
from tests.test_bcb.ptax_replay import ptax_http as ptax_http
from tests.test_bcb.ptax_replay import quote_row as quote_row
from tests.test_bcb.sgs_replay import sgs_captures as sgs_captures
from tests.test_bcb.sgs_replay import sgs_http as sgs_http
from tests.test_comtrade.replay import captures as captures
from tests.test_comtrade.replay import replay_http as replay_http
from tests.test_lista_suja.conftest import publication_files as publication_files
from tests.test_lista_suja.conftest import replay_http as lista_suja_replay_http
from tests.test_mapbiomas.conftest import municipal_capture as municipal_capture
from tests.test_mapbiomas.conftest import replay_mapbiomas as replay_mapbiomas

__all__ = ["lista_suja_replay_http"]


@pytest.fixture(autouse=True)
def _isolate_dataset_info(monkeypatch):
    isolate_dataset_info(monkeypatch)


def mock_source_meta(
    source_url: str = "http://test",
    parser_version: int = 1,
) -> MetaInfo:
    return MetaInfo(
        source="fixture",
        source_url=source_url,
        source_method="fixture",
        fetched_at=datetime(2026, 9, 6, tzinfo=UTC),
        parser_version=parser_version,
    )


def make_source(
    df: pd.DataFrame,
    meta: MetaInfo | None = None,
    *,
    raises: Exception | None = None,
) -> AsyncMock:
    if raises is not None:
        return AsyncMock(side_effect=raises)
    return AsyncMock(return_value=(df, meta or mock_source_meta()))


CADASTRO_RURAL_CAPTURA = (
    Path(__file__).parent.parent / "golden_data" / "sicar" / "selecao_20260906" / "df_tabular.json"
)


def amostra_abate_trimestral() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "trimestre": "202303",
                "localidade": "Brasil",
                "localidade_cod": 1,
                "especie": "bovino",
                "categoria": "total",
                "animais_abatidos": 8500000.0,
                "peso_carcacas": 2200000.0,
                "fonte": "ibge_abate",
            },
        ]
    )


def amostra_cadastro_rural() -> pd.DataFrame:
    payload = json.loads(CADASTRO_RURAL_CAPTURA.read_text(encoding="utf-8"))
    frame = pd.DataFrame([feature["properties"] for feature in payload["features"][:2]])
    frame = frame.rename(
        columns={
            "status_imovel": "status",
            "dat_criacao": "data_criacao",
            "area": "area_ha",
            "m_fiscal": "modulos_fiscais",
            "tipo_imovel": "tipo",
        }
    )
    for column in ("data_criacao", "data_atualizacao"):
        frame[column] = pd.to_datetime(frame[column], utc=True)
    return frame


def meta_cadastro_rural() -> MetaInfo:
    return mock_source_meta(
        source_url="https://geoserver.car.gov.br/geoserver/sicar/wfs",
        parser_version=2,
    )


def amostra_censo_agropecuario_legado(nivel: str = "uf") -> pd.DataFrame:
    df = pd.DataFrame(
        [
            {
                "ano": 1995,
                "localidade": "Santa Maria" if nivel == "municipio" else "Rio Grande do Sul",
                "localidade_cod": None if nivel == "municipio" else 43,
                "uf": "RS",
                "tema": "tecnologia",
                "categoria": "Total",
                "variavel": "Estabelecimentos com declaração de uso de / Irrigação",
                "valor": 100000.0,
                "unidade": "estabelecimentos",
                "fonte": "ibge_censo_agro_legado",
            },
        ]
    )
    df["localidade_cod"] = df["localidade_cod"].astype("Int64")
    return df


def amostra_leite_industrial() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "trimestre": "202301",
                "localidade": "Minas Gerais",
                "localidade_cod": 31,
                "leite_adquirido": 1800000.0,
                "leite_industrializado": 1500000.0,
                "preco_medio": 2.45,
                "fonte": "ibge_leite_trimestral",
            },
        ]
    )


def amostra_preco_atacado() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "data": [pd.Timestamp("2024-01-15")],
            "produto": ["TOMATE"],
            "categoria": ["HORTALICAS"],
            "unidade": ["KG"],
            "ceasa": ["CEAGESP - SAO PAULO"],
            "ceasa_uf": ["SP"],
            "preco": [5.50],
        }
    )
