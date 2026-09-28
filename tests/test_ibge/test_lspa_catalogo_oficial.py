from __future__ import annotations

import hashlib
import json
from pathlib import Path
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from agrobr import ibge
from agrobr.contracts import validate_dataset
from agrobr.exceptions import ParseError
from agrobr.ibge import client, lspa_parser
from tests import helpers

FIXTURES = Path(__file__).parents[1] / "golden_data/ibge/lspa_catalogo_oficial"
LSPA_OFICIAL = Path(__file__).parents[1] / "golden_data/ibge/lspa_oficial/lspa_client_capture.json"
EXPECTED_NAMES = {
    "soja": "Soja",
    "milho_1": "Milho (1ª Safra)",
    "milho_2": "Milho (2ª Safra)",
    "arroz": "Arroz",
    "feijao_1": "Feijão (1ª Safra)",
    "feijao_2": "Feijão (2ª Safra)",
    "feijao_3": "Feijão (3ª Safra)",
    "trigo": "Trigo",
    "algodao": "Algodão herbáceo",
    "cafe_arabica": "Café arábica",
    "cafe_canephora": "Café canephora",
    "amendoim_1": "Amendoim (1ª Safra)",
    "amendoim_2": "Amendoim (2ª Safra)",
    "aveia": "Aveia",
    "batata_1": "Batata - inglesa (1ª Safra)",
    "batata_2": "Batata - inglesa (2ª Safra)",
    "batata_3": "Batata - inglesa (3ª Safra)",
    "cevada": "Cevada",
    "mamona": "Mamona",
    "sorgo": "Sorgo",
    "triticale": "Triticale",
}


def _rows(period: str) -> pd.DataFrame:
    file = f"{period}.json" if period.endswith("07") else "cafe_201112_201201.json"
    capture = next(
        item
        for item in json.loads((FIXTURES / "provenance.json").read_bytes())["captures"]
        if item["file"] == file
    )
    content = (FIXTURES / file).read_bytes()
    assert len(content) == capture["bytes"]
    assert hashlib.sha256(content).hexdigest() == capture["sha256"]
    frame = pd.DataFrame(json.loads(content))
    return frame.loc[frame["D2C"] == period].reset_index(drop=True)


def _mock_fetch(raw: pd.DataFrame) -> AsyncMock:
    async def fetch(classifications: dict[str, str], **_kwargs: object) -> pd.DataFrame:
        return raw.loc[raw["D3C"] == classifications["48"]].copy()

    return AsyncMock(side_effect=fetch)


@pytest.mark.parametrize("period", ["201007", "202307", "202607"])
async def test_lspa_catalogo_preserva_celulas_e_dimensoes(period):
    raw = _rows(period)
    with helpers.collect_failures() as check:
        for produto, official_name in EXPECTED_NAMES.items():
            with check(produto), helpers.isolated_dataset_case((period, produto)) as monkeypatch:
                expected = raw.loc[raw["D3N"].str.split(" ", n=1).str[1] == official_name]
                assert len(expected) == 4
                fetch = _mock_fetch(raw)
                monkeypatch.setattr(client, "fetch_sidra", fetch)
                result = await ibge.lspa(produto, ano=int(period[:4]), mes=int(period[4:]))
                fetch.assert_awaited_once()
                assert fetch.await_args.kwargs["classifications"] == {"48": expected["D3C"].iloc[0]}
                assert result["produto"].unique().tolist() == [produto]
                assert result["variavel"].tolist() == expected["D4N"].tolist()
                assert result["unidade"].tolist() == expected["MN"].tolist()
                expected_values = pd.to_numeric(expected["V"].replace("-", "0"), errors="coerce")
                pd.testing.assert_series_equal(
                    result["valor"],
                    expected_values.astype("float64").reset_index(drop=True),
                    check_names=False,
                    check_exact=True,
                )
                validate_dataset(result, "lspa")
                assert result["ano"].tolist() == [int(period[:4])] * len(expected)
                assert result["mes"].tolist() == [int(period[4:])] * len(expected)
                assert result["localidade_cod"].tolist() == pd.to_numeric(expected["D1C"]).tolist()
                assert result["localidade"].tolist() == expected["D1N"].tolist()
                if period == "202607" and produto == "soja":
                    assert result["ano"].tolist() == [2026] * 4
                    assert result["mes"].tolist() == [7] * 4
                    assert result["localidade_cod"].tolist() == [1] * 4
                    assert result["localidade"].tolist() == ["Brasil"] * 4
                    assert result["produto"].tolist() == ["soja"] * 4
                    assert result["variavel_cod"].tolist() == [109, 216, 35, 36]
                    assert result["variavel"].tolist() == [
                        "Área plantada",
                        "Área colhida",
                        "Produção",
                        "Rendimento médio",
                    ]
                    assert result["valor"].tolist() == [48391709.0, 48331820.0, 174887646.0, 3618.0]
                    assert result["unidade"].tolist() == [
                        "Hectares",
                        "Hectares",
                        "Toneladas",
                        "Quilogramas por Hectare",
                    ]


@pytest.mark.parametrize(
    "period,expected_products,production",
    [
        ("201007", {"cafe"}, 2753091.0),
        ("201112", {"cafe"}, 2670676.0),
        ("201201", {"cafe_arabica", "cafe_canephora"}, 2957597.0),
        ("202607", {"cafe_arabica", "cafe_canephora"}, 3959028.0),
    ],
)
async def test_lspa_cafe_transicao_oficial_sem_duplicar_total(
    period, expected_products, production, monkeypatch
):
    fetch = _mock_fetch(_rows(period))
    monkeypatch.setattr(client, "fetch_sidra", fetch)
    result = await ibge.lspa("cafe", ano=period[:4], mes=period[4:])
    assert set(result["produto"]) == expected_products
    assert len(result) == len(expected_products) * 4
    assert result.loc[result["variavel_cod"] == 35, "valor"].sum() == production
    validate_dataset(result, "lspa")


def _lspa_oficial() -> pd.DataFrame:
    return pd.DataFrame(json.loads(LSPA_OFICIAL.read_text(encoding="utf-8"))["rows"])


def test_lspa_layout_sem_variavel_falha():
    with pytest.raises(ParseError, match="ausentes"):
        lspa_parser.parse_lspa(_lspa_oficial().drop(columns="D4N"), "milho_1")


@pytest.mark.parametrize("period", ["202313", "202300", "2023", "109"])
def test_lspa_periodo_malformado_nao_inventa_mes(period):
    raw = _lspa_oficial().head(1).copy()
    raw["D2C"] = period
    with pytest.raises(ParseError, match="Período"):
        lspa_parser.parse_lspa(raw, "milho_1")
