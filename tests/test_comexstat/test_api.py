from __future__ import annotations

from unittest.mock import patch

import pytest

from agrobr import comexstat
from agrobr.comexstat import api
from agrobr.datasets.deterministic import deterministic
from agrobr.exceptions import InvalidParameterError
from tests.helpers import comexstat_csv, install_comexstat_http


@pytest.mark.parametrize("fluxo", ["exportacao", "importacao"])
@pytest.mark.parametrize(
    "kwargs",
    [
        {"ano": "2024"},
        {"ano": True},
        {"ano": 1996},
        {"ano": 9999},
        {"uf": "XX"},
        {"uf": 3},
        {"agregacao": "diaria"},
        {"agregacao": []},
        {"pais": True},
        {"pais": "１６０"},
        {"pais": "Brasil"},
        {"pais": -1},
        {"pais": 1000},
        {"via": "001"},
        {"urf": "1e6"},
        {"via": 4.0},
        {"as_polars": 1},
        {"return_meta": "sim"},
        {"max_linhas": 0},
        {"max_linhas": True},
        {"max_memoria_bytes": 0},
        {"max_memoria_bytes": None},
    ],
)
async def test_invalid_parameters_before_io(monkeypatch, fluxo, kwargs):
    calls = install_comexstat_http(monkeypatch, b"unreached")
    with (
        patch.object(api.client, "_temporary_file") as spool,
        pytest.raises(InvalidParameterError),
    ):
        await getattr(comexstat, fluxo)("soja", **kwargs)
    assert calls == []
    spool.assert_not_called()


@pytest.mark.parametrize("fluxo", ["exportacao", "importacao"])
async def test_monthly_and_detail_preserve_flow_measures(monkeypatch, fluxo):
    install_comexstat_http(monkeypatch, comexstat_csv(fluxo=fluxo))
    function = getattr(comexstat, fluxo)
    monthly = await function("soja", ano=2024)
    detail = await function("soja", ano=2024, agregacao="detalhado")
    assert len(monthly) == len(detail) == 3
    assert "qtd_estatistica" not in monthly and "volume_ton" not in detail
    assert monthly["volume_ton"].tolist() == [50000.0, 40000.0, 30000.0]
    assert detail["cod_urf"].tolist() == ["0817800", "0817800", "0917502"]
    assert detail["cod_via"].tolist() == ["04", "04", "07"]
    assert "cod_porto" not in detail
    assert str(detail["kg_liquido"].dtype) == "float64"
    assert str(detail["valor_fob_usd"].dtype) == "float64"
    if fluxo == "importacao":
        assert monthly["valor_frete_usd"].tolist() == [10.0, 11.0, 12.0]
        assert detail["valor_seguro_usd"].tolist() == [0.0, 1.0, 2.0]
        assert len(monthly.columns) == 9 and len(detail.columns) == 13
    else:
        assert len(monthly.columns) == 7 and len(detail.columns) == 11


@pytest.mark.parametrize("fluxo", ["exportacao", "importacao"])
async def test_codes_canonicalized_only_in_request(monkeypatch, fluxo):
    install_comexstat_http(monkeypatch, comexstat_csv(fluxo=fluxo))
    frame, meta = await getattr(comexstat, fluxo)(
        " SOJA ",
        ano=2024,
        uf=" pr ",
        pais=586,
        via="7",
        urf=917502,
        agregacao="detalhado",
        return_meta=True,
    )
    assert len(frame) == 1
    assert frame.iloc[0]["cod_urf"] == "0917502"
    assert frame.iloc[0]["cod_unidade"] == "21"
    assert frame.iloc[0]["qtd_estatistica"] == 500
    assert frame.iloc[0]["kg_liquido"] == 30000000
    query = meta.source_details["query"]
    assert query["filtros"] == {"uf": "PR", "pais": "586", "via": "07", "urf": "0917502"}
    assert query["pedido"]["urf"] == 917502 and query["pedido"]["uf"] == " pr "


@pytest.mark.parametrize("uf", ["ND", "EX"])
async def test_special_uf_and_code_zero_preserved(monkeypatch, uf):
    row = ["2024", "01", "12019000", "10", "000", uf, "00", "0000000", "0", "0", "0"]
    install_comexstat_http(monkeypatch, comexstat_csv(rows=[row]))
    frame = await comexstat.exportacao(
        "soja", ano=2024, uf=uf.lower(), pais=0, via=0, urf=0, agregacao="detalhado"
    )
    assert frame["uf"].tolist() == [uf]
    assert frame["cod_pais"].tolist() == ["000"]
    assert frame["cod_via"].tolist() == ["00"]
    assert frame["cod_urf"].tolist() == ["0000000"]
    assert frame["valor_fob_usd"].tolist() == [0.0]


@pytest.mark.parametrize(
    "function,args",
    [("exportacao", ("soja",)), ("importacao", ("soja",)), ("dicionario", ("paises",))],
)
async def test_deterministic_rejected_before_io(monkeypatch, function, args):
    calls = install_comexstat_http(monkeypatch, b"unreached")
    async with deterministic("2024-06-15"):
        with pytest.raises(InvalidParameterError, match="deterministic"):
            await getattr(comexstat, function)(*args)
    assert calls == []


async def test_missing_polars_rejected_before_io(monkeypatch):
    calls = install_comexstat_http(monkeypatch, b"unreached")
    original = api.importlib.import_module

    def missing(name, *args, **kwargs):
        if name == "polars":
            raise ImportError("absent")
        return original(name, *args, **kwargs)

    monkeypatch.setattr(api.importlib, "import_module", missing)
    with pytest.raises(ImportError, match="agrobr"):
        await comexstat.exportacao("soja", ano=2024, as_polars=True)
    assert calls == []


@pytest.mark.parametrize("table", [None, 1, "portos", ""])
async def test_dictionary_invalid_table_before_io(monkeypatch, table):
    calls = install_comexstat_http(monkeypatch, b"unreached")
    with pytest.raises(InvalidParameterError):
        await comexstat.dicionario(table)
    assert calls == []
