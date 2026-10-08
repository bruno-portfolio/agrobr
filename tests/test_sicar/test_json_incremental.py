from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from agrobr import contracts
from agrobr.alt.sicar import client, parser
from agrobr.alt.sicar.models import COLUNAS_IMOVEIS, COLUNAS_IMOVEIS_GEO
from agrobr.exceptions import ParseError
from tests.helpers import collect_failures

FIXTURE = Path(__file__).parents[1] / "golden_data/sicar/selecao_20260906"
GEO_VAZIO = Path(__file__).parents[1] / "golden_data/sicar/geo_20260922/df_vazio_geo.json"
TEXTO = str(pd.Series([""]).dtype)
TIPOS_VAZIOS = {
    "cod_imovel": TEXTO,
    "status": TEXTO,
    "data_criacao": "datetime64[ns, UTC]",
    "data_atualizacao": "datetime64[ns, UTC]",
    "area_ha": "float64",
    "condicao": TEXTO,
    "uf": TEXTO,
    "municipio": TEXTO,
    "cod_municipio_ibge": "Int64",
    "modulos_fiscais": "float64",
    "tipo": TEXTO,
    "cod_municipio": "Int64",
}


def load_capture(name: str) -> bytes:
    content = (FIXTURE / name).read_bytes()
    manifest = json.loads((FIXTURE / "manifest.json").read_text(encoding="utf-8"))
    resource = next(item for item in manifest["requests"] if item["body_file"] == name)
    assert hashlib.sha256(content).hexdigest() == resource["sha256"]
    return content


def test_json_incompativel_vira_parse_error():
    base = json.loads(load_capture("df_same_record.json"))
    propriedades: list[tuple[str, Any]] = [
        ("cod_imovel", ""),
        ("cod_imovel", None),
        ("status_imovel", ""),
        ("status_imovel", None),
        ("status_imovel", "INVALIDO"),
        ("tipo_imovel", ""),
        ("tipo_imovel", None),
        ("tipo_imovel", "INVALIDO"),
        ("uf", ""),
        ("uf", "XX"),
        ("municipio", ""),
        ("municipio", None),
        ("cod_municipio_ibge", None),
        ("cod_municipio_ibge", 5107925),
        ("dat_criacao", "2014-11-12T02:23:04.299"),
        ("dat_criacao", 1415766184),
        ("dat_criacao", "9999-12-31T00:00:00Z"),
        ("data_atualizacao", "2026-09-01T13:44:58.004"),
        ("area", float("nan")),
        ("area", float("inf")),
        ("area", -1),
        ("area", True),
        ("m_fiscal", -1),
    ]
    casos: list[tuple[object, dict[str, Any], str | None]] = []
    for campo, valor in propriedades:
        documento = copy.deepcopy(base)
        documento["features"][0]["properties"][campo] = valor
        casos.append(((campo, valor), documento, None))
    for contagem in (0, 2, True, "1"):
        casos.append(
            (("numberReturned", contagem), base | {"numberReturned": contagem}, "numberReturned")
        )
    for identificador in (None, "", 123, True, "sicar_imoveis_go.invalid"):
        documento = copy.deepcopy(base)
        documento["features"][0]["id"] = identificador
        casos.append((("id", identificador), documento, "id"))
    sem_id = copy.deepcopy(base)
    sem_id["features"][0].pop("id")
    casos.append(("id ausente", sem_id, "id"))
    sem_area = copy.deepcopy(base)
    del sem_area["features"][0]["properties"]["area"]
    casos.append(("area ausente", sem_area, "Propriedades obrigatorias"))
    with collect_failures() as check:
        for caso, documento, mensagem in casos:
            with check(caso), pytest.raises(ParseError, match=mensagem):
                parser.parse_imoveis_json([json.dumps(documento).encode()])
        with check("csv"), pytest.raises(ParseError, match="FeatureCollection"):
            parser.parse_imoveis_json(
                [b"cod_imovel,status_imovel,dat_criacao,area,uf\nDF-1,AT,,1,DF\n"]
            )


def test_numero_em_texto_com_virgula_vira_numero():
    documento = json.loads(load_capture("df_same_record.json"))
    documento["features"][0]["properties"].update(
        area="12,5846", m_fiscal="2,5169", cod_municipio_ibge="5300108"
    )
    frame = parser.parse_imoveis_json([json.dumps(documento).encode()])
    assert frame.loc[0, ["area_ha", "modulos_fiscais", "cod_municipio_ibge"]].tolist() == [
        12.5846,
        2.5169,
        5300108,
    ]


def test_vazio_preserva_contrato_utc():
    pytest.importorskip("geopandas")
    with collect_failures() as check:
        for nome, pages in [
            ("sem páginas", []),
            ("página vazia", [b'{"type":"FeatureCollection","features":[],"numberReturned":0}']),
        ]:
            with check(nome):
                frame = parser.parse_imoveis_json(pages)
                assert frame.empty
                assert list(frame.columns) == COLUNAS_IMOVEIS
                assert frame.dtypes.astype(str).to_dict() == TIPOS_VAZIOS
                contracts.validate_dataset(frame, "sicar_imoveis")
        for nome, pages in [
            ("geo sem páginas", []),
            ("geo só vazias", [GEO_VAZIO.read_bytes()] * 2),
        ]:
            with check(nome):
                frame = parser.parse_imoveis_geojson(pages)
                assert frame.empty
                assert frame.geometry.name == "geometry"
                assert list(frame.columns) == COLUNAS_IMOVEIS_GEO
                tipos = frame.drop(columns="geometry").dtypes.astype(str).to_dict()
                assert tipos == TIPOS_VAZIOS


@pytest.mark.parametrize(
    "failure", ["empty", "missing_final", "number_matched", "duplicate", "overlap", "cardinality"]
)
async def test_paginacao_inconsistente_falha_sem_simular_completude(
    monkeypatch: pytest.MonkeyPatch, failure: str
):
    data = json.loads(load_capture("df_tabular.json"))
    if failure == "empty":
        data["features"] = []
    elif failure == "missing_final":
        data["features"].pop()
    elif failure == "number_matched":
        data["numberMatched"] = 19
    elif failure == "duplicate":
        data["features"][-1] = data["features"][0]
    data["numberReturned"] = len(data["features"]) + (failure == "cardinality")
    pages = [data]
    if failure == "overlap":
        monkeypatch.setattr(client, "PAGE_SIZE", 10)
        pages = [
            data | {"features": data["features"][:10], "numberReturned": 10},
            data | {"features": data["features"][9:19], "numberReturned": 10},
        ]
    payloads = iter(json.dumps(page).encode() for page in pages)

    async def fetch(url: str, **_kwargs: Any) -> bytes:
        if "resultType=hits" in url:
            return load_capture("df_hits.xml")
        return next(payloads)

    monkeypatch.setattr(client, "fetch_wfs", fetch)
    mensagem = {
        "duplicate": "id de feature repetido",
        "overlap": "id de feature repetido",
        "cardinality": "Pagina JSON invalida",
    }.get(failure, "Varredura inconsistente")
    with pytest.raises(ParseError, match=mensagem):
        await client.fetch_imoveis("DF", "cod_municipio_ibge=5300108")
