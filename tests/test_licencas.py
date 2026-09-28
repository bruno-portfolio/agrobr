from __future__ import annotations

import re
import unicodedata
from datetime import UTC, datetime
from pathlib import Path

import pytest

from agrobr import constants, datasets
from agrobr.constants import Fonte
from agrobr.models import MetaInfo
from tests.helpers import sem_excecao

DOCS = Path(__file__).parents[1] / "docs"
LINHA = re.compile(r"^\| \*\*(.+?)\*\* \|[^|]*\|[^|]*\| `(\w+)`")
ROTULOS = {
    "cepea/esalq": ("cepea",),
    "conab": ("conab",),
    "ibge/sidra": ("ibge",),
    "nasa power": ("nasa_power",),
    "bcb/sicor": ("bcb",),
    "comexstat": ("comexstat",),
    "anda": ("anda",),
    "antaq": ("antaq",),
    "anp diesel": ("anp_diesel",),
    "antt pedagio": ("antt_pedagio",),
    "mapa psr": ("mapa_psr",),
    "sicar": ("sicar",),
    "abiove": ("abiove",),
    "anec": ("anec",),
    "usda psd": ("usda",),
    "un comtrade": ("comtrade",),
    "cftc cot": ("cftc",),
    "imea": ("imea",),
    "deral": ("deral",),
    "inmet": ("inmet",),
    "noticias agricolas": ("noticias_agricolas",),
    "queimadas/inpe": ("queimadas", "inpe"),
    "desmatamento prodes/deter": ("desmatamento", "inpe"),
    "mapbiomas": ("mapbiomas",),
    "conab progresso": ("conab",),
    "ibge ppm": ("ibge",),
    "ibge abate": ("ibge",),
    "ibge censo agro": ("ibge",),
    "ibge censo agro historico": ("ibge",),
    "ibge censo agro municipal 1985": ("ibge",),
    "b3 futuros agro": ("b3",),
    "conab ceasa/prohort": ("conab_ceasa", "conab_prohort"),
    "mapa agrofit (defensivos)": ("defensivos",),
    "mapa agrofit (pesticides)": ("defensivos",),
    "zarc": ("zarc",),
    "ana/snirh": ("ana",),
    "funai terras indigenas": ("funai",),
    "funai indigenous lands": ("funai",),
    "ibama embargos": ("ibama",),
    "ibama embargoes": ("ibama",),
    "icmbio ucs federais": ("icmbio",),
    "icmbio federal ucs": ("icmbio",),
    "incra quilombolas": ("incra",),
    "lista suja": ("lista_suja",),
    "mapbiomas alerta": ("mapbiomas_alerta",),
    "sfb": ("sfb",),
    "rnc/cultivarweb": ("rnc",),
    "embrapa solos": ("embrapa_solos",),
    "acervo fundiario/incra": ("acervo_fundiario",),
    "fundacao rio verde": ("rio_verde",),
    "unica": ("unica",),
}


def _rotulo(texto: str) -> str:
    sem_acento = unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode()
    return sem_acento.strip().lower()


def _tabela_da_doc(pagina: str) -> dict[str, str]:
    linhas = (DOCS / pagina).read_text(encoding="utf-8").splitlines()
    return {_rotulo(achado[1]): achado[2] for linha in linhas if (achado := LINHA.match(linha))}


@pytest.mark.parametrize("pagina", ["licenses.md", "licenses.en.md"])
def test_tabela_de_licencas_do_codigo_e_a_da_doc(pagina: str):
    doc = _tabela_da_doc(pagina)

    assert set(doc) <= set(ROTULOS), sorted(set(doc) - set(ROTULOS))
    divergentes = {
        (rotulo, chave): (classe, constants.LICENCAS.get(chave))
        for rotulo, classe in doc.items()
        for chave in ROTULOS[rotulo]
        if constants.LICENCAS.get(chave) != classe
    }
    assert divergentes == {}
    assert {chave for rotulo in doc for chave in ROTULOS[rotulo]} == set(constants.LICENCAS)


def test_toda_fonte_tem_classificacao():
    assert {str(fonte) for fonte in Fonte} - set(constants.LICENCAS) == set()


def _meta(source: str, selected_source: str = "", data_sources: tuple[str, ...] = ()) -> MetaInfo:
    return MetaInfo(
        source=source,
        source_url="",
        source_method="teste",
        fetched_at=datetime(2026, 9, 27, tzinfo=UTC),
        selected_source=selected_source,
        data_sources=list(data_sources),
    )


@pytest.mark.parametrize(
    ("meta", "esperada"),
    [
        (_meta("datasets.exportacao/abiove", "abiove"), "zona_cinza"),
        (_meta("datasets.exportacao/comexstat", "comexstat"), "livre"),
        (_meta("cepea", "cache", ("cepea",)), "nc"),
        (
            _meta("datasets.preco_diario/cepea", "cepea", ("cepea", "noticias_agricolas")),
            "restrito",
        ),
        (_meta("conab_ceasa"), "zona_cinza"),
        (_meta("ibge_lspa", "ibge_lspa"), "livre"),
        (_meta("datasets.queimadas/inpe", "inpe"), "livre"),
        (_meta("health_check"), None),
    ],
    ids=[
        "fallback_abiove",
        "comexstat",
        "cache_do_cepea",
        "cascata_na",
        "subfonte_ceasa",
        "prefixo_ibge",
        "apelido_inpe",
        "fora_da_tabela",
    ],
)
def test_licenca_do_metainfo(meta: MetaInfo, esperada: str | None):
    assert meta.license == esperada
    assert meta.to_dict()["license"] == esperada
    with sem_excecao():
        de_volta = MetaInfo.from_dict(meta.to_dict())
    assert de_volta.license == esperada


def test_info_do_dataset_diz_as_licencas_de_cada_fonte():
    info = datasets.info("exportacao")

    assert info["licenses"] == {"comexstat": "livre", "abiove": "zona_cinza"}
    assert "License: livre (comexstat), zona_cinza (abiove)" in datasets.describe("exportacao")
