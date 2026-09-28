from __future__ import annotations

import csv
import io
from pathlib import Path

import pytest

from agrobr import datasets
from agrobr.alt.mapa_psr import api
from agrobr.exceptions import InvalidParameterError, ParseError
from tests.helpers import binary_stream, levanta_exatamente, sem_excecao

GOLDEN = Path(__file__).resolve().parents[1] / "golden_data/mapa_psr"
SEM_GEOCODIGO = GOLDEN / "sem_geocodigo_20260918/apolices.csv"
CAXIAS_2024 = GOLDEN / "caxias_do_sul_2024_20260918/apolices.csv"
CAXIAS = "4305108"
APOLICES_POR_ROTULO = {
    "CAXIAS DO SUL": 274,
    "FAZENDA SOUZA": 241,
    "CRIÚVA": 78,
    "VILA OLIVA": 48,
    "VILA SECA": 32,
    "SANTA LÚCIA DO PIAÍ": 20,
}
SINISTROS_POR_ROTULO = {
    "CAXIAS DO SUL": 30,
    "FAZENDA SOUZA": 28,
    "VILA OLIVA": 5,
    "CRIÚVA": 3,
    "SANTA LÚCIA DO PIAÍ": 1,
}


def _servir(monkeypatch: pytest.MonkeyPatch, conteudo: bytes) -> None:
    monkeypatch.setattr(api, "_resolve_periodos", lambda *_: ["2016-2024"])
    monkeypatch.setattr(api.client, "open_periodo", lambda _: binary_stream(conteudo))


def _caxias_e_sem_geocodigo() -> bytes:
    cabecalho, _, linhas = SEM_GEOCODIGO.read_bytes().partition(b"\n")
    caxias = CAXIAS_2024.read_bytes()
    assert caxias.startswith(cabecalho + b"\n")
    return caxias + linhas


def _sem_coluna(conteudo: bytes, coluna: str) -> bytes:
    linhas = list(csv.reader(io.StringIO(conteudo.decode("utf-8")), delimiter=";"))
    posicao = linhas[0].index(coluna)
    saida = io.StringIO(newline="")
    csv.writer(saida, delimiter=";", lineterminator="\n").writerows(
        linha[:posicao] + linha[posicao + 1 :] for linha in linhas
    )
    return saida.getvalue().encode("utf-8")


def _sem_rede(_periodo: str) -> None:
    raise AssertionError("a validação tem de vir antes da rede")


@pytest.mark.parametrize("camada", ["fonte", "dataset"])
@pytest.mark.parametrize(
    ("tipo", "por_rotulo"),
    [("apolices", APOLICES_POR_ROTULO), ("sinistros", SINISTROS_POR_ROTULO)],
)
async def test_filtro_cd_ibge_pega_as_apolices_rotuladas_com_o_distrito(
    monkeypatch, camada, tipo, por_rotulo
):
    _servir(monkeypatch, _caxias_e_sem_geocodigo())
    with sem_excecao():
        if camada == "fonte":
            por_codigo = await getattr(api, tipo)(cd_ibge=CAXIAS)
            por_nome = await getattr(api, tipo)(municipio="Caxias do Sul")
        else:
            por_codigo = await datasets.seguro_rural(tipo=tipo, cd_ibge=CAXIAS)
            por_nome = await datasets.seguro_rural(tipo=tipo, municipio="Caxias do Sul")
    assert por_codigo["municipio"].value_counts().to_dict() == por_rotulo
    assert por_codigo["cd_ibge"].eq(CAXIAS).all()
    assert len(por_nome) == por_rotulo["CAXIAS DO SUL"]
    assert por_nome["cd_ibge"].eq(CAXIAS).all()


@pytest.mark.parametrize(
    "valor",
    [4305108, "430510", "43051080", " 4305108"],
    ids=["inteiro", "seis_digitos", "oito_digitos", "espaco_inicial"],
)
async def test_cd_ibge_invalido_recusado_antes_da_rede(monkeypatch, valor):
    monkeypatch.setattr(api.client, "open_periodo", _sem_rede)
    for chamar in (api.apolices, api.sinistros, datasets.seguro_rural):
        with levanta_exatamente(InvalidParameterError, match="cd_ibge deve ser o código IBGE"):
            await chamar(cd_ibge=valor)


async def test_filtro_cd_ibge_sem_coluna_de_geocodigo_levanta_parse_error(monkeypatch):
    _servir(monkeypatch, _sem_coluna(CAXIAS_2024.read_bytes(), "CD_GEOCMU"))
    with levanta_exatamente(ParseError, match="CD_GEOCMU ausente"):
        await api.apolices(cd_ibge=CAXIAS)


@pytest.mark.parametrize(
    ("argumentos", "linhas"),
    [
        ({"produto": "uva"}, 470),
        ({"uf": "PR"}, 18),
        ({"ano_inicio": 2024}, 731),
        ({"ano_fim": 2023}, 30),
        ({"tipo": "sinistros"}, 72),
        ({"tipo": "sinistros", "evento": "GRANIZO"}, 60),
        ({"tipo": "sinistros", "evento": "seca"}, 5),
        ({"tipo": "sinistros", "evento": "."}, 0),
        ({"tipo": "sinistros", "produto": "uva", "evento": "granizo"}, 30),
    ],
    ids=[
        "cultura",
        "uf",
        "ano_inicio",
        "ano_fim",
        "sinistros",
        "evento_em_caixa_alta",
        "evento",
        "evento_literal",
        "cultura_e_evento",
    ],
)
async def test_seguro_rural_repassa_os_filtros_a_fonte(monkeypatch, argumentos, linhas):
    _servir(monkeypatch, _caxias_e_sem_geocodigo())
    with sem_excecao():
        frame, meta = await datasets.seguro_rural(**argumentos, return_meta=True)
    assert len(frame) == linhas
    assert meta.contract_version == ("1.1" if argumentos.get("tipo") == "sinistros" else "1.2")
