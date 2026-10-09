from __future__ import annotations

import csv
import io
import re
from pathlib import Path

import pytest

from agrobr import datasets
from agrobr.alt.mapa_psr import api, models
from agrobr.exceptions import InvalidParameterError
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
@pytest.mark.parametrize("municipio", [CAXIAS, int(CAXIAS), "caxias do sul"])
async def test_filtro_municipio_pega_as_apolices_rotuladas_com_o_distrito(
    monkeypatch, camada, tipo, por_rotulo, municipio
):
    _servir(monkeypatch, _caxias_e_sem_geocodigo())
    with sem_excecao():
        if camada == "fonte":
            frame = await getattr(api, tipo)(municipio=municipio)
        else:
            frame = await datasets.seguro_rural(tipo=tipo, municipio=municipio)
    assert frame["municipio"].value_counts().to_dict() == por_rotulo
    assert frame["cd_ibge"].eq(CAXIAS).all()


async def test_filtro_municipio_inclui_a_linha_sem_codigo_pelo_nome_inteiro(monkeypatch):
    _servir(monkeypatch, _caxias_e_sem_geocodigo())
    with sem_excecao():
        frame = await api.apolices(municipio="Santa Bárbara d'Oeste", uf="SP")
    assert frame["municipio"].tolist() == ["SANTA BÁRBARA D'OESTE"] * 3
    assert frame["cd_ibge"].isna().all()


@pytest.mark.parametrize(
    ("valor", "uf", "trecho"),
    [
        ("430510", None, "7 dígitos"),
        ("43051080", None, "7 dígitos"),
        ("Caxias", "RS", "Caxias do Sul/RS (4305108)"),
        ("Bom Jesus", None, "informe a uf"),
        (CAXIAS, "PR", "não pertence à UF PR"),
        (True, None, "nome ou o código"),
    ],
)
async def test_municipio_invalido_recusado_antes_da_rede(monkeypatch, valor, uf, trecho):
    monkeypatch.setattr(api.client, "open_periodo", _sem_rede)
    for chamar in (api.apolices, api.sinistros, datasets.seguro_rural):
        with levanta_exatamente(InvalidParameterError, match=re.escape(trecho)):
            await chamar(municipio=valor, uf=uf)


async def test_filtro_municipio_sem_coluna_de_geocodigo_usa_o_nome_inteiro(monkeypatch):
    _servir(monkeypatch, _sem_coluna(CAXIAS_2024.read_bytes(), "CD_GEOCMU"))
    with sem_excecao():
        frame = await api.apolices(municipio=CAXIAS)
    assert frame["municipio"].unique().tolist() == ["CAXIAS DO SUL"]
    assert len(frame) == APOLICES_POR_ROTULO["CAXIAS DO SUL"]


async def test_evento_com_apolices_recusado_antes_da_rede(monkeypatch):
    monkeypatch.setattr(api.client, "open_periodo", _sem_rede)
    with levanta_exatamente(InvalidParameterError, match="evento só filtra"):
        await datasets.seguro_rural(evento="granizo")


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
    assert meta.contract_version == ("1.2" if argumentos.get("tipo") == "sinistros" else "2.1")


@pytest.mark.parametrize("tipo", ["apolices", "sinistros"])
async def test_vazio_tem_os_dtypes_do_cheio_real(monkeypatch, tipo):
    _servir(monkeypatch, CAXIAS_2024.read_bytes())
    with sem_excecao():
        cheio = await getattr(api, tipo)()
        filtrado = await getattr(api, tipo)(uf="AC")

    async def sem_arquivo(*_args):
        return {}, []

    monkeypatch.setattr(api, "_urls_dos_periodos", sem_arquivo)
    with sem_excecao():
        sem_periodo = await getattr(api, tipo)()
    assert len(cheio) and filtrado.empty and sem_periodo.empty
    assert filtrado.dtypes.to_dict() == cheio.dtypes.to_dict()
    assert sem_periodo.dtypes.to_dict() == cheio.dtypes.to_dict()
    assert str(cheio.dtypes["ano_apolice"]) == "Int64"
    assert {str(cheio.dtypes[coluna]) for coluna in models.COLUNAS_DATA} == {"datetime64[ns]"}


@pytest.mark.parametrize(
    "chamar",
    [
        lambda: api.apolices(None, None, None, None, None, None, True),
        lambda: api.sinistros(None, None, None, None, None, None, None, True),
    ],
)
async def test_flags_somente_nomeadas(chamar):
    with pytest.raises(TypeError):
        await chamar()
