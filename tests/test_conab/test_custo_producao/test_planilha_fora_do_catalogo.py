from __future__ import annotations

from types import SimpleNamespace

from agrobr.conab._custo_producao import api, models
from agrobr.exceptions import InvalidParameterError
from tests.helpers import levanta_exatamente


def _recurso(planilha: str) -> models.RecursoCusto:
    return models.RecursoCusto(
        planilha=planilha,
        cultura="soja",
        titulo=planilha,
        pagina_url="https://www.gov.br/conab",
    )


ATUAL = "serie-historica-custos-soja-1997-a-2026.xls"
ANTERIOR = "serie-historica-custos-soja-1997-a-2025.xls"


async def test_planilha_fora_do_catalogo_diz_isso_e_lista_as_do_catalogo():
    baixadas = []

    async def catalogo(_cultura, **_opcoes):
        return [_recurso(ATUAL)]

    async def planilha(recurso):
        baixadas.append(recurso)
        raise AssertionError("não deveria baixar planilha")

    aquisicao = SimpleNamespace(catalog=catalogo, workbook=planilha, culturas_catalogo=["soja"])
    consulta = models.ConsultaCusto(cultura="soja", planilha=ANTERIOR, aba="Barreiras-BA-2025")

    with levanta_exatamente(InvalidParameterError, "não está no catálogo") as erro:
        await api._load(consulta, aquisicao)

    mensagem = str(erro.value)
    assert ANTERIOR in mensagem
    assert f"Planilhas no catálogo (1): {ATUAL}." in mensagem
    assert "catalogo_custos(produto)" in mensagem
    assert "candidatas" not in mensagem
    assert baixadas == []


def test_planilha_fora_do_catalogo_corta_a_lista_em_15():
    nomes = [f"serie-historica-custos-cultura-{n:02d}.xls" for n in range(17)]

    with levanta_exatamente(InvalidParameterError, "não está no catálogo") as erro:
        api._resource([_recurso(nome) for nome in nomes], ANTERIOR)

    mensagem = str(erro.value)
    assert f"Planilhas no catálogo (17): {', '.join(nomes[:15])} e mais 2." in mensagem
    assert nomes[15] not in mensagem


def test_planilha_fora_de_catalogo_vazio_diz_nenhuma():
    with levanta_exatamente(InvalidParameterError, "não está no catálogo") as erro:
        api._resource([], ANTERIOR)

    assert "Planilhas no catálogo (0): nenhuma." in str(erro.value)


def test_planilha_no_catalogo_segue_selecionada():
    atual = _recurso(ATUAL)

    assert api._resource([_recurso(ANTERIOR), atual], ATUAL) is atual
