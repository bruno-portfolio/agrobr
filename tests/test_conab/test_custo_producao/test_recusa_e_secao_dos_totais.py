from __future__ import annotations

import re
import warnings
from pathlib import Path
from types import SimpleNamespace

from agrobr.conab._custo_producao import _context, _parse, _workbook, api, models
from agrobr.exceptions import InvalidParameterError
from tests.helpers import levanta_exatamente

SOJA = (
    Path(__file__).resolve().parents[2] / "golden_data" / "conab" / "custos_20260908" / "soja.xls"
)
RECURSO = models.RecursoCusto(
    planilha="serie-historica-custos-soja-1997-a-2025.xls",
    cultura="soja",
    titulo="Série histórica de custos da soja",
    pagina_url="https://www.gov.br/conab",
)
NAO_IDENTIFICADAS = [
    "P. do Leste-MT-1997",
    "Francisco Beltrão-PR-2011",
    "Francisco Beltrão-PR-2012",
    "OGM-Francisco Beltrão-PR-2011",
    "OGM-Francisco Beltrão-PR-2012",
    "Londrina-PR-1999",
    "Toledo-PR-2011",
    "Toledo-PR-2012",
    "OGM-Toledo-PR-2011",
    "OGM-Toledo-PR-2012",
]


async def _catalogo(_cultura, **_opcoes):
    return [RECURSO]


async def _planilha(_recurso):
    return SOJA.read_bytes()


AQUISICAO = SimpleNamespace(catalog=_catalogo, workbook=_planilha, culturas_catalogo=["soja"])


async def _recusa(**filtros) -> str:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        with levanta_exatamente(InvalidParameterError, "contexto não identificado") as erro:
            await api._load(models.ConsultaCusto(cultura="soja", **filtros), AQUISICAO)
    return str(erro.value)


async def test_recusa_lista_as_abas_sem_contexto_e_os_candidatos_do_filtro():
    mensagem = await _recusa(uf="MT", local="Sorriso", ano=2024)

    assert "10 aba(s) com contexto não identificado" in mensagem
    assert ", ".join(NAO_IDENTIFICADAS) in mensagem
    assert (
        "casam com o filtro (1): OGM-Sorriso-MT-2024 (Sorriso/MT, ano 2024, safra 2024/25)."
        in mensagem
    )
    assert "aba=" in mensagem


async def test_recusa_com_muitos_candidatos_mostra_15_e_conta_o_resto():
    mensagem = await _recusa(uf="MT", local="Sorriso")

    candidatos = re.search(r"casam com o filtro \(31\): (.*) e mais 16\.", mensagem)
    assert candidatos is not None, mensagem
    assert len(candidatos.group(1).split("), ")) == 15


def test_linhas_de_total_saem_sem_secao():
    livro = _workbook.Workbook(SOJA.read_bytes())
    try:
        contextos, _ = _context.inventory(livro, RECURSO)
        (contexto,) = [c for c in contextos if c.aba == "OGM-Sorriso-MT-2024"]
        resultado = _parse.parse_selected(livro.read(contexto.aba), contexto)
    finally:
        livro.close()
    linhas = resultado.observacoes

    totais = [linha for linha in linhas if linha.tipo_linha == "total"]
    assert totais and all(linha.secao is None for linha in totais)
    assert any(linha.item.upper().startswith("CUSTO VARI") for linha in totais)
    outras = [linha for linha in linhas if linha.tipo_linha != "total"]
    assert all(linha.secao is not None for linha in outras)
    financeiras = [linha.secao for linha in outras if linha.secao and linha.secao.startswith("III")]
    assert financeiras
