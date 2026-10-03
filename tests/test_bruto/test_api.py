from __future__ import annotations

import asyncio
import dataclasses
import warnings

import pytest

from agrobr import bruto, sync
from agrobr.bruto import registry
from agrobr.exceptions import InvalidParameterError, UnknownNameError
from tests.test_bruto.conftest import ARQUIVO, PAGINADO, AdaptadorArquivo, AdaptadorPaginas


def test_exporta_a_api_publica():
    assert sorted(bruto.__all__) == ["ColetaBruta", "LimitesBrutos", "RecursoBruto", "coletar"]
    assert sync.bruto.coletar.__name__ == "coletar"


def test_todo_recurso_ligado_tem_o_adaptador_da_fonte(monkeypatch):
    assert all(registrado.habilitado for registrado in registry.RECURSOS.values())
    for registrado in registry.RECURSOS.values():
        adaptador = registry.adaptador(registrado)
        assert callable(adaptador.planejar) and callable(adaptador.adquirir)
    assert len(registry.RECURSOS) == 10 and ("funai", "terras_indigenas") not in registry.RECURSOS
    chave = ("sicar", "imoveis")
    monkeypatch.setitem(
        registry.RECURSOS, chave, dataclasses.replace(registry.RECURSOS[chave], habilitado=False)
    )
    with pytest.raises(InvalidParameterError, match="não está disponível"):
        registry.recurso(*chave)


@pytest.mark.parametrize(
    ("fonte", "recurso", "classe"),
    [
        ("inexistente", "x", UnknownNameError),
        ("ibge", "pam", UnknownNameError),
        (1, "x", UnknownNameError),
    ],
)
async def test_fonte_ou_recurso_fora_da_tabela(tmp_path, fonte, recurso, classe):
    with pytest.raises(classe):
        await bruto.coletar(fonte, recurso, destino=tmp_path)
    assert list(tmp_path.iterdir()) == []


INVALIDOS_ARQUIVO = [
    ({}, "exige uf"),
    ({"uf": "XX"}, "UF inválida"),
    ({"uf": 51}, "sigla em texto"),
    ({"uf": "AL", "bbox": (-48.0, -16.0, -47.0, -15.0), "nome": "a"}, "não aceita bbox"),
    ({"uf": "AL", "tamanho_pagina": 10}, "tamanho_pagina não se aplica"),
    ({"uf": "AL", "compactar": 1}, "compactar deve ser"),
    ({"uf": "AL", "retomar": "sim"}, "retomar deve ser"),
    ({"uf": "AL", "limites": {"max_paginas": 1}}, "limites deve ser"),
    ({"uf": "AL", "nome": "con"}, "dispositivo"),
    ({"uf": "AL", "nome": "COM1"}, "dispositivo"),
    ({"uf": "AL", "nome": "a.b"}, "deve casar"),
    ({"uf": "AL", "nome": "_x"}, "deve casar"),
    ({"uf": "AL", "nome": "x" * 65}, "deve casar"),
    ({"uf": "AL", "bbox_crs": "EPSG:4326"}, "bbox_crs sem bbox"),
]
INVALIDOS_PAGINADO = [
    ({"bbox": (-48.0, -16.0, -47.0, -15.0)}, "nome é obrigatório"),
    ({"bbox": (-48.0, -16.0, -47.0), "nome": "a"}, "4 números"),
    ({"bbox": (True, -16.0, -47.0, -15.0), "nome": "a"}, "4 números"),
    ({"bbox": "-48,-16,-47,-15", "nome": "a"}, "4 números"),
    ({"bbox": (-47.0, -16.0, -48.0, -15.0), "nome": "a"}, "antimeridiano"),
    ({"bbox": (170.0, -16.0, 190.0, -15.0), "nome": "a"}, "antimeridiano"),
    ({"bbox": (float("nan"), -16.0, -47.0, -15.0), "nome": "a"}, "não finito"),
    ({"bbox": (-48.0, -16.0, -47.0, -15.0), "nome": "a", "bbox_crs": "EPSG:3857"}, "bbox_crs deve"),
    ({"tamanho_pagina": 0}, "1 a 1000"),
    ({"tamanho_pagina": 1001}, "1 a 1000"),
    ({"tamanho_pagina": True}, "1 a 1000"),
    ({"tamanho_pagina": 10.0}, "1 a 1000"),
]


@pytest.mark.parametrize(("kwargs", "mensagem"), INVALIDOS_ARQUIVO)
async def test_argumento_invalido_do_arquivo_antes_da_rede(arquivo, tmp_path, kwargs, mensagem):
    with pytest.raises(InvalidParameterError, match=mensagem):
        await bruto.coletar(*ARQUIVO, destino=tmp_path, **kwargs)
    assert arquivo.pedidos == [] and list(tmp_path.iterdir()) == []


@pytest.mark.parametrize(("kwargs", "mensagem"), INVALIDOS_PAGINADO)
async def test_argumento_invalido_do_paginado_antes_da_rede(paginado, tmp_path, kwargs, mensagem):
    with pytest.raises(InvalidParameterError, match=mensagem):
        await bruto.coletar(*PAGINADO, destino=tmp_path, **kwargs)
    assert paginado.pedidos == [] and list(tmp_path.iterdir()) == []


async def test_uf_recusada_e_recorte_obrigatorio_seguem_o_registro(habilitar, fonte, tmp_path):
    habilitar(("ibge", "areas_urbanizadas"), AdaptadorPaginas(fonte))
    habilitar(("ana", "massas_dagua"), AdaptadorPaginas(fonte))

    with pytest.raises(InvalidParameterError, match="não aceita uf"):
        await bruto.coletar("ibge", "areas_urbanizadas", uf="DF", destino=tmp_path)
    with pytest.raises(InvalidParameterError, match="exige uf ou bbox"):
        await bruto.coletar("ana", "massas_dagua", destino=tmp_path)
    assert fonte.pedidos == []


async def test_destino_que_e_arquivo_e_recusado(arquivo, tmp_path):
    destino = tmp_path / "arquivo.txt"
    destino.write_text("x")

    with pytest.raises(InvalidParameterError, match="não é pasta"):
        await bruto.coletar(*ARQUIVO, uf="AL", destino=destino)
    with pytest.raises(InvalidParameterError, match="destino deve ser caminho"):
        await bruto.coletar(*ARQUIVO, uf="AL", destino=1)
    assert arquivo.pedidos == []


@pytest.mark.usefixtures("paginado")
async def test_nome_padrao_e_bbox_normalizada(tmp_path):
    padrao = await bruto.coletar(*PAGINADO, destino=tmp_path)
    recorte = await bruto.coletar(
        *PAGINADO, uf="df", bbox=(-48, -0.0, -47, 1), nome="area_1", destino=tmp_path
    )

    assert padrao.entrada.nome == "brasil" and padrao.entrada.selecao.bbox_crs is None
    selecao = recorte.entrada.selecao
    assert (selecao.uf, selecao.bbox, selecao.bbox_crs) == (
        "DF",
        (-48.0, 0.0, -47.0, 1.0),
        "EPSG:4674",
    )
    assert str(selecao.bbox[1]) == "0.0"


@pytest.mark.usefixtures("arquivo")
def test_sync_fora_e_dentro_de_loop(tmp_path):
    fora = sync.bruto.coletar(*ARQUIVO, uf="AL", destino=tmp_path / "a")

    async def dentro():
        return sync.bruto.coletar(*ARQUIVO, uf="AL", destino=tmp_path / "b")

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        no_loop = asyncio.run(dentro())

    assert type(fora) is type(no_loop) is bruto.ColetaBruta
    assert fora.entrada.sha256 == no_loop.entrada.sha256


async def test_modo_deterministico_nao_redireciona_o_coletor(arquivo, tmp_path):
    from agrobr import deterministic

    async with deterministic("2024-01-01"):
        resultado = await bruto.coletar(*ARQUIVO, uf="AL", destino=tmp_path)

    assert resultado.entrada.status == "ok" and len(arquivo.pedidos) == 1


async def test_plano_que_difere_do_pedido_e_recusado(habilitar, fonte, tmp_path):
    from agrobr.exceptions import ContractViolationError

    class Errado(AdaptadorArquivo):
        def planejar(self, pedido):
            return super().planejar(pedido).model_copy(update={"nome": "outro"})

    habilitar(ARQUIVO, Errado(fonte))

    with pytest.raises(ContractViolationError, match="nome"):
        await bruto.coletar(*ARQUIVO, uf="AL", destino=tmp_path)
    assert fonte.pedidos == [] and list(tmp_path.iterdir()) == []
