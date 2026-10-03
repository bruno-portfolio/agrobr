from __future__ import annotations

import gzip

import pytest

from agrobr import bruto
from agrobr.exceptions import ContractViolationError, InvalidParameterError, ResourceLimitError
from tests.test_bruto.conftest import ARQUIVO, PAGINADO, manifesto


async def test_ok_e_reutilizado_sem_rede_depois_de_conferir_todos_os_artefatos(paginado, tmp_path):
    primeira = await bruto.coletar(*PAGINADO, destino=tmp_path, tamanho_pagina=2)
    antes = (tmp_path / "manifesto.jsonl").read_bytes()
    pedidos = len(paginado.pedidos)

    segunda = await bruto.coletar(*PAGINADO, destino=tmp_path, tamanho_pagina=2, retomar=True)

    assert segunda.reutilizado and segunda.entrada == primeira.entrada
    assert len(paginado.pedidos) == pedidos
    assert (tmp_path / "manifesto.jsonl").read_bytes() == antes


@pytest.mark.usefixtures("paginado")
async def test_limites_diferentes_nao_mudam_a_identidade(tmp_path):
    await bruto.coletar(*PAGINADO, destino=tmp_path)
    limites = bruto.LimitesBrutos(max_paginas=50)

    segunda = await bruto.coletar(*PAGINADO, destino=tmp_path, retomar=True, limites=limites)

    assert segunda.reutilizado


@pytest.mark.parametrize("estrago", ["byte", "falta", "gzip"])
async def test_artefato_corrompido_levanta_contractviolation_e_preserva_o_manifesto(
    paginado, tmp_path, estrago
):
    entrada = (await bruto.coletar(*PAGINADO, destino=tmp_path, tamanho_pagina=2)).entrada
    alvo = tmp_path / entrada.controles[-1].arquivo
    antes = (tmp_path / "manifesto.jsonl").read_bytes()
    if estrago == "falta":
        alvo.unlink()
    elif estrago == "gzip":
        alvo.write_bytes(alvo.read_bytes()[:-9] + b"\x00" * 9)
    else:
        corpo = bytearray(gzip.decompress(alvo.read_bytes()))
        corpo[0] ^= 1
        alvo.write_bytes(gzip.compress(bytes(corpo), mtime=0))
    pedidos = len(paginado.pedidos)

    with pytest.raises(ContractViolationError):
        await bruto.coletar(*PAGINADO, destino=tmp_path, tamanho_pagina=2, retomar=True)

    assert len(paginado.pedidos) == pedidos
    assert (tmp_path / "manifesto.jsonl").read_bytes() == antes


@pytest.mark.usefixtures("arquivo")
async def test_limite_menor_que_a_verificacao_levanta_resourcelimit(tmp_path):
    await bruto.coletar(*ARQUIVO, uf="AL", destino=tmp_path)

    with pytest.raises(ResourceLimitError, match="max_bytes_recurso"):
        await bruto.coletar(
            *ARQUIVO,
            uf="AL",
            destino=tmp_path,
            retomar=True,
            limites=bruto.LimitesBrutos(max_bytes_recurso=100),
        )


async def test_erro_recomeca_o_recurso_inteiro_em_outra_coleta(paginado, tmp_path):
    paginado.total_depois = 9
    with pytest.raises(Exception, match="contagem antes 5, depois 9"):
        await bruto.coletar(*PAGINADO, destino=tmp_path, tamanho_pagina=2)
    [erro] = manifesto(tmp_path)
    paginado.total_depois = None
    paginado.contagens = 0

    retomada = await bruto.coletar(*PAGINADO, destino=tmp_path, tamanho_pagina=2, retomar=True)

    assert not retomada.reutilizado and retomada.entrada.status == "ok"
    assert retomada.entrada.coleta_id != erro["coleta_id"]
    assert [e["coleta_id"] for e in manifesto(tmp_path)] == [retomada.entrada.coleta_id]
    assert (tmp_path / erro["paginas"][0]["arquivo"]).exists()


async def test_ausente_na_fonte_volta_a_ser_tentado(arquivo, tmp_path):
    arquivo.status_arquivo = 404
    assert (
        await bruto.coletar(*ARQUIVO, uf="AL", destino=tmp_path)
    ).entrada.status == "ausente_na_fonte"
    arquivo.status_arquivo = 200

    segunda = await bruto.coletar(*ARQUIVO, uf="AL", destino=tmp_path, retomar=True)

    assert segunda.entrada.status == "ok" and len(arquivo.pedidos) == 2


@pytest.mark.parametrize(
    ("kwargs", "mensagem"),
    [
        ({}, "use retomar=True"),
        ({"retomar": True, "tamanho_pagina": 3}, "outra consulta"),
        ({"retomar": True, "compactar": False}, "outra consulta"),
        ({"retomar": True, "nome": "Brasil"}, "colide"),
    ],
)
async def test_chave_existente_recusada_antes_da_rede(paginado, tmp_path, kwargs, mensagem):
    await bruto.coletar(*PAGINADO, destino=tmp_path)
    antes = (tmp_path / "manifesto.jsonl").read_bytes()
    pedidos = len(paginado.pedidos)

    with pytest.raises(InvalidParameterError, match=mensagem):
        await bruto.coletar(*PAGINADO, destino=tmp_path, **kwargs)

    assert len(paginado.pedidos) == pedidos
    assert (tmp_path / "manifesto.jsonl").read_bytes() == antes
