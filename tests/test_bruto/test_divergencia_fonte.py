from __future__ import annotations

import pytest

from agrobr import bruto, exceptions
from agrobr.bruto import models
from tests.test_bruto.conftest import AdaptadorPaginas, manifesto


async def test_divergencia_informada_pela_fonte_registra_cobertura_divergente(
    habilitar, fonte, tmp_path
):
    mensagem = "IDs da página diferem da faixa solicitada"

    class Divergente(AdaptadorPaginas):
        async def adquirir(self, _plano, *, contexto):
            raise contexto.divergencia(mensagem)

    habilitar(("ibge", "malha_municipal"), Divergente(fonte))

    with pytest.raises(exceptions.ParseError, match=mensagem):
        await bruto.coletar("ibge", "malha_municipal", destino=tmp_path)

    [entrada] = manifesto(tmp_path)
    assert entrada["status"] == "erro"
    assert entrada["erro"]["tipo"] == "ParseError"
    assert mensagem in entrada["erro"]["mensagem"]
    assert entrada["cobertura"]["estado"] == "divergente"
    assert not entrada["cobertura"]["completa"]
    models.RecursoBruto.model_validate(entrada)
