from __future__ import annotations

import math

import pytest
from pydantic import ValidationError

from agrobr import bruto
from agrobr.bruto import api
from agrobr.exceptions import ResourceLimitError
from tests.test_bruto.conftest import PAGINADO, manifesto


def test_padroes_do_contrato():
    assert bruto.LimitesBrutos().model_dump() == {
        "max_bytes_recurso": 4 * 1024**3,
        "max_bytes_pagina": 8 * 1024**2,
        "max_paginas": 10_000,
        "max_ids": 500_000,
        "max_bytes_ids": 64 * 1024**2,
        "max_segundos": 3600.0,
    }
    assert bruto.LimitesBrutos(max_segundos=60).max_segundos == 60.0


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_paginas": 0},
        {"max_paginas": True},
        {"max_bytes_pagina": 1.5},
        {"max_segundos": True},
        {"max_segundos": 0},
        {"max_segundos": math.inf},
        {"max_segundos": math.nan},
        {"desconhecido": 1},
    ],
)
def test_limite_invalido_e_recusado(kwargs):
    with pytest.raises(ValidationError):
        bruto.LimitesBrutos(**kwargs)


@pytest.mark.parametrize(
    ("limites", "mensagem"),
    [
        ({"max_paginas": 2}, "max_paginas"),
        ({"max_bytes_pagina": 300}, "teto de 300"),
        ({"max_bytes_recurso": 600}, "max_bytes_recurso"),
        ({"max_ids": 4}, "max_ids"),
        ({"max_bytes_ids": 100}, "max_bytes_ids"),
    ],
)
@pytest.mark.usefixtures("paginado")
async def test_orcamento_excedido_registra_erro_sem_truncar(tmp_path, limites, mensagem):
    with pytest.raises(ResourceLimitError, match=mensagem):
        await bruto.coletar(
            *PAGINADO, destino=tmp_path, tamanho_pagina=2, limites=bruto.LimitesBrutos(**limites)
        )

    [linha] = manifesto(tmp_path)
    assert (linha["status"], linha["erro"]["tipo"]) == ("erro", "ResourceLimitError")
    assert (
        linha["cobertura"]["estado"] == "nao_comprovada"
        and linha["opcoes"]["limites"]["max_paginas"]
    )


@pytest.mark.usefixtures("paginado")
async def test_prazo_da_chamada(tmp_path, monkeypatch):
    relogio = iter([0.0, 0.0] + [100.0] * 50)
    monkeypatch.setattr(api.time, "monotonic", lambda: next(relogio))
    monkeypatch.setattr("agrobr.bruto.transport.time.monotonic", lambda: next(relogio))

    with pytest.raises(ResourceLimitError, match="prazo"):
        await bruto.coletar(
            *PAGINADO, destino=tmp_path, limites=bruto.LimitesBrutos(max_segundos=10)
        )


async def test_memoria_dos_ids_e_contabilizada(paginado, tmp_path, monkeypatch):
    medidas = []
    original = api.Coleta.conferir_ids

    def medir(self, ids, *, papel):
        original(self, ids, papel=papel)
        medidas.append(self.memoria_ids)

    monkeypatch.setattr(api.Coleta, "conferir_ids", medir)
    paginado.ids = [f"id-{i:04d}" for i in range(50)]

    await bruto.coletar(*PAGINADO, destino=tmp_path, tamanho_pagina=10)

    assert len(medidas) == 5 and medidas == sorted(medidas) and medidas[0] < medidas[-1]
