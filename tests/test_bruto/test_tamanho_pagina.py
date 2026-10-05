from __future__ import annotations

from typing import Any

import pytest

from agrobr import bruto, constants
from agrobr.bruto import registry, validation
from agrobr.exceptions import InvalidParameterError


def _pedido(chave: tuple[str, str], tamanho_pagina: Any) -> Any:
    return validation.pedido(
        registry.RECURSOS[chave],
        nome=None,
        uf=None,
        bbox=None,
        bbox_crs="EPSG:4674",
        tamanho_pagina=tamanho_pagina,
        compactar=True,
        retomar=False,
        limites=None,
    )


@pytest.mark.parametrize(
    "chave,esperado",
    [
        (("funai", "terras_indigenas"), 20),
        (("funai", "terras_indigenas_pontos"), constants.BRUTO_TAMANHO_PAGINA_PADRAO),
        (("ibge", "malha_municipal"), constants.BRUTO_TAMANHO_PAGINA_PADRAO),
    ],
)
def test_tamanho_pagina_none_usa_o_padrao_do_registro(chave, esperado):
    assert _pedido(chave, None).tamanho_pagina == esperado


@pytest.mark.parametrize("tamanho", [1, 7, constants.BRUTO_TAMANHO_PAGINA_MAX])
def test_tamanho_pagina_explicito_vence_o_padrao_do_registro(tamanho):
    assert _pedido(("funai", "terras_indigenas"), tamanho).tamanho_pagina == tamanho


@pytest.mark.parametrize("tamanho", [0, constants.BRUTO_TAMANHO_PAGINA_MAX + 1, True, 2.0, "20"])
def test_tamanho_pagina_fora_dos_limites_recusado_com_padrao_proprio(tamanho):
    with pytest.raises(InvalidParameterError, match="inteiro de 1 a"):
        _pedido(("funai", "terras_indigenas"), tamanho)


def test_pagina_unica_resolve_none_para_o_maximo_do_contrato():
    assert _pedido(("incra", "quilombolas"), None).tamanho_pagina == (
        constants.BRUTO_TAMANHO_PAGINA_MAX
    )


@pytest.mark.parametrize("tamanho", [100, constants.BRUTO_TAMANHO_PAGINA_MAX, 0])
async def test_pagina_unica_recusa_tamanho_pagina_explicito_antes_da_rede(tmp_path, tamanho):
    with pytest.raises(InvalidParameterError, match="página única"):
        await bruto.coletar("incra", "quilombolas", destino=tmp_path, tamanho_pagina=tamanho)

    assert list(tmp_path.iterdir()) == []


def test_so_os_recursos_previstos_mudam_o_padrao_ou_pagina_unica():
    proprios = {
        chave: (r.tamanho_pagina_padrao, r.pagina_unica)
        for chave, r in registry.RECURSOS.items()
        if (r.tamanho_pagina_padrao, r.pagina_unica)
        != (constants.BRUTO_TAMANHO_PAGINA_PADRAO, False)
    }

    assert proprios == {
        ("funai", "terras_indigenas"): (20, False),
        ("incra", "quilombolas"): (100, True),
    }
