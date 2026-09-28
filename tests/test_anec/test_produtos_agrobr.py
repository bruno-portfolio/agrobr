from __future__ import annotations

from pathlib import Path

import pytest

from agrobr.anec import models
from agrobr.normalize import crops

DOCS = Path(__file__).parents[2] / "docs"
PAGINAS = [DOCS / "sources" / f"anec{sufixo}" for sufixo in (".md", ".en.md")] + [
    DOCS / "contracts" / f"{nome}{sufixo}"
    for nome in (
        "embarques_anec",
        "embarques_mensais_anec",
        "comparacao_anual_anec",
        "destinos_anec",
    )
    for sufixo in (".md", ".en.md")
]


def _tabela(pagina: Path) -> dict[str, str | None]:
    linhas = pagina.read_text(encoding="utf-8").splitlines()
    inicio = next(
        i for i, linha in enumerate(linhas) if linha.startswith(("| Código ANEC", "| ANEC code"))
    )
    tabela: dict[str, str | None] = {}
    for linha in linhas[inicio + 2 :]:
        if not linha.startswith("|"):
            break
        codigo, _, nome = (celula.strip() for celula in linha.strip("|").split("|"))
        tabela[codigo.strip("`")] = nome.strip("`") if nome.startswith("`") else None
    return tabela


@pytest.mark.parametrize("pagina", PAGINAS, ids=lambda pagina: pagina.relative_to(DOCS).as_posix())
def test_tabela_de_produtos_da_anec_segue_o_normalizar_cultura(pagina):
    tabela = _tabela(pagina)

    assert tabela
    assert set(tabela) - {"total_products"} <= set(models.PRODUTO_ALIASES.values())
    for codigo, nome in tabela.items():
        if nome is None:
            assert not crops.is_cultura_valida(codigo), codigo
        else:
            assert crops.normalizar_cultura(codigo) == nome, codigo
