from __future__ import annotations

import re
from pathlib import Path

import pytest

RAIZ = Path(__file__).resolve().parents[1]
RELATIVO = re.compile(r'(?:\]\(|src=")(?!https?:|#|mailto:)([^)"\s]+)')


@pytest.mark.parametrize("nome", ["README.md", "README.pt-BR.md"])
def test_readme_sem_link_nem_imagem_relativa(nome):
    """O README vai para a página do PyPI, onde caminho relativo não abre."""
    assert RELATIVO.findall((RAIZ / nome).read_text(encoding="utf-8")) == []
