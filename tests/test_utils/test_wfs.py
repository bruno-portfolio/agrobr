from __future__ import annotations

from types import SimpleNamespace

import pytest
from pydantic import BaseModel, Field

from agrobr.utils.wfs import PageState


class Properties(BaseModel):
    label: str | None = Field(alias="publishedLabel")


@pytest.mark.parametrize("name", ["label", "publishedLabel"])
@pytest.mark.parametrize(
    "value,key", [(None, "null_count"), ("", "empty_count"), (" ", "whitespace_count")]
)
def test_estatistica_de_overlap_reconhece_nome_e_alias(name, value, key):
    state = PageState(lambda: 10)
    state.accepted = 1
    counts = {"null_count": 0, "empty_count": 0, "whitespace_count": 0}
    counts[key] = 1
    parsed = SimpleNamespace(
        records=[SimpleNamespace(properties=Properties(publishedLabel=value))],
        diagnostics={},
        statistics={name: counts},
        warnings=[],
    )
    state._diagnostics(parsed, 0)
    assert state.statistics[name][key] == 1
    assert state.accepted_statistics[name][key] == 0


def test_estatistica_de_campo_desconhecido_falha_sem_deducao_silenciosa():
    state = PageState(lambda: 10)
    state.accepted = 1
    parsed = SimpleNamespace(
        records=[SimpleNamespace(properties=Properties(publishedLabel=None))],
        diagnostics={},
        statistics={"missing": {"null_count": 1}},
        warnings=[],
    )
    with pytest.raises(ValueError, match="Campo estatístico ausente"):
        state._diagnostics(parsed, 0)
