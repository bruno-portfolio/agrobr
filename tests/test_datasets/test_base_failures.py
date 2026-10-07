from __future__ import annotations

from unittest import mock

import httpx
import pandas as pd
import pytest

from agrobr import exceptions
from agrobr.datasets import base
from agrobr.datasets.preco_diario import PrecoDiarioDataset


def _dataset(*outcomes):
    dataset = PrecoDiarioDataset()
    sources = [
        base.DatasetSource(
            name=f"source_{index}",
            priority=index,
            fetch_fn=mock.AsyncMock(side_effect=outcome)
            if isinstance(outcome, Exception)
            else mock.AsyncMock(return_value=(outcome, None)),
        )
        for index, outcome in enumerate(outcomes)
    ]
    dataset.info = base.DatasetInfo("preco_diario", "", sources=sources, products=["soja"])
    return dataset


async def test_falhas_de_layout_agregam_fontes_erros_e_causa():
    errors = [exceptions.ParseError(f"source_{i}", i + 1, "layout alterado") for i in range(2)]
    dataset = _dataset(*errors)
    with pytest.raises(exceptions.ParseError) as caught:
        await dataset._try_sources("soja")
    error = caught.value
    assert error.source == "preco_diario/soja"
    assert error.attempted_sources == ["source_0", "source_1"]
    assert error.errors == [(f"source_{i}", "parse", str(e)) for i, e in enumerate(errors)]
    assert error.__cause__ is errors[-1]
    assert error.parser_version == 2


@pytest.mark.parametrize(
    "last_error,category",
    [
        (httpx.ConnectError("sem conexão"), "network"),
        (exceptions.ContractViolationError("preco_diario", "coluna ausente"), "contract"),
        (exceptions.SourceUnavailableError("source_1", last_error="fora do ar"), "unavailable"),
    ],
)
async def test_falhas_mistas_preservam_indisponibilidade(last_error, category):
    parse_error = exceptions.ParseError("source_0", 1, "layout alterado")
    dataset = _dataset(parse_error, last_error)
    with pytest.raises(exceptions.SourceUnavailableError) as caught:
        await dataset._try_sources("soja")
    assert caught.value.attempted_sources == ["source_0", "source_1"]
    assert caught.value.errors == [
        ("source_0", "parse", str(parse_error)),
        ("source_1", category, str(last_error)),
    ]
    assert caught.value.__cause__ is last_error


@pytest.mark.parametrize(
    "first_error",
    [httpx.ConnectError("sem conexão"), exceptions.ParseError("source_0", 1, "layout alterado")],
)
async def test_falha_de_rede_ou_layout_permite_fallback(first_error):
    frame = pd.DataFrame({"valor": [145.0]})
    dataset = _dataset(first_error, frame)
    with pytest.warns(exceptions.SourceFallbackWarning):
        result, selected, meta, attempted = await dataset._try_sources("soja")
    assert result is frame
    assert selected == "source_1"
    assert attempted == ["source_0", "source_1"]
    assert meta is None


@pytest.mark.parametrize("error_type", [RuntimeError, KeyError, AttributeError, ValueError])
async def test_bug_propagado_sem_tentar_fallback(error_type):
    error = error_type("falha de programação")
    dataset = _dataset(error, pd.DataFrame())
    with pytest.raises(error_type) as caught:
        await dataset._try_sources("soja")
    assert caught.value is error
    dataset.info.sources[1].fetch_fn.assert_not_awaited()


async def test_aviso_de_fallback_cita_motivo_de_cada_fonte_na_ordem():
    errors = [
        exceptions.SourceUnavailableError("source_0", last_error="token ausente"),
        exceptions.ParseError("source_1", 1, "layout do historico alterado"),
        httpx.ConnectError("sem conexao com a terceira fonte"),
        exceptions.ContractViolationError("preco_diario", "coluna valor ausente"),
    ]
    categories = ["unavailable", "parse", "network", "contract"]
    frame = pd.DataFrame({"valor": [145.0]})
    dataset = _dataset(*errors, frame)

    with pytest.warns(exceptions.SourceFallbackWarning) as caught:
        result, selected, meta, attempted = await dataset._try_sources("soja")

    assert len(caught) == 1
    message = str(caught[0].message)
    reasons = [
        f"source_{index}: {category}: {str(error)[:120]}"
        for index, (category, error) in enumerate(zip(categories, errors))
    ]
    assert "; ".join(reasons) in message
    assert "usando fallback 'source_4'" in message
    assert result is frame and selected == "source_4" and meta is None
    assert attempted == [f"source_{index}" for index in range(5)]
