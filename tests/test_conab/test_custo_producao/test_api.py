import pytest

from agrobr.conab.custo_producao import api
from agrobr.exceptions import InvalidParameterError


@pytest.mark.asyncio
@pytest.mark.parametrize("name", ["custo_producao", "custo_producao_total"])
@pytest.mark.parametrize("selectors", [{"uf": "XX"}, {"ano": True}, {"local": ""}])
async def test_public_invalid_selector_before_acquisition(monkeypatch, name, selectors):
    async def forbidden(*_args, **_kwargs):
        raise AssertionError("Acquisition reached")

    monkeypatch.setattr(api.Acquisition, "get", forbidden)
    with pytest.raises(InvalidParameterError):
        await getattr(api, name)("soja", **selectors)
