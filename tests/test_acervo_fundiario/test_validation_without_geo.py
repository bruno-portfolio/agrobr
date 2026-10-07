from __future__ import annotations

from unittest.mock import AsyncMock, Mock

import pytest

from agrobr.acervo_fundiario import api


@pytest.fixture
def missing_geo(monkeypatch: pytest.MonkeyPatch) -> tuple[Mock, AsyncMock]:
    check = Mock(side_effect=ImportError("Install with: pip install agrobr[geo]"))
    download = AsyncMock()
    monkeypatch.setattr(api, "check_pyogrio", check)
    monkeypatch.setattr(api.client, "download_and_cache", download)
    return check, download


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "api_name", ["sigef", "sigef_geo", "snci", "snci_geo", "assentamentos", "assentamentos_geo"]
)
@pytest.mark.parametrize("invalid", ["uf", "bbox"])
async def test_invalid_parameter_precedes_missing_geo(
    api_name: str, invalid: str, missing_geo: tuple[Mock, AsyncMock]
):
    check, download = missing_geo
    uf = "GO" if api_name.startswith("snci") else "ES"
    if invalid == "uf":
        uf = "XX"
    bbox = (10.0, 0.0, -10.0, 1.0) if invalid == "bbox" else None

    try:
        await getattr(api, api_name)(uf=uf, bbox=bbox)
    except Exception as exc:
        caught = exc
    else:
        caught = None
    assert isinstance(caught, ValueError), caught
    check.assert_not_called()
    download.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "api_name", ["sigef", "sigef_geo", "snci", "snci_geo", "assentamentos", "assentamentos_geo"]
)
async def test_valid_parameters_require_geo_before_download(
    api_name: str, missing_geo: tuple[Mock, AsyncMock]
):
    check, download = missing_geo
    uf = "GO" if api_name.startswith("snci") else "ES"

    try:
        await getattr(api, api_name)(uf=uf, bbox=(-50.0, -25.0, -35.0, -10.0))
    except Exception as exc:
        caught = exc
    else:
        caught = None
    assert isinstance(caught, ImportError) and "pip install agrobr[geo]" in str(caught), caught
    check.assert_called_once_with()
    download.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("api_name", "bbox"),
    [
        ("sigef", None),
        ("sigef", (-48.25, -15.9, -48.1, -15.6)),
        ("sigef_geo", None),
        ("snci", None),
        ("snci", (-61.0, 2.0, -60.0, 3.0)),
        ("snci_geo", None),
        ("assentamentos", None),
        ("assentamentos", (-50.0, -25.0, -35.0, -10.0)),
        ("assentamentos_geo", None),
    ],
)
async def test_geopandas_conferido_antes_do_download_quando_a_leitura_usa(
    monkeypatch: pytest.MonkeyPatch, api_name: str, bbox: tuple[float, ...] | None
):
    sem_geopandas = Mock(side_effect=ImportError("Install with: pip install agrobr[geo]"))
    download = AsyncMock(side_effect=RuntimeError("download"))
    monkeypatch.setattr(api, "check_pyogrio", Mock())
    monkeypatch.setattr(api, "check_geopandas", sem_geopandas)
    monkeypatch.setattr(api.client, "download_and_cache", download)
    uf = "RR" if api_name.startswith("snci") else "DF"

    with pytest.raises(ImportError, match=r"pip install agrobr\[geo\]"):
        await getattr(api, api_name)(uf=uf, bbox=bbox)

    sem_geopandas.assert_called_once_with()
    download.assert_not_awaited()
