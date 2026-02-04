"""Tests for CEPEA API."""

from __future__ import annotations

import pytest

from agrobr import cepea


@pytest.mark.asyncio
async def test_produtos_returns_list():
    """Test that produtos returns a list of available products."""
    result = await cepea.produtos()
    assert isinstance(result, list)
    assert "soja" in result
    assert "milho" in result
    assert "cafe" in result


@pytest.mark.asyncio
async def test_pracas_returns_list():
    """Test that pracas returns a list for known products."""
    result = await cepea.pracas("soja")
    assert isinstance(result, list)
    assert len(result) > 0


@pytest.mark.asyncio
async def test_pracas_unknown_product():
    """Test that pracas returns empty list for unknown products."""
    result = await cepea.pracas("unknown_product")
    assert isinstance(result, list)
    assert len(result) == 0
