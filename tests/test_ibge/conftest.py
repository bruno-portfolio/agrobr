from __future__ import annotations

import pytest

from agrobr.ibge import client


@pytest.fixture(autouse=True)
def sem_periodos_ibge(monkeypatch: pytest.MonkeyPatch) -> None:
    """Os replays antigos do IBGE não trazem o `/periodos`; os testes dele sobrescrevem esta fixture."""

    async def sem_metadado(_table_code: str, _df: object) -> dict[str, object]:
        return {}

    monkeypatch.setattr(client, "_periodos_modificacao", sem_metadado)
