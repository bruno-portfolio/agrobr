from __future__ import annotations

from agrobr.http.settings import get_timeout
from agrobr.utils.geo import fetch_arcgis_layer

from .models import LAYERS, SFB_BASE

TIMEOUT = get_timeout(read=120.0)


async def fetch_layer(
    layer_key: str,
    *,
    where: str = "1=1",
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = None,
    f: str = "geojson",
) -> tuple[list[bytes], str]:
    return await fetch_arcgis_layer(
        SFB_BASE,
        LAYERS[layer_key],
        source="sfb",
        timeout=TIMEOUT,
        where=where,
        bbox=bbox,
        max_registros=max_registros,
        f=f,
        return_geometry=False if f == "json" else None,
    )
