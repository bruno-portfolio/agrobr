from __future__ import annotations

from dataclasses import dataclass
from urllib import parse
from xml.sax.saxutils import escape

from agrobr import _log
from agrobr.http.settings import get_timeout
from agrobr.normalize.regions import UFS
from agrobr.utils.geo import fetch_wfs

from .models import (
    CRS_URN_4326,
    ESFERAS,
    GEOM_COLUMN,
    GRUPOS,
    LIMITE_UC,
    MAPFILE,
    MAPSERVER,
    NS_FES,
    NS_GML,
    PROPERTY_NAMES,
    TYPENAME,
    WFS_VERSION,
)

logger = _log.get_logger(__name__)

TIMEOUT = get_timeout(read=120.0)
_NOMES_UF = [str(info["nome"]).upper() for info in UFS.values()]


@dataclass(frozen=True)
class FiltroServidor:
    uf: str | None = None
    esfera: str | None = None
    categoria: str | None = None
    grupo: str | None = None
    bbox: tuple[float, float, float, float] | None = None


def _igual(campo: str, valor: str) -> str:
    return (
        f"<fes:PropertyIsEqualTo><fes:ValueReference>{campo}</fes:ValueReference>"
        f"<fes:Literal>{escape(valor)}</fes:Literal></fes:PropertyIsEqualTo>"
    )


def _like(campo: str, padrao: str) -> str:
    return (
        "<fes:PropertyIsLike wildCard='%' singleChar='_' escapeChar='!'>"
        f"<fes:ValueReference>{campo}</fes:ValueReference>"
        f"<fes:Literal>{escape(padrao)}</fes:Literal></fes:PropertyIsLike>"
    )


def _contem(campo: str, valor: str) -> str:
    return _like(campo, f"%{valor}%")


def _uf(nome: str) -> list[str]:
    return [_contem("uf", nome)] + [
        f"<fes:Or><fes:Not>{_contem('uf', outro)}</fes:Not>"
        f"{_contem('uf', f'{nome},')}{_like('uf', f'%{nome}')}</fes:Or>"
        for outro in _NOMES_UF
        if nome in outro and outro != nome
    ]


def _bbox(bbox: tuple[float, float, float, float]) -> str:
    minlon, minlat, maxlon, maxlat = (float(valor) for valor in bbox)
    return (
        f"<fes:BBOX><fes:ValueReference>{GEOM_COLUMN}</fes:ValueReference>"
        f"<gml:Envelope srsName='{CRS_URN_4326}'>"
        f"<gml:lowerCorner>{minlat!r} {minlon!r}</gml:lowerCorner>"
        f"<gml:upperCorner>{maxlat!r} {maxlon!r}</gml:upperCorner>"
        "</gml:Envelope></fes:BBOX>"
    )


def build_filter(filtro: FiltroServidor) -> str:
    condicoes = [_igual("limite", LIMITE_UC)]
    if filtro.uf is not None:
        condicoes.extend(_uf(str(UFS[filtro.uf]["nome"]).upper()))
    if filtro.esfera is not None:
        condicoes.append(_igual("esfera", ESFERAS[filtro.esfera]))
    if filtro.categoria is not None:
        condicoes.append(_igual("categoria", filtro.categoria))
    if filtro.grupo is not None:
        condicoes.append(_igual("grupo", GRUPOS[filtro.grupo]))
    if filtro.bbox is not None:
        condicoes.append(_bbox(filtro.bbox))
    corpo = condicoes[0] if len(condicoes) == 1 else f"<fes:And>{''.join(condicoes)}</fes:And>"
    return f"<fes:Filter xmlns:fes='{NS_FES}' xmlns:gml='{NS_GML}'>{corpo}</fes:Filter>"


def _url(params: dict[str, str]) -> str:
    base = {
        "MAP": MAPFILE,
        "SERVICE": "WFS",
        "VERSION": WFS_VERSION,
        "REQUEST": "GetFeature",
        "TYPENAMES": TYPENAME,
    }
    return f"{MAPSERVER}?{parse.urlencode(base | params, quote_via=parse.quote, safe='/:')}"


def count_url(filtro: FiltroServidor) -> str:
    return _url({"RESULTTYPE": "hits", "FILTER": build_filter(filtro)})


def features_url(filtro: FiltroServidor, *, geo: bool, count: int | None = None) -> str:
    propriedades = [GEOM_COLUMN, *PROPERTY_NAMES] if geo else PROPERTY_NAMES
    params = {
        "FILTER": build_filter(filtro),
        "PROPERTYNAME": ",".join(propriedades),
        "SORTBY": "cd_cnuc",
    }
    if count is not None:
        params["COUNT"] = str(count)
    if geo:
        params["SRSNAME"] = CRS_URN_4326
    return _url(params)


async def fetch_count(filtro: FiltroServidor) -> tuple[bytes, str]:
    url = count_url(filtro)
    content = await fetch_wfs(url, source="cnuc", timeout=TIMEOUT)
    return content, url


async def fetch_ucs(
    filtro: FiltroServidor, *, geo: bool, count: int | None = None
) -> tuple[bytes, str]:
    url = features_url(filtro, geo=geo, count=count)
    content = await fetch_wfs(url, source="cnuc", timeout=TIMEOUT)
    logger.info("cnuc_ucs_gml", source="cnuc", geo=geo, size=len(content))
    return content, url
