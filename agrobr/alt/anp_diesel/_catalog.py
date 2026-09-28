from __future__ import annotations

import re
from urllib.parse import urljoin, urlsplit

import pydantic
from bs4 import BeautifulSoup

from agrobr import constants
from agrobr.exceptions import ParseError

from . import models


def parse_municipal_catalog(html: str) -> dict[str, str]:
    base = constants.URLS[constants.Fonte.ANP_DIESEL]["precos_catalogo"]
    resources: dict[str, str] = {}
    years: dict[int, str] = {}
    for link in BeautifulSoup(html, "lxml").find_all("a", href=True):
        url = urljoin(base, str(link["href"]))
        match = re.fullmatch(
            r"semanal-municipios?-(20\d{2})(?:[-_]a?[-_]?(20\d{2}))?\.xlsx",
            urlsplit(url).path.rsplit("/", 1)[-1],
            re.I,
        )
        if match is None or int(match[1]) < 2022:
            continue
        try:
            resource = models.MunicipalWorkbook(
                inicio=int(match[1]),
                fim=int(match[2] or match[1]),
                url=url,
            )
        except pydantic.ValidationError as exc:
            raise ParseError(
                source="anp_diesel",
                parser_version=2,
                reason="Planilha municipal inválida no catálogo",
            ) from exc
        for year in range(resource.inicio, resource.fim + 1):
            if year in years and years[year] != url:
                raise ParseError(
                    source="anp_diesel",
                    parser_version=2,
                    reason=f"Planilhas municipais sobrepostas para {year}",
                )
            years[year] = url
        resources[resource.periodo] = url
    if not resources:
        raise ParseError(
            source="anp_diesel",
            parser_version=2,
            reason="Planilhas municipais ausentes no catálogo semanal",
        )
    return dict(sorted(resources.items()))
