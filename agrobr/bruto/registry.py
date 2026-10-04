from __future__ import annotations

import importlib
from dataclasses import dataclass
from typing import Literal, cast

from agrobr.bruto import models, protocols
from agrobr.exceptions import InvalidParameterError, UnknownNameError


@dataclass(frozen=True)
class RecursoRegistrado:
    """Regra de seleção de um recurso; o adaptador é o ``atributo`` de ``modulo``, importado só ao coletar (sem ciclo com a fonte)."""

    fonte: str
    recurso: str
    modulo: str
    modo: models.Modo
    formato: models.Formato
    campo_id: str | None
    uf: Literal["obrigatoria", "opcional", "recusada"]
    bbox: Literal["opcional", "recusada"]
    exige_recorte: bool
    habilitado: bool
    atributo: str = "adaptador"


def _acervo(recurso: str) -> RecursoRegistrado:
    return RecursoRegistrado(
        "acervo_fundiario",
        recurso,
        "agrobr.acervo_fundiario.bruto",
        "arquivo",
        "zip",
        None,
        "obrigatoria",
        "recusada",
        False,
        True,
    )


def _arquivo_nacional(
    fonte: str, recurso: str, formato: models.FormatoArquivo, atributo: str
) -> RecursoRegistrado:
    return RecursoRegistrado(
        fonte,
        recurso,
        f"agrobr.{fonte}.bruto",
        "arquivo",
        formato,
        None,
        "recusada",
        "recusada",
        False,
        True,
        atributo,
    )


RECURSOS: dict[tuple[str, str], RecursoRegistrado] = {
    (r.fonte, r.recurso): r
    for r in (
        RecursoRegistrado(
            "ana",
            "massas_dagua",
            "agrobr.ana.bruto",
            "paginado",
            "esri_json",
            "FID",
            "opcional",
            "opcional",
            True,
            True,
        ),
        RecursoRegistrado(
            "cnuc",
            "ucs",
            "agrobr.cnuc.bruto",
            "paginado",
            "gml",
            "cd_cnuc",
            "opcional",
            "opcional",
            False,
            True,
        ),
        RecursoRegistrado(
            "ibge",
            "malha_municipal",
            "agrobr.ibge.bruto",
            "paginado",
            "geojson",
            "cd_mun",
            "opcional",
            "opcional",
            False,
            True,
        ),
        RecursoRegistrado(
            "ibge",
            "areas_urbanizadas",
            "agrobr.ibge.bruto",
            "paginado",
            "geojson",
            "fid",
            "recusada",
            "opcional",
            False,
            True,
        ),
        _acervo("sigef_publico"),
        _acervo("sigef_privado"),
        _acervo("snci_publico"),
        _acervo("snci_privado"),
        _acervo("snci_brasil"),
        _arquivo_nacional("cnuc", "cadastro", "csv", "cadastro"),
        _arquivo_nacional("ibama", "termos_embargo", "csv", "adaptador"),
        _arquivo_nacional("ibge", "malha_municipal_zip", "zip", "malha_municipal_zip"),
        _arquivo_nacional("ibge", "areas_urbanizadas_zip", "zip", "areas_urbanizadas_zip"),
        RecursoRegistrado(
            "sicar",
            "imoveis",
            "agrobr.alt.sicar.bruto",
            "paginado",
            "geojson",
            "feature.id",
            "obrigatoria",
            "opcional",
            False,
            True,
        ),
    )
}


def recurso(fonte: object, nome_recurso: object) -> RecursoRegistrado:
    fontes = sorted({f for f, _ in RECURSOS})
    if not isinstance(fonte, str) or fonte not in fontes:
        raise UnknownNameError(f"bruto: fonte desconhecida {fonte!r}. Fontes: {', '.join(fontes)}")
    recursos = sorted(r for f, r in RECURSOS if f == fonte)
    if not isinstance(nome_recurso, str) or nome_recurso not in recursos:
        raise UnknownNameError(
            f"bruto: recurso desconhecido {nome_recurso!r} para {fonte}. Recursos: {', '.join(recursos)}"
        )
    registrado = RECURSOS[fonte, nome_recurso]
    if not registrado.habilitado:
        raise InvalidParameterError(
            f"bruto: {fonte}/{nome_recurso} não está disponível nesta versão do agrobr"
        )
    return registrado


def adaptador(registrado: RecursoRegistrado) -> protocols.AdaptadorBruto:
    modulo = importlib.import_module(registrado.modulo)
    return cast("protocols.AdaptadorBruto", getattr(modulo, registrado.atributo))
