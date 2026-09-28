"""agrobr - Dados agricolas brasileiros em uma linha de codigo."""

from __future__ import annotations

import structlog

__version__ = "2.0.0"
__author__ = "Bruno"

from agrobr import (
    abiove,
    acervo_fundiario,
    alt,
    ana,
    anda,
    anec,
    antaq,
    b3,
    bcb,
    cepea,
    cftc,
    comexstat,
    comtrade,
    conab,
    contracts,
    datasets,
    defensivos,
    deral,
    desmatamento,
    embrapa_solos,
    funai,
    ibama,
    ibge,
    icmbio,
    imea,
    incra,
    inmet,
    lista_suja,
    mapbiomas,
    mapbiomas_alerta,
    nasa_power,
    noticias_agricolas,
    queimadas,
    rio_verde,
    rnc,
    sfb,
    unica,
    usda,
    zarc,
)
from agrobr.datasets.deterministic import deterministic
from agrobr.exceptions import (
    AgrobrError,
    CacheMigrationError,
    ContractViolationError,
    InvalidParameterError,
    ParseError,
    ResourceLimitError,
    SnapshotError,
    SourceUnavailableError,
)
from agrobr.models import MetaInfo

__all__ = [
    "abiove",
    "acervo_fundiario",
    "alt",
    "ana",
    "anda",
    "anec",
    "antaq",
    "b3",
    "bcb",
    "cepea",
    "cftc",
    "comexstat",
    "comtrade",
    "conab",
    "contracts",
    "datasets",
    "defensivos",
    "deral",
    "desmatamento",
    "embrapa_solos",
    "deterministic",
    "funai",
    "ibama",
    "ibge",
    "icmbio",
    "imea",
    "incra",
    "inmet",
    "lista_suja",
    "mapbiomas",
    "mapbiomas_alerta",
    "nasa_power",
    "noticias_agricolas",
    "queimadas",
    "rio_verde",
    "rnc",
    "sfb",
    "unica",
    "usda",
    "zarc",
    "AgrobrError",
    "CacheMigrationError",
    "ContractViolationError",
    "InvalidParameterError",
    "ParseError",
    "ResourceLimitError",
    "SnapshotError",
    "SourceUnavailableError",
    "MetaInfo",
    "__version__",
]


def _configurar_logs() -> None:
    """Roteia o structlog pelo ``logging`` da stdlib, como biblioteca, se ninguém o configurou antes.

    Sem configuração do usuário, só warning e acima saem, na saída de erro (o padrão do ``logging``);
    ``logging.basicConfig`` e ``logging.getLogger("agrobr")`` controlam o resto. Sem cache do logger,
    para uma configuração posterior do usuário valer.
    """
    if structlog.is_configured():
        return
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=False,
    )


_configurar_logs()
