from __future__ import annotations

import logging

import structlog

PROCESSADORES: list[structlog.typing.Processor] = [
    structlog.stdlib.filter_by_level,
    structlog.stdlib.add_logger_name,
    structlog.stdlib.add_log_level,
    structlog.processors.TimeStamper(fmt="iso"),
    structlog.processors.JSONRenderer(),
]


def get_logger(nome: str) -> structlog.stdlib.BoundLogger:
    """Logger do agrobr sobre o ``logging`` da stdlib, sem depender da configuração global do structlog.

    A lista ``PROCESSADORES`` é compartilhada por referência com todo logger criado aqui: trocá-la no lugar
    vale para todos, como o ``structlog.testing.capture_logs`` faz com a lista global.
    """
    logger: structlog.stdlib.BoundLogger = structlog.wrap_logger(
        logging.getLogger(nome),
        processors=PROCESSADORES,
        wrapper_class=structlog.stdlib.BoundLogger,
    )
    return logger
