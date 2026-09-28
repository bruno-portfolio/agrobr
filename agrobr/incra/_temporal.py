from __future__ import annotations

from functools import lru_cache
from typing import Literal

from lxml import etree

from agrobr import constants


@lru_cache(maxsize=2)
def _schema(kind: Literal["date", "dateTime"]) -> etree.XMLSchema:
    namespace = constants.INCRA_TEMPORAL_XSD_NAMESPACE
    schema = etree.Element(f"{{{namespace}}}schema", nsmap={"xs": namespace})
    etree.SubElement(schema, f"{{{namespace}}}element", name="value", type=f"xs:{kind}")
    return etree.XMLSchema(etree.ElementTree(schema))


def _validate(value: str, kind: Literal["date", "dateTime"]) -> str:
    if type(value) is not str:
        raise ValueError("Valor temporal exige string JSON literal")
    if value != value.strip():
        raise ValueError("Whitespace externo em texto temporal não permitido pelo protocolo INCRA")
    node = etree.Element("value")
    node.text = value
    if not _schema(kind).validate(node):
        raise ValueError(f"Valor fora do domínio XSD {kind}")
    return value


def validate_date(value: str) -> str:
    return _validate(value, "date")


def validate_datetime(value: str) -> str:
    return _validate(value, "dateTime")
