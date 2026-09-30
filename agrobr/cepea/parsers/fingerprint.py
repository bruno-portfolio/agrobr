from __future__ import annotations

import hashlib

from bs4 import BeautifulSoup

from agrobr import _log
from agrobr.constants import Fonte
from agrobr.models import Fingerprint
from agrobr.utils.time import utcnow
from agrobr.validators.structural import compare_fingerprints as compare_fingerprints

logger = _log.get_logger(__name__)


def extract_fingerprint(
    html: str,
    source: Fonte,
    url: str,
) -> Fingerprint:
    soup = BeautifulSoup(html, "lxml")

    table_classes: list[list[str]] = []
    for table in soup.find_all("table")[:10]:
        classes_raw = table.get("class")
        if classes_raw is None:
            classes: list[str] = []
        elif isinstance(classes_raw, str):
            classes = [classes_raw]
        else:
            classes = list(classes_raw)
        table_classes.append(sorted(classes))

    keywords = ["preco", "indicador", "cotacao", "valor", "tabela", "dados"]
    key_ids: list[str] = []
    for elem in soup.find_all(id=True):
        elem_id_raw = elem.get("id")
        if elem_id_raw is None or not isinstance(elem_id_raw, str):
            continue
        elem_id = elem_id_raw.lower()
        if any(kw in elem_id for kw in keywords):
            key_ids.append(elem_id_raw)
    key_ids = sorted(set(key_ids))[:20]

    table_headers: list[list[str]] = []
    for table in soup.find_all("table")[:5]:
        headers: list[str] = []
        for th in table.find_all("th"):
            text = th.get_text(strip=True)[:50]
            if text:
                headers.append(text)
        if headers:
            table_headers.append(headers)

    element_counts = {
        "tables": len(soup.find_all("table")),
        "forms": len(soup.find_all("form")),
        "divs_with_id": len(soup.find_all("div", id=True)),
        "inputs": len(soup.find_all("input")),
        "selects": len(soup.find_all("select")),
        "links": len(soup.find_all("a")),
        "scripts": len(soup.find_all("script")),
    }

    structure_elements: list[tuple[str, int, tuple[str, ...]]] = []
    for tag in soup.find_all(["table", "div", "form", "section", "article"])[:30]:
        tag_classes_raw = tag.get("class")
        if tag_classes_raw is None:
            tag_classes: list[str] = []
        elif isinstance(tag_classes_raw, str):
            tag_classes = [tag_classes_raw]
        else:
            tag_classes = list(tag_classes_raw)
        structure_elements.append(
            (
                tag.name or "",
                len(tag.find_all(recursive=False)),
                tuple(sorted(tag_classes))[:3] if tag_classes else (),
            )
        )

    structure_hash = hashlib.md5(str(structure_elements).encode()).hexdigest()[:12]

    return Fingerprint(
        source=source,
        url=url,
        collected_at=utcnow(),
        table_classes=table_classes,
        key_ids=key_ids,
        structure_hash=structure_hash,
        table_headers=table_headers,
        element_counts=element_counts,
    )
