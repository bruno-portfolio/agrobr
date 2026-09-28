from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timedelta
from typing import Any, NamedTuple

from pydantic import AwareDatetime, BaseModel, ConfigDict

from agrobr.models import MetaInfo


class Receipt(BaseModel):
    model_config = ConfigDict(frozen=True)

    request_url: str
    effective_url: str | None = None
    acquired_at: AwareDatetime
    status: int | None = None
    sha256: str | None = None
    size_bytes: int = 0
    error: str | None = None


class Bloco(NamedTuple):
    inicio: date
    fim: date
    fontes: tuple[str, ...]


class _BaixaLatencia(NamedTuple):
    nome: str
    substituta: str
    nome_substituta: str
    fecha_por_mes: bool
    recomendacao: str


BAIXA_LATENCIA = {
    "GEOSIT": _BaixaLatencia(
        "GEOS-IT", "MERRA2", "MERRA-2", True, "para tendência, a NASA recomenda parar 2 meses antes"
    ),
    "FLASHFLUX": _BaixaLatencia(
        "FLASHFlux",
        "SYN1DEG",
        "SYN1deg",
        False,
        "para tendência, a NASA não recomenda cruzar a troca de fonte",
    ),
}


class FetchResult(dict[str, Any]):
    def __init__(
        self,
        data: dict[str, Any],
        receipts: tuple[Receipt, ...] = (),
        blocks: tuple[Bloco, ...] = (),
    ) -> None:
        super().__init__(data)
        self.receipts = receipts
        self.blocks = blocks


def receipt_manifest(receipts: tuple[Receipt, ...]) -> bytes:
    return json.dumps(
        [item.model_dump(mode="json") for item in receipts],
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def apply_receipts(meta: MetaInfo, data: dict[str, Any], acquired_at: datetime) -> None:
    receipts = data.receipts if isinstance(data, FetchResult) else ()
    meta.fetched_at = receipts[-1].acquired_at if receipts else acquired_at
    meta.fetch_timestamp = meta.fetched_at
    meta.timestamp = datetime.now(meta.fetched_at.tzinfo)
    meta.source_details["http_receipts"] = [item.model_dump(mode="json") for item in receipts]
    if receipts:
        used = [item for item in receipts if item.status is not None and 200 <= item.status < 300]
        if used:
            meta.source_details["logical_source_url"] = meta.source_url
            meta.source_url = used[0].effective_url or used[0].request_url
            meta.source_details["source_url_scope"] = (
                "first_response_block" if len(used) > 1 else "response"
            )
            meta.source_details["response_blocks"] = len(used)
        meta.raw_content_size = sum(item.size_bytes for item in receipts)
        meta.raw_content_hash = hashlib.sha256(receipt_manifest(receipts)).hexdigest()
        meta.source_details["raw_hash_method"] = "sha256_canonical_receipt_manifest_utf8"
        meta.source_details["raw_size_method"] = "sum_response_body_bytes_all_attempts"


def apply_sources(meta: MetaInfo, data: dict[str, Any]) -> None:
    """Publica as fontes que cada cabeçalho declara e avisa o trecho de baixa latência.

    Num bloco com a fonte de baixa latência e a definitiva, o cabeçalho não diz o dia da troca.
    O MERRA-2 fecha por mês ("Month Behind Near Real Time"), então o GEOS-IT começa num dia 1:
    com um só dia 1 depois do início do bloco, o trecho é exato; com mais de um, fica o bloco.
    """
    blocos = data.blocks if isinstance(data, FetchResult) else ()
    meta.source_details["fontes"] = sorted({fonte for bloco in blocos for fonte in bloco.fontes})
    meta.source_details["fontes_por_bloco"] = [
        {
            "inicio": bloco.inicio.isoformat(),
            "fim": bloco.fim.isoformat(),
            "fontes": list(bloco.fontes),
        }
        for bloco in blocos
    ]
    periodos = []
    for codigo, fonte in BAIXA_LATENCIA.items():
        trechos = [_trecho(fonte, bloco) for bloco in blocos if codigo in bloco.fontes]
        if not trechos:
            continue
        inicio = min(trecho[0] for trecho in trechos)
        fim = max(trecho[1] for trecho in trechos)
        exato = all(trecho[2] for trecho in trechos)
        regra = any(trecho[3] for trecho in trechos)
        periodos.append(
            {
                "fonte": codigo,
                "inicio": inicio.isoformat(),
                "fim": fim.isoformat(),
                "origem": "cabecalho+regra_mensal_nasa" if regra else "cabecalho",
                "exato": exato,
            }
        )
        dias = f"dias de {inicio:%d/%m/%Y} a {fim:%d/%m/%Y}"
        meta.validation_warnings.append(
            f"{dias} vêm do {fonte.nome} (baixa latência), que a NASA substitui pelo "
            f"{fonte.nome_substituta} depois; {fonte.recomendacao}"
            if exato
            else f"{dias} vêm do {fonte.nome} ou do {fonte.nome_substituta} (o cabeçalho da NASA "
            f"não separa por dia); o {fonte.nome} é de baixa latência, e a NASA o substitui pelo "
            f"{fonte.nome_substituta} depois; {fonte.recomendacao}"
        )
    meta.source_details["periodos_baixa_latencia"] = periodos


def _trecho(fonte: _BaixaLatencia, bloco: Bloco) -> tuple[date, date, bool, bool]:
    if fonte.substituta not in bloco.fontes:
        return bloco.inicio, bloco.fim, True, False
    dias_1 = [
        bloco.inicio + timedelta(days=passo)
        for passo in range(1, (bloco.fim - bloco.inicio).days + 1)
        if (bloco.inicio + timedelta(days=passo)).day == 1
    ]
    if fonte.fecha_por_mes and len(dias_1) == 1:
        return dias_1[0], bloco.fim, True, True
    return bloco.inicio, bloco.fim, False, False
