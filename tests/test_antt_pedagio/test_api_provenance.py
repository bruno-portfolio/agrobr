from __future__ import annotations

import hashlib
import json
from datetime import datetime

from agrobr.alt.antt_pedagio import api
from agrobr.models import MetaInfo
from tests.helpers import install_anttpedagio_source

TRAFEGO = (
    b"concessionaria;praca;mes_ano;categoria;tipo_de_veiculo;tipo_cobranca;sentido;volume_total\n"
    b"Concessionaria;P1;01/01/2023;Categoria 4;Comercial;N/I;Crescente;423,00\n"
)
PRACAS = (
    b"concessionaria;praca_de_pedagio;rodovia;uf;municipal;latitude;longitude\n"
    b"Concessionaria;P1;BR-163;MT;Municipio;-10;-55\n"
)


async def test_fluxo_meta_confere_cada_corpo_e_manifesto(monkeypatch):
    calls, bodies = install_anttpedagio_source(
        monkeypatch, {"volume-2023.csv": TRAFEGO}, plazas=PRACAS
    )
    frame, meta = await api.fluxo_pedagio(ano=2023, return_meta=True)
    assert frame["volume"].tolist() == [423]
    assert frame["municipio"].tolist() == ["Municipio"]
    assert meta.dataset == "antt_pedagio_fluxo"
    assert meta.contract_version == meta.schema_version == "3.0"
    assert meta.data_sources == meta.attempted_sources == ["antt_pedagio"]
    assert meta.selected_source == meta.source == "antt_pedagio"
    assert meta.source_url == "https://dados.antt.gov.br/volume-2023.csv"
    assert meta.source_details["received_bytes"] == sum(
        len(bodies[str(request.url)]) for request in calls
    )
    assert meta.source_details["data_file_bytes"] == len(TRAFEGO) + len(PRACAS)
    acquired = meta.source_details["acquisitions"]
    receipts = [attempt for entry in acquired for attempt in entry["acquisition"]["attempts"]]
    assert len(receipts) == len(calls) == 4
    for receipt in receipts:
        body = bodies[receipt["url"]]
        assert receipt["sha256"] == hashlib.sha256(body).hexdigest()
        assert receipt["size_bytes"] == len(body)
        assert receipt["status"] == 200 and receipt["complete_body"] and receipt["closed"]
    assert (
        meta.fetch_timestamp
        == meta.fetched_at
        == max(datetime.fromisoformat(receipt["finished_at"]) for receipt in receipts)
    )
    manifest = json.dumps(
        meta.source_details["manifest"],
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    ).encode()
    assert meta.raw_content_hash == hashlib.sha256(manifest).hexdigest()
    assert meta.raw_content_size == len(manifest)
    coverage = meta.source_details["coverage"]
    assert coverage["source_validated_rows"] == coverage["selected_occurrences"] == 1
    assert coverage["all_resources_eof"] is True
    assert coverage["transactional_snapshot"] is False
    assert meta.source_details["enrichment"]["status"] == "available"
    serialized = meta.to_dict()
    serialized["source_details"]["query"]["anos"].append(1900)
    assert meta.source_details["query"]["anos"] == [2023]
    assert MetaInfo.from_dict(meta.to_dict()).to_dict() == meta.to_dict()


async def test_fluxo_multianual_preserva_todos_arquivos_e_revisoes_literais(monkeypatch):
    calls, _ = install_anttpedagio_source(
        monkeypatch,
        {
            "volume-2022.csv": TRAFEGO.replace(b"2023", b"2022"),
            "volume-2023.csv": TRAFEGO,
        },
    )
    frame, meta = await api.fluxo_pedagio(
        ano_inicio=2022, ano_fim=2023, enriquecer=False, return_meta=True
    )
    assert frame["data"].dt.year.tolist() == [2022, 2023]
    assert len(calls) == 3
    acquired = meta.source_details["acquisitions"][0]["acquisition"]
    assert [file["ano"] for file in acquired["files"]] == [2022, 2023]
    assert acquired["requested_years"] == [2022, 2023]
    assert all(
        file["resource"]["last_modified"] == "2026-08-28T11:35:14.750047"
        for file in acquired["files"]
    )
    assert meta.source_details["coverage"]["source_validated_rows"] == 2
    assert meta.source_details["query"]["anos"] == [2022, 2023]


async def test_pracas_meta_preserva_url_efetiva(monkeypatch):
    install_anttpedagio_source(monkeypatch, {}, plazas=PRACAS)
    frame, meta = await api.pracas_pedagio(return_meta=True)
    assert frame["municipio"].tolist() == ["Municipio"]
    assert meta.source_url == "https://dados.antt.gov.br/pracas.csv"
    assert meta.data_sources == ["antt_pedagio"]
    assert meta.raw_content_size == len(PRACAS)
    assert meta.dataset == "antt_pedagio_pracas"
    assert meta.schema_version == meta.contract_version == "1.0.1"


async def test_fluxo_urls_sem_repeticao_com_nova_tentativa(monkeypatch):
    class Once(dict):
        def get(self, key, default=None):
            return self.pop(key, default)

    url = "https://dados.antt.gov.br/volume-2023.csv"
    calls, _ = install_anttpedagio_source(
        monkeypatch, {"volume-2023.csv": TRAFEGO}, faults=Once({url: 503})
    )
    frame, meta = await api.fluxo_pedagio(ano=2023, enriquecer=False, return_meta=True)
    assert frame["volume"].tolist() == [423] and len(calls) == 3
    assert meta.source_details["source_urls"] == [str(calls[0].url), url]
