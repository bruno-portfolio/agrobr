from __future__ import annotations

import csv
import io
import warnings
from pathlib import Path

import pytest

from agrobr import contracts, datasets
from agrobr.alt.mapa_psr import api, parser
from agrobr.exceptions import ContractViolationError, SourceUnavailableError
from agrobr.utils.warnings import warn_once_reset
from tests.helpers import binary_stream, conferir_corpo, levanta_exatamente, sem_excecao

GOLDEN = Path(__file__).resolve().parents[1] / "golden_data/mapa_psr/seguradoras_20260918"
CHAVE_SEM_SEGURADORA = ["nr_apolice", "ano_apolice", "uf", "cultura", "cd_ibge"]
PROPOSTA_REENVIADA = "Mapfre Seguros Gerais S.A."
AVISO_REENVIO = (
    "PSR: 1 registro(s) publicados em dobro pelo MAPA, iguais em todas as colunas, saem uma vez só: "
    "1977000249501/2009/Mapfre Seguros Gerais S.A."
)


def _servir(monkeypatch: pytest.MonkeyPatch, conteudo: bytes) -> None:
    monkeypatch.setattr(api, "_resolve_periodos", lambda *_: ["2006-2015"])
    monkeypatch.setattr(api.client, "open_periodo", lambda _: binary_stream(conteudo))


def _reenvio_divergente() -> bytes:
    linhas = list(
        csv.reader(
            io.StringIO((GOLDEN / "apolices.csv").read_text(encoding="utf-8")), delimiter=";"
        )
    )
    area = linhas[0].index("NR_AREA_TOTAL")
    ultima = max(i for i, linha in enumerate(linhas) if linha[0] == PROPOSTA_REENVIADA)
    linhas[ultima][area] = "13"
    saida = io.StringIO(newline="")
    csv.writer(saida, delimiter=";", lineterminator="\n").writerows(linhas)
    return saida.getvalue().encode("utf-8")


def test_seguradora_nula_viola_o_contrato():
    frame = parser.parse_apolices((GOLDEN / "apolices.csv").read_bytes())
    frame.loc[0, "seguradora"] = None
    with levanta_exatamente(ContractViolationError, match="'seguradora' has 1 null"):
        contracts.validate_dataset(frame.drop_duplicates(), "mapa_psr_apolices")


@pytest.mark.parametrize("camada", ["fonte", "dataset"])
async def test_apolice_publicada_em_dobro_sai_uma_vez_com_aviso(monkeypatch, camada):
    _servir(monkeypatch, (GOLDEN / "apolices.csv").read_bytes())
    warn_once_reset()
    with sem_excecao(), warnings.catch_warnings(record=True) as avisos:
        warnings.simplefilter("always")
        if camada == "fonte":
            frame, meta = await api.apolices(ano=2009, return_meta=True)
        else:
            frame, meta = await datasets.seguro_rural(tipo="apolices", ano=2009, return_meta=True)
    assert [str(aviso.message) for aviso in avisos if "em dobro" in str(aviso.message)] == [
        AVISO_REENVIO
    ]
    reenviada = frame[frame["seguradora"] == PROPOSTA_REENVIADA]
    assert reenviada[["nr_apolice", "area_total", "valor_premio"]].values.tolist() == [
        ["1977000249501", 12.0, 288.62]
    ]
    assert len(frame) == 3
    conferir_corpo(meta, (GOLDEN / "apolices.csv").read_bytes())
    assert meta.source_details.get("duplicatas_colapsadas") == {
        "linhas": 1,
        "apolices": ["1977000249501/2009/Mapfre Seguros Gerais S.A."],
    }


@pytest.mark.parametrize("camada", ["fonte", "dataset"])
async def test_chave_repetida_com_valor_diferente_continua_erro(monkeypatch, camada):
    _servir(monkeypatch, _reenvio_divergente())
    if camada == "fonte":
        with levanta_exatamente(ContractViolationError, match="com valores diferentes"):
            await api.apolices(ano=2009)
    else:
        with levanta_exatamente(SourceUnavailableError, match="com valores diferentes"):
            await datasets.seguro_rural(tipo="apolices", ano=2009)
