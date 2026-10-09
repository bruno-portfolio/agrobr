from __future__ import annotations

import warnings

import pytest

from agrobr import contracts
from agrobr.alt.mapa_psr import api, client
from agrobr.datasets.seguro_rural import seguro_rural
from tests.helpers import binary_stream, isolated_dataset_case

PUBLICADA_NEGATIVA = {
    "ANO_APOLICE": "2010",
    "NR_APOLICE": "020001576",
    "SG_UF_PROPRIEDADE": "MG",
    "NM_MUNICIPIO_PROPRIEDADE": "CARMO DO RIO CLARO",
    "CD_GEOCMU": "3114402",
    "NM_CULTURA_GLOBAL": "MILHO 1ª SAFRA",
    "NM_CLASSIF_PRODUTO": "-",
    "NR_AREA_TOTAL": "0",
    "VL_PREMIO_LIQUIDO": "-100",
    "VL_SUBVENCAO_FEDERAL": "0",
    "VL_LIMITE_GARANTIA": "0",
    "VALOR_INDENIZACAO": "",
    "EVENTO_PREPONDERANTE": "-",
    "NR_PRODUTIVIDADE_ESTIMADA": "4200",
    "NR_PRODUTIVIDADE_SEGURADA": "2520",
    "NivelDeCobertura": "60",
    "PE_TAXA": "",
    "NM_RAZAO_SOCIAL": "Allianz Seguros S.A",
}
COMUM = {
    **PUBLICADA_NEGATIVA,
    "NR_APOLICE": "020001577",
    "NR_AREA_TOTAL": "12",
    "VL_PREMIO_LIQUIDO": "1500",
    "VL_SUBVENCAO_FEDERAL": "600",
    "VL_LIMITE_GARANTIA": "20000",
}
AVISO = (
    "PSR: 1 apólice(s) com valor_premio negativo publicado pelo MAPA, mantido como publicado: "
    "020001576/2010"
)


def _csv(*linhas: dict[str, str]) -> bytes:
    cabecalho = list(linhas[0])
    corpo = [";".join(cabecalho)] + [";".join(linha[c] for c in cabecalho) for linha in linhas]
    return "\n".join(corpo).encode("utf-8")


@pytest.fixture()
def arquivo_2006_2015(monkeypatch: pytest.MonkeyPatch) -> bytes:
    corpo = _csv(PUBLICADA_NEGATIVA, COMUM)
    monkeypatch.setattr(client, "open_periodo", lambda _: binary_stream(corpo))
    return corpo


@pytest.mark.usefixtures("arquivo_2006_2015")
async def test_fonte_premio_negativo_sai_como_publicado_com_aviso():
    with pytest.warns(UserWarning, match="valor_premio negativo"):
        df, meta = await api.apolices(ano=2010, return_meta=True)

    premios = dict(zip(df["nr_apolice"], df["valor_premio"], strict=True))
    assert premios == {"020001576": -100.0, "020001577": 1500.0}
    assert meta.validation_warnings.count(AVISO) == 1
    contracts.validate_dataset(df, "mapa_psr_apolices")


async def test_dataset_premio_negativo_sai_como_publicado_com_aviso():
    corpo = _csv(PUBLICADA_NEGATIVA, COMUM)
    with isolated_dataset_case("premio_negativo") as monkeypatch:
        monkeypatch.setattr(client, "open_periodo", lambda _: binary_stream(corpo))
        with pytest.warns(UserWarning, match="valor_premio negativo"):
            df, meta = await seguro_rural(ano=2010, return_meta=True)

    assert df.loc[df["nr_apolice"] == "020001576", "valor_premio"].tolist() == [-100.0]
    assert AVISO in meta.validation_warnings


async def test_premio_negativo_fora_do_recorte_nao_avisa(arquivo_2006_2015: bytes):
    with warnings.catch_warnings(record=True) as capturados:
        warnings.simplefilter("always")
        fonte, meta_fonte = await api.apolices(ano=2010, uf="SP", return_meta=True)
        with isolated_dataset_case("premio_negativo_vazio") as monkeypatch:
            monkeypatch.setattr(client, "open_periodo", lambda _: binary_stream(arquivo_2006_2015))
            dataset, meta_dataset = await seguro_rural(ano=2010, uf="SP", return_meta=True)

    assert fonte.empty and dataset.empty
    assert not [w for w in capturados if "valor_premio negativo" in str(w.message)]
    assert not [
        a
        for a in meta_fonte.validation_warnings + meta_dataset.validation_warnings
        if "valor_premio" in a
    ]
    contracts.validate_dataset(fonte, "mapa_psr_apolices")


def test_contrato_aceita_negativo_so_no_premio_das_apolices():
    apolices = {c.name: c.min_value for c in contracts.get_contract("mapa_psr_apolices").columns}
    sinistros = {c.name: c.min_value for c in contracts.get_contract("mapa_psr_sinistros").columns}

    assert apolices["valor_premio"] is None
    assert [
        apolices[c]
        for c in ("area_total", "valor_subvencao", "valor_limite_garantia", "valor_indenizacao")
    ] == [0, 0, 0, 0]
    assert sinistros["valor_premio"] == 0
