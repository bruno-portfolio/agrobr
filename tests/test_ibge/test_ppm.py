from __future__ import annotations

import json
import warnings
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from agrobr import datasets, ibge
from agrobr.ibge import agregados, client

GALINHAS = Path(__file__).resolve().parents[1] / "golden_data/ibge/ppm_galinhas_oficial_20260922"
CORPO_GALINHAS = json.loads(
    (GALINHAS / "agregados_3939_2024_galinhas.json").read_text(encoding="utf-8")
)
METADADOS_3939 = json.loads((GALINHAS / "metadados_3939.json").read_text(encoding="utf-8"))
REBANHOS_OFICIAIS = {
    str(categoria["id"]): categoria["nome"]
    for classificacao in METADADOS_3939["classificacoes"]
    if classificacao["id"] == 79
    for categoria in classificacao["categorias"]
}


class TestPpmValidation:
    @pytest.mark.asyncio
    async def test_especie_invalida(self):
        with pytest.raises(ValueError) as exc:
            await ibge.ppm("especie_inexistente")

        assert "Espécie/produto inválido" in str(exc.value)
        assert "bovino" in str(exc.value)


class TestPpmMocked:
    @pytest.fixture
    def mock_rebanho_response(self):
        return pd.DataFrame(
            {
                "NC": ["3", "3", "3"],
                "NN": ["Unidade da Federação"] * 3,
                "MC": ["51", "41", "43"],
                "MN": ["Mato Grosso", "Paraná", "Rio Grande do Sul"],
                "V": ["33500000", "9500000", "12500000"],
                "D1C": ["2023", "2023", "2023"],
                "D1N": ["2023", "2023", "2023"],
                "D2C": ["105", "105", "105"],
                "D2N": ["Efetivo dos rebanhos"] * 3,
                "D3C": ["2670", "2670", "2670"],
                "D3N": ["Bovino", "Bovino", "Bovino"],
            }
        )

    @pytest.fixture
    def mock_producao_response(self):
        return pd.DataFrame(
            {
                "NC": ["3", "3"],
                "NN": ["Unidade da Federação"] * 2,
                "MC": ["31", "52"],
                "MN": ["Minas Gerais", "Goiás"],
                "V": ["9500000", "4200000"],
                "D1C": ["2023", "2023"],
                "D1N": ["2023", "2023"],
                "D2C": ["106", "106"],
                "D2N": ["Produção de origem animal"] * 2,
                "D3C": ["2682", "2682"],
                "D3N": ["Leite", "Leite"],
            }
        )

    @pytest.mark.asyncio
    async def test_ppm_rebanho_calls_table_3939(self, mock_rebanho_response):
        with patch.object(client, "fetch_sidra", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = mock_rebanho_response
            await ibge.ppm("bovino", ano=2023)
            call_args = mock_fetch.call_args
            assert call_args.kwargs["table_code"] == "3939"
            assert call_args.kwargs["variable"] == "105"
            assert call_args.kwargs["classifications"] == {"79": "2670"}


def _servir_galinhas_oficiais(monkeypatch):
    async def fetch_sidra(**kwargs):
        return agregados.to_sidra_frame(
            CORPO_GALINHAS,
            variable=kwargs["variable"],
            classifications=kwargs["classifications"],
        )

    fetch = AsyncMock(side_effect=fetch_sidra)
    monkeypatch.setattr(client, "fetch_sidra", fetch)
    return fetch


async def test_galinhas_entrega_a_categoria_oficial_com_o_rotulo_oficial(monkeypatch):
    fetch = _servir_galinhas_oficiais(monkeypatch)
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        frame = await ibge.ppm("galinhas", ano=2024, nivel="brasil")
        dataset = await datasets.pecuaria_municipal("galinhas", ano=2024, nivel="brasil")

    categoria = fetch.call_args.kwargs["classifications"]["79"]
    rotulos = CORPO_GALINHAS[0]["resultados"][0]["classificacoes"][0]["categoria"]
    assert REBANHOS_OFICIAIS.get(categoria) == rotulos.get(categoria)
    esperado = [
        {
            "ano": int(ano),
            "localidade": serie["localidade"]["nome"],
            "localidade_cod": int(serie["localidade"]["id"]),
            "especie": rotulos[categoria].split(" - ")[-1],
            "valor": float(valor),
            "unidade": CORPO_GALINHAS[0]["unidade"].lower(),
        }
        for serie in CORPO_GALINHAS[0]["resultados"][0]["series"]
        for ano, valor in serie["serie"].items()
    ]
    colunas = ["ano", "localidade", "localidade_cod", "especie", "valor", "unidade"]
    assert frame[colunas].to_dict("records") == esperado
    assert dataset[colunas].to_dict("records") == esperado


async def test_galinhas_poedeiras_vira_alias_que_avisa_das_matrizeiras(monkeypatch):
    fetch = _servir_galinhas_oficiais(monkeypatch)
    canonico = await ibge.ppm("galinhas", ano=2024, nivel="brasil")
    with warnings.catch_warnings(record=True) as avisos:
        warnings.simplefilter("always")
        alias = await ibge.ppm("galinhas_poedeiras", ano=2024, nivel="brasil")
    dataset = await datasets.pecuaria_municipal("galinhas_poedeiras", ano=2024, nivel="brasil")

    categoria = fetch.call_args.kwargs["classifications"]["79"]
    rotulo = CORPO_GALINHAS[0]["resultados"][0]["classificacoes"][0]["categoria"].get(categoria)
    assert [aviso.category for aviso in avisos] == [FutureWarning]
    mensagem = str(avisos[0].message)
    assert "'galinhas_poedeiras'" in mensagem
    assert f"'{rotulo}'" in mensagem
    assert "poedeiras e matrizeiras" in mensagem
    assert "especie='galinhas'" in mensagem
    pd.testing.assert_frame_equal(alias, canonico)
    pd.testing.assert_frame_equal(dataset.drop(columns="cod_municipio"), canonico)
    assert dataset["cod_municipio"].isna().all()
