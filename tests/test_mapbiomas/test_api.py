from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from agrobr.exceptions import InvalidParameterError
from agrobr.mapbiomas import api
from tests.helpers import mapbiomas_workbook_bundle as workbook_bundle

GOLDEN_DIR = Path(__file__).parent.parent / "golden_data" / "mapbiomas"


def _golden_xlsx() -> bytes:
    return GOLDEN_DIR.joinpath("biome_state_sample", "response.xlsx").read_bytes()


class TestCobertura:
    @pytest.mark.asyncio
    async def test_combined_filters(self):
        xlsx_bytes = _golden_xlsx()
        with patch.object(
            api.client,
            "fetch_biome_state_bundle",
            new_callable=AsyncMock,
            return_value=workbook_bundle(
                *(xlsx_bytes, "https://storage.googleapis.com/mapbiomas-public/test.xlsx")
            ),
        ):
            df = await api.cobertura(colecao=10, bioma="Cerrado", uf="GO", ano=2020)

        assert len(df) >= 1
        assert (df["bioma"] == "Cerrado").all()
        assert (df["uf"] == "GO").all()
        assert (df["ano"] == 2020).all()


class TestCoberturaMunicipal:
    @pytest.mark.asyncio
    async def test_invalid_uf_raises_before_municipal_fetch(self):
        with (
            patch.object(
                api.client,
                "fetch_biome_state_municipality_bundle",
                new_callable=AsyncMock,
            ) as mock_fetch,
            pytest.raises(InvalidParameterError, match="UF inválida: 'XX'.*siglas válidas: AC, AL"),
        ):
            await api.cobertura(colecao=11, nivel="municipio", uf="XX")

        mock_fetch.assert_not_awaited()


class TestTransicao:
    @pytest.mark.asyncio
    async def test_invalid_bioma_raises_before_download(self):
        with (
            patch.object(api.client, "fetch_biome_state_bundle", new_callable=AsyncMock) as fetch,
            pytest.raises(InvalidParameterError, match="Bioma inválido"),
        ):
            await api.transicao(colecao=10, bioma="Atlantida")

        fetch.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_filter_uf_nome_completo(self):
        xlsx_bytes = _golden_xlsx()
        with patch.object(
            api.client,
            "fetch_biome_state_bundle",
            new_callable=AsyncMock,
            return_value=workbook_bundle(
                *(xlsx_bytes, "https://storage.googleapis.com/mapbiomas-public/test.xlsx")
            ),
        ):
            df = await api.transicao(colecao=10, uf="Goiás")

        assert not df.empty
        assert set(df["uf"]) == {"GO"}

    @pytest.mark.asyncio
    async def test_filter_periodo(self):
        xlsx_bytes = _golden_xlsx()
        with patch.object(
            api.client,
            "fetch_biome_state_bundle",
            new_callable=AsyncMock,
            return_value=workbook_bundle(
                *(xlsx_bytes, "https://storage.googleapis.com/mapbiomas-public/test.xlsx")
            ),
        ):
            df = await api.transicao(colecao=10, periodo="1985-2024")

        assert len(df) >= 1
        assert (df["periodo"] == "1985-2024").all()


class TestValidacaoColecao:
    @pytest.mark.asyncio
    async def test_transicao_colecao_invalida(self):
        with pytest.raises(ValueError, match="nao suportada"):
            await api.transicao(colecao=9)


@pytest.mark.parametrize(
    ("funcao", "argumentos"),
    [
        ("cobertura", {"classe_id": 99999}),
        ("transicao", {"classe_de_id": 399}),
        ("transicao", {"classe_para_id": 99999}),
    ],
    ids=["cobertura", "transicao_de", "transicao_para"],
)
async def test_classe_fora_das_publicadas_levanta_com_a_lista(funcao, argumentos):
    with patch.object(
        api.client,
        "fetch_biome_state_bundle",
        new_callable=AsyncMock,
        return_value=workbook_bundle(
            _golden_xlsx(), "https://storage.googleapis.com/mapbiomas-public/test.xlsx"
        ),
    ):
        publicadas = await getattr(api, funcao)(colecao=10)
        nome = next(iter(argumentos))
        coluna = "classe_id" if nome == "classe_id" else nome
        lista = sorted(int(codigo) for codigo in publicadas[coluna].unique())
        with pytest.raises(InvalidParameterError) as erro:
            await getattr(api, funcao)(colecao=10, **argumentos)
    assert str(erro.value) == (
        f"{nome}={argumentos[nome]} fora das classes publicadas na coleção 10: {lista}"
    )


async def test_classe_fora_das_publicadas_no_municipal_levanta(replay_mapbiomas, municipal_capture):
    replay_mapbiomas()
    publicadas = sorted({int(row["cells"]["I"]) for row in municipal_capture["oracle"]["rows"]})
    with pytest.raises(InvalidParameterError, match="fora das classes publicadas na coleção 11"):
        await api.cobertura(nivel="municipio", classe_id=99999)
    assert 99999 not in publicadas and 39 in publicadas
