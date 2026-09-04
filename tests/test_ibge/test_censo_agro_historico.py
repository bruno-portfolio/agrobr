from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from agrobr.contracts import get_contract, has_contract
from agrobr.contracts.ibge import IBGE_CENSO_AGRO_HISTORICO_V1
from agrobr.datasets.censo_agropecuario_historico import (
    CENSO_AGROPECUARIO_HISTORICO_INFO,
)
from agrobr.ibge import client
from agrobr.ibge.censo_api import (
    _parse_censo_historico_raw,
    censo_agro_historico,
    temas_censo_agro_historico,
)

# ---------------------------------------------------------------------------
# Bloco 1: testes de constantes (mantidos)
# ---------------------------------------------------------------------------


class TestTabelasCensoHistorico:
    def test_has_9_temas(self):
        assert len(client.TABELAS_CENSO_HISTORICO) == 9

    def test_estabelecimentos_area(self):
        assert client.TABELAS_CENSO_HISTORICO["estabelecimentos_area"] == "263"

    def test_uso_terra(self):
        assert client.TABELAS_CENSO_HISTORICO["uso_terra"] == "264"

    def test_pessoal_tratores(self):
        assert client.TABELAS_CENSO_HISTORICO["pessoal_tratores"] == "265"

    def test_condicao_produtor(self):
        assert client.TABELAS_CENSO_HISTORICO["condicao_produtor"] == "280"

    def test_efetivo_animais(self):
        assert client.TABELAS_CENSO_HISTORICO["efetivo_animais"] == "281"

    def test_producao_animal(self):
        assert client.TABELAS_CENSO_HISTORICO["producao_animal"] == "282"

    def test_producao_vegetal(self):
        assert client.TABELAS_CENSO_HISTORICO["producao_vegetal"] == "283"

    def test_lavoura_permanente(self):
        assert client.TABELAS_CENSO_HISTORICO["lavoura_permanente"] == "1730"

    def test_lavoura_temporaria(self):
        assert client.TABELAS_CENSO_HISTORICO["lavoura_temporaria"] == "1731"

    def test_all_codes_numeric(self):
        for tema, code in client.TABELAS_CENSO_HISTORICO.items():
            assert code.isdigit(), f"{tema} has non-numeric code: {code}"

    def test_no_duplicate_codes(self):
        codes = list(client.TABELAS_CENSO_HISTORICO.values())
        assert len(codes) == len(set(codes))


class TestPeriodosCensoHistorico:
    def test_consistency_with_tabelas(self):
        assert set(client.PERIODOS_CENSO_HISTORICO) == set(client.TABELAS_CENSO_HISTORICO)

    def test_1985_present_in_all_temas(self):
        for tema, periodos in client.PERIODOS_CENSO_HISTORICO.items():
            assert 1985 in periodos, f"1985 missing from {tema}"

    def test_all_sorted_ascending(self):
        for tema, periodos in client.PERIODOS_CENSO_HISTORICO.items():
            assert periodos == sorted(periodos), f"{tema} periods not sorted"

    def test_all_have_at_least_one_period(self):
        for tema, periodos in client.PERIODOS_CENSO_HISTORICO.items():
            assert len(periodos) >= 1, f"{tema} has no periods"

    def test_estabelecimentos_area_10_censos(self):
        expected = [1920, 1940, 1950, 1960, 1970, 1975, 1980, 1985, 1995, 2006]
        assert client.PERIODOS_CENSO_HISTORICO["estabelecimentos_area"] == expected

    def test_uso_terra_6_censos(self):
        expected = [1970, 1975, 1980, 1985, 1995, 2006]
        assert client.PERIODOS_CENSO_HISTORICO["uso_terra"] == expected

    def test_pessoal_tratores_6_censos(self):
        expected = [1970, 1975, 1980, 1985, 1995, 2006]
        assert client.PERIODOS_CENSO_HISTORICO["pessoal_tratores"] == expected

    def test_condicao_produtor_10_censos(self):
        expected = [1920, 1940, 1950, 1960, 1970, 1975, 1980, 1985, 1995, 2006]
        assert client.PERIODOS_CENSO_HISTORICO["condicao_produtor"] == expected

    def test_efetivo_animais_6_censos(self):
        expected = [1970, 1975, 1980, 1985, 1995, 2006]
        assert client.PERIODOS_CENSO_HISTORICO["efetivo_animais"] == expected

    def test_producao_animal_10_censos(self):
        expected = [1920, 1940, 1950, 1960, 1970, 1975, 1980, 1985, 1995, 2006]
        assert client.PERIODOS_CENSO_HISTORICO["producao_animal"] == expected

    def test_producao_vegetal_10_censos(self):
        expected = [1920, 1940, 1950, 1960, 1970, 1975, 1980, 1985, 1995, 2006]
        assert client.PERIODOS_CENSO_HISTORICO["producao_vegetal"] == expected

    def test_lavoura_permanente_9_censos(self):
        expected = [1940, 1950, 1960, 1970, 1975, 1980, 1985, 1995, 2006]
        assert client.PERIODOS_CENSO_HISTORICO["lavoura_permanente"] == expected

    def test_lavoura_temporaria_9_censos(self):
        expected = [1940, 1950, 1960, 1970, 1975, 1980, 1985, 1995, 2006]
        assert client.PERIODOS_CENSO_HISTORICO["lavoura_temporaria"] == expected

    def test_only_valid_census_years(self):
        valid_years = {1920, 1940, 1950, 1960, 1970, 1975, 1980, 1985, 1995, 2006}
        for tema, periodos in client.PERIODOS_CENSO_HISTORICO.items():
            for ano in periodos:
                assert ano in valid_years, f"{tema} has invalid year: {ano}"


class TestVariaveisCensoHistorico:
    def test_consistency_with_tabelas(self):
        assert set(client.VARIAVEIS_CENSO_HISTORICO) == set(client.TABELAS_CENSO_HISTORICO)

    def test_all_have_at_least_one_variable(self):
        for tema, vars_ in client.VARIAVEIS_CENSO_HISTORICO.items():
            assert len(vars_) >= 1, f"{tema} has no variables"

    def test_all_ids_numeric(self):
        for tema, vars_ in client.VARIAVEIS_CENSO_HISTORICO.items():
            for nome, vid in vars_.items():
                assert vid.isdigit(), f"{tema}.{nome} has non-numeric id: {vid}"

    def test_estabelecimentos_area_4_vars(self):
        vars_ = client.VARIAVEIS_CENSO_HISTORICO["estabelecimentos_area"]
        assert vars_["estabelecimentos"] == "183"
        assert vars_["estabelecimentos_pct"] == "1000183"
        assert vars_["area"] == "184"
        assert vars_["area_pct"] == "1000184"

    def test_uso_terra_2_vars(self):
        vars_ = client.VARIAVEIS_CENSO_HISTORICO["uso_terra"]
        assert vars_["area"] == "184"
        assert vars_["area_pct"] == "1000184"
        assert len(vars_) == 2

    def test_pessoal_tratores_2_vars(self):
        vars_ = client.VARIAVEIS_CENSO_HISTORICO["pessoal_tratores"]
        assert vars_["pessoal_ocupado"] == "185"
        assert vars_["tratores"] == "1862"
        assert len(vars_) == 2

    def test_condicao_produtor_4_vars(self):
        vars_ = client.VARIAVEIS_CENSO_HISTORICO["condicao_produtor"]
        assert len(vars_) == 4
        assert vars_["estabelecimentos"] == "183"
        assert vars_["area"] == "184"

    def test_efetivo_animais_1_var(self):
        vars_ = client.VARIAVEIS_CENSO_HISTORICO["efetivo_animais"]
        assert vars_["efetivo"] == "1863"
        assert len(vars_) == 1

    def test_producao_animal_1_var(self):
        vars_ = client.VARIAVEIS_CENSO_HISTORICO["producao_animal"]
        assert vars_["producao"] == "1864"
        assert len(vars_) == 1

    def test_producao_vegetal_2_vars(self):
        vars_ = client.VARIAVEIS_CENSO_HISTORICO["producao_vegetal"]
        assert vars_["producao"] == "1865"
        assert vars_["area_colhida"] == "216"
        assert len(vars_) == 2

    def test_lavoura_permanente_1_var(self):
        vars_ = client.VARIAVEIS_CENSO_HISTORICO["lavoura_permanente"]
        assert vars_["quantidade_produzida"] == "214"
        assert len(vars_) == 1

    def test_lavoura_temporaria_1_var(self):
        vars_ = client.VARIAVEIS_CENSO_HISTORICO["lavoura_temporaria"]
        assert vars_["quantidade_produzida"] == "214"
        assert len(vars_) == 1


class TestClassificacoesCensoHistorico:
    def test_consistency_with_tabelas(self):
        assert set(client.CLASSIFICACOES_CENSO_HISTORICO) == set(client.TABELAS_CENSO_HISTORICO)

    def test_pessoal_tratores_sem_classificacao(self):
        assert client.CLASSIFICACOES_CENSO_HISTORICO["pessoal_tratores"] == {}

    def test_temas_com_classificacao(self):
        temas_com = [
            "estabelecimentos_area",
            "uso_terra",
            "condicao_produtor",
            "efetivo_animais",
            "producao_animal",
            "producao_vegetal",
            "lavoura_permanente",
            "lavoura_temporaria",
        ]
        for tema in temas_com:
            classifs = client.CLASSIFICACOES_CENSO_HISTORICO[tema]
            assert len(classifs) >= 1, f"{tema} should have classification"

    def test_all_classif_ids_numeric(self):
        for tema, classifs in client.CLASSIFICACOES_CENSO_HISTORICO.items():
            for cid in classifs:
                assert cid.isdigit(), f"{tema} has non-numeric classif id: {cid}"

    def test_all_values_are_all(self):
        for tema, classifs in client.CLASSIFICACOES_CENSO_HISTORICO.items():
            for cid, val in classifs.items():
                assert val == "all", f"{tema}/{cid} value should be 'all', got: {val}"

    def test_estabelecimentos_area_classif_220(self):
        assert "220" in client.CLASSIFICACOES_CENSO_HISTORICO["estabelecimentos_area"]

    def test_uso_terra_classif_222(self):
        assert "222" in client.CLASSIFICACOES_CENSO_HISTORICO["uso_terra"]

    def test_condicao_produtor_classif_12441(self):
        assert "12441" in client.CLASSIFICACOES_CENSO_HISTORICO["condicao_produtor"]

    def test_efetivo_animais_classif_12443(self):
        assert "12443" in client.CLASSIFICACOES_CENSO_HISTORICO["efetivo_animais"]

    def test_producao_animal_classif_12444(self):
        assert "12444" in client.CLASSIFICACOES_CENSO_HISTORICO["producao_animal"]

    def test_producao_vegetal_classif_12445(self):
        assert "12445" in client.CLASSIFICACOES_CENSO_HISTORICO["producao_vegetal"]

    def test_lavoura_permanente_classif_227(self):
        assert "227" in client.CLASSIFICACOES_CENSO_HISTORICO["lavoura_permanente"]

    def test_lavoura_temporaria_classif_226(self):
        assert "226" in client.CLASSIFICACOES_CENSO_HISTORICO["lavoura_temporaria"]


class TestNiveisCensoHistorico:
    def test_consistency_with_tabelas(self):
        assert set(client.NIVEIS_CENSO_HISTORICO) == set(client.TABELAS_CENSO_HISTORICO)

    def test_all_include_brasil(self):
        for tema, niveis in client.NIVEIS_CENSO_HISTORICO.items():
            assert "brasil" in niveis, f"{tema} missing 'brasil'"

    def test_all_include_uf(self):
        for tema, niveis in client.NIVEIS_CENSO_HISTORICO.items():
            assert "uf" in niveis, f"{tema} missing 'uf'"

    def test_all_include_regiao(self):
        for tema, niveis in client.NIVEIS_CENSO_HISTORICO.items():
            assert "regiao" in niveis, f"{tema} missing 'regiao'"

    def test_none_include_municipio(self):
        for tema, niveis in client.NIVEIS_CENSO_HISTORICO.items():
            assert "municipio" not in niveis, f"{tema} should not have 'municipio'"


class TestCategoriasCensoHistorico:
    def test_pessoal_tratores_not_in_categorias(self):
        assert "pessoal_tratores" not in client.CATEGORIAS_CENSO_HISTORICO

    def test_temas_with_classif_have_categorias(self):
        for tema, classifs in client.CLASSIFICACOES_CENSO_HISTORICO.items():
            if classifs:
                assert tema in client.CATEGORIAS_CENSO_HISTORICO, (
                    f"{tema} has classification but no categories"
                )

    def test_estabelecimentos_area_6_cats(self):
        assert len(client.CATEGORIAS_CENSO_HISTORICO["estabelecimentos_area"]) == 6

    def test_uso_terra_7_cats(self):
        assert len(client.CATEGORIAS_CENSO_HISTORICO["uso_terra"]) == 7

    def test_condicao_produtor_5_cats(self):
        assert len(client.CATEGORIAS_CENSO_HISTORICO["condicao_produtor"]) == 5

    def test_efetivo_animais_9_cats(self):
        assert len(client.CATEGORIAS_CENSO_HISTORICO["efetivo_animais"]) == 9

    def test_producao_animal_4_cats(self):
        assert len(client.CATEGORIAS_CENSO_HISTORICO["producao_animal"]) == 4

    def test_producao_vegetal_13_cats(self):
        assert len(client.CATEGORIAS_CENSO_HISTORICO["producao_vegetal"]) == 13

    def test_lavoura_permanente_14_cats(self):
        assert len(client.CATEGORIAS_CENSO_HISTORICO["lavoura_permanente"]) == 14

    def test_lavoura_temporaria_20_cats(self):
        assert len(client.CATEGORIAS_CENSO_HISTORICO["lavoura_temporaria"]) == 20

    def test_all_cat_ids_numeric(self):
        for tema, cats in client.CATEGORIAS_CENSO_HISTORICO.items():
            for cid in cats:
                assert cid.isdigit(), f"{tema} has non-numeric cat id: {cid}"

    def test_all_cat_names_non_empty(self):
        for tema, cats in client.CATEGORIAS_CENSO_HISTORICO.items():
            for cid, nome in cats.items():
                assert nome, f"{tema}/{cid} has empty name"

    def test_efetivo_animais_has_aves(self):
        cats = client.CATEGORIAS_CENSO_HISTORICO["efetivo_animais"]
        assert "110064" in cats
        assert cats["110064"] == "aves"

    def test_producao_vegetal_has_soja(self):
        cats = client.CATEGORIAS_CENSO_HISTORICO["producao_vegetal"]
        assert "soja" in cats.values()

    def test_lavoura_permanente_has_cafe(self):
        cats = client.CATEGORIAS_CENSO_HISTORICO["lavoura_permanente"]
        assert "cafe" in cats.values()

    def test_lavoura_temporaria_has_milho(self):
        cats = client.CATEGORIAS_CENSO_HISTORICO["lavoura_temporaria"]
        assert "milho" in cats.values()


class TestUnidadesVariaveisCensoHistorico:
    def test_area_hectares(self):
        assert client.UNIDADES_VARIAVEIS_CENSO_HISTORICO["184"] == "Hectares"

    def test_area_colhida_hectares(self):
        assert client.UNIDADES_VARIAVEIS_CENSO_HISTORICO["216"] == "Hectares"

    def test_estabelecimentos_unidades(self):
        assert client.UNIDADES_VARIAVEIS_CENSO_HISTORICO["183"] == "Unidades"

    def test_pessoal_pessoas(self):
        assert client.UNIDADES_VARIAVEIS_CENSO_HISTORICO["185"] == "Pessoas"

    def test_tratores_unidades(self):
        assert client.UNIDADES_VARIAVEIS_CENSO_HISTORICO["1862"] == "Unidades"

    def test_percentuais(self):
        assert client.UNIDADES_VARIAVEIS_CENSO_HISTORICO["1000183"] == "%"
        assert client.UNIDADES_VARIAVEIS_CENSO_HISTORICO["1000184"] == "%"

    def test_vars_with_category_dependent_units_not_in_dict(self):
        category_dependent = {"1863", "1864", "1865", "214"}
        for vid in category_dependent:
            assert vid not in client.UNIDADES_VARIAVEIS_CENSO_HISTORICO


class TestUnidadesCategoriasCensoHistorico:
    def test_aves_mil_cabecas(self):
        assert client.UNIDADES_CATEGORIAS_CENSO_HISTORICO["110064"] == "Mil cabeças"

    def test_bovinos_cabecas(self):
        assert client.UNIDADES_CATEGORIAS_CENSO_HISTORICO["110056"] == "Cabeças"

    def test_all_efetivo_except_aves_cabecas(self):
        efetivo_cats = client.CATEGORIAS_CENSO_HISTORICO["efetivo_animais"]
        for cid, nome in efetivo_cats.items():
            unit = client.UNIDADES_CATEGORIAS_CENSO_HISTORICO[cid]
            if nome == "aves":
                assert unit == "Mil cabeças"
            else:
                assert unit == "Cabeças", f"{nome} should be Cabeças, got {unit}"

    def test_leite_vaca_mil_litros(self):
        assert client.UNIDADES_CATEGORIAS_CENSO_HISTORICO["110067"] == "Mil litros"

    def test_ovos_galinha_mil_duzias(self):
        assert client.UNIDADES_CATEGORIAS_CENSO_HISTORICO["110068"] == "Mil dúzias"

    def test_la_toneladas(self):
        assert client.UNIDADES_CATEGORIAS_CENSO_HISTORICO["110069"] == "Toneladas"

    def test_producao_vegetal_laranja_mil_frutos(self):
        assert client.UNIDADES_CATEGORIAS_CENSO_HISTORICO["110073"] == "Mil frutos"

    def test_producao_vegetal_soja_toneladas(self):
        assert client.UNIDADES_CATEGORIAS_CENSO_HISTORICO["110082"] == "Toneladas"

    def test_lavoura_permanente_banana_mil_cachos(self):
        assert client.UNIDADES_CATEGORIAS_CENSO_HISTORICO["4930"] == "Mil cachos"

    def test_lavoura_temporaria_abacaxi_mil_frutos(self):
        assert client.UNIDADES_CATEGORIAS_CENSO_HISTORICO["4844"] == "Mil frutos"

    def test_lavoura_temporaria_arroz_toneladas(self):
        assert client.UNIDADES_CATEGORIAS_CENSO_HISTORICO["4851"] == "Toneladas"

    def test_all_category_dependent_temas_have_units(self):
        temas_with_cat_units = [
            "efetivo_animais",
            "producao_animal",
            "producao_vegetal",
            "lavoura_permanente",
            "lavoura_temporaria",
        ]
        for tema in temas_with_cat_units:
            cats = client.CATEGORIAS_CENSO_HISTORICO[tema]
            for cid in cats:
                assert cid in client.UNIDADES_CATEGORIAS_CENSO_HISTORICO, (
                    f"{tema}/{cid} missing from UNIDADES_CATEGORIAS"
                )

    def test_fixed_unit_temas_not_in_unidades_categorias(self):
        temas_fixed = ["estabelecimentos_area", "uso_terra", "condicao_produtor"]
        for tema in temas_fixed:
            cats = client.CATEGORIAS_CENSO_HISTORICO[tema]
            for cid in cats:
                assert cid not in client.UNIDADES_CATEGORIAS_CENSO_HISTORICO, (
                    f"{tema}/{cid} should not be in UNIDADES_CATEGORIAS"
                )


class TestTemasCensoHistorico:
    def test_has_9_temas(self):
        assert len(client.TEMAS_CENSO_HISTORICO) == 9

    def test_matches_tabelas_keys(self):
        assert list(client.TABELAS_CENSO_HISTORICO.keys()) == client.TEMAS_CENSO_HISTORICO

    def test_all_temas_present(self):
        expected = [
            "estabelecimentos_area",
            "uso_terra",
            "pessoal_tratores",
            "condicao_produtor",
            "efetivo_animais",
            "producao_animal",
            "producao_vegetal",
            "lavoura_permanente",
            "lavoura_temporaria",
        ]
        assert set(client.TEMAS_CENSO_HISTORICO) == set(expected)


# ---------------------------------------------------------------------------
# Bloco 2: testes de validacao, parsing e integracao
# ---------------------------------------------------------------------------

EXPECTED_COLS = [
    "ano",
    "localidade",
    "localidade_cod",
    "tema",
    "categoria",
    "variavel",
    "valor",
    "unidade",
    "fonte",
]


def _mock_sidra_estabelecimentos_area() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "NC": ["3", "3", "3", "3"],
            "NN": ["UF"] * 4,
            "MC": ["33", "33", "33", "33"],
            "MN": ["Unidades", "Unidades", "Hectares", "Hectares"],
            "V": ["5801809", "234567", "374924421", "15000000"],
            "D1C": ["35", "35", "35", "35"],
            "D1N": ["São Paulo"] * 4,
            "D2C": ["183", "183", "184", "184"],
            "D2N": ["Estabelecimentos"] * 2 + ["Área"] * 2,
            "D3C": ["1985", "1985", "1985", "1985"],
            "D3N": ["1985"] * 4,
            "D4C": ["110085", "110045", "110085", "110045"],
            "D4N": ["Total", "Menos de 10 ha", "Total", "Menos de 10 ha"],
        }
    )


def _mock_sidra_pessoal_tratores() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "NC": ["3", "3"],
            "NN": ["UF", "UF"],
            "MC": ["5512", "1032"],
            "MN": ["Pessoas", "Unidades"],
            "V": ["23456789", "567890"],
            "D1C": ["35", "35"],
            "D1N": ["São Paulo", "São Paulo"],
            "D2C": ["185", "1862"],
            "D2N": ["Pessoal ocupado", "Tratores"],
            "D3C": ["1985", "1985"],
            "D3N": ["1985", "1985"],
        }
    )


def _mock_sidra_efetivo_animais() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "NC": ["3", "3"],
            "NN": ["UF", "UF"],
            "MC": ["35", "35"],
            "MN": ["Cabeças", "Cabeças"],
            "V": ["15000000", "500000"],
            "D1C": ["35", "35"],
            "D1N": ["São Paulo", "São Paulo"],
            "D2C": ["1863", "1863"],
            "D2N": ["Efetivo", "Efetivo"],
            "D3C": ["1985", "1985"],
            "D3N": ["1985", "1985"],
            "D4C": ["110056", "110064"],
            "D4N": ["Bovinos", "Aves"],
        }
    )


def _mock_sidra_missing_values() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "NC": ["3", "3", "3", "3"],
            "NN": ["UF"] * 4,
            "MC": ["5512", "1032", "5512", "5512"],
            "MN": ["Pessoas", "Unidades", "Pessoas", "Pessoas"],
            "V": ["..", "...", "-", "X"],
            "D1C": ["35", "35", "35", "35"],
            "D1N": ["São Paulo"] * 4,
            "D2C": ["185", "1862", "185", "185"],
            "D2N": ["Pessoal", "Tratores", "Pessoal", "Pessoal"],
            "D3C": ["1985", "1985", "1980", "1975"],
            "D3N": ["1985", "1985", "1980", "1975"],
        }
    )


def _mock_sidra_producao_animal() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "NC": ["3", "3"],
            "NN": ["UF", "UF"],
            "MC": ["52", "53"],
            "MN": ["Mil litros", "Mil dúzias"],
            "V": ["1234567", "89012"],
            "D1C": ["35", "35"],
            "D1N": ["São Paulo", "São Paulo"],
            "D2C": ["1864", "1864"],
            "D2N": ["Produção animal"] * 2,
            "D3C": ["1985", "1985"],
            "D3N": ["1985", "1985"],
            "D4C": ["110067", "110068"],
            "D4N": ["Leite de vaca", "Ovos de galinha"],
        }
    )


class TestCensoHistoricoValidation:
    @pytest.mark.asyncio
    async def test_tema_invalido(self):
        with pytest.raises(ValueError, match="Tema não suportado"):
            await censo_agro_historico("tema_inexistente")

    @pytest.mark.asyncio
    async def test_ano_invalido(self):
        with pytest.raises(ValueError, match="Ano 1990 não disponível"):
            await censo_agro_historico("estabelecimentos_area", ano=1990)

    @pytest.mark.asyncio
    async def test_ano_list_invalido(self):
        with pytest.raises(ValueError, match="Ano 1999 não disponível"):
            await censo_agro_historico("estabelecimentos_area", ano=[1985, 1999])

    @pytest.mark.asyncio
    async def test_nivel_municipio(self):
        with pytest.raises(ValueError, match="Nível 'municipio' não disponível"):
            await censo_agro_historico("estabelecimentos_area", nivel="municipio")

    @pytest.mark.asyncio
    async def test_nivel_invalido(self):
        with pytest.raises(ValueError, match="Nível 'meso' não disponível"):
            await censo_agro_historico("estabelecimentos_area", nivel="meso")

    @pytest.mark.asyncio
    @patch("agrobr.ibge.client.fetch_sidra", new_callable=AsyncMock)
    async def test_ano_int_valido(self, mock_fetch):
        mock_fetch.return_value = pd.DataFrame()
        df = await censo_agro_historico("estabelecimentos_area", ano=1985)
        assert df.empty or list(df.columns) == EXPECTED_COLS

    @pytest.mark.asyncio
    @patch("agrobr.ibge.client.fetch_sidra", new_callable=AsyncMock)
    async def test_ano_list_valido(self, mock_fetch):
        mock_fetch.return_value = pd.DataFrame()
        df = await censo_agro_historico("uso_terra", ano=[1985, 1995])
        assert df.empty or list(df.columns) == EXPECTED_COLS

    @pytest.mark.asyncio
    @patch("agrobr.ibge.client.fetch_sidra", new_callable=AsyncMock)
    async def test_ano_none_valido(self, mock_fetch):
        mock_fetch.return_value = pd.DataFrame()
        df = await censo_agro_historico("pessoal_tratores", ano=None)
        assert df.empty or list(df.columns) == EXPECTED_COLS

    @pytest.mark.asyncio
    async def test_temas_helper(self):
        temas = await temas_censo_agro_historico()
        assert len(temas) == 9
        assert "estabelecimentos_area" in temas


class TestCensoHistoricoParsing:
    def test_estabelecimentos_area_output_schema(self):
        df = _parse_censo_historico_raw(
            _mock_sidra_estabelecimentos_area(), "estabelecimentos_area"
        )
        assert list(df.columns) == EXPECTED_COLS
        assert len(df) == 4

    def test_estabelecimentos_area_types(self):
        df = _parse_censo_historico_raw(
            _mock_sidra_estabelecimentos_area(), "estabelecimentos_area"
        )
        assert df["ano"].dtype == "Int64"
        assert df["localidade_cod"].dtype == "Int64"
        assert pd.api.types.is_numeric_dtype(df["valor"])
        assert (df["ano"] == 1985).all()

    def test_estabelecimentos_area_variavel_mapped(self):
        df = _parse_censo_historico_raw(
            _mock_sidra_estabelecimentos_area(), "estabelecimentos_area"
        )
        assert set(df["variavel"]) == {"estabelecimentos", "area"}

    def test_estabelecimentos_area_categoria_mapped(self):
        df = _parse_censo_historico_raw(
            _mock_sidra_estabelecimentos_area(), "estabelecimentos_area"
        )
        assert "total" in df["categoria"].values
        assert "menos_10ha" in df["categoria"].values

    def test_estabelecimentos_area_unidade_fixed(self):
        df = _parse_censo_historico_raw(
            _mock_sidra_estabelecimentos_area(), "estabelecimentos_area"
        )
        estab = df[df["variavel"] == "estabelecimentos"]
        assert (estab["unidade"] == "Unidades").all()
        area = df[df["variavel"] == "area"]
        assert (area["unidade"] == "Hectares").all()

    def test_estabelecimentos_area_fonte(self):
        df = _parse_censo_historico_raw(
            _mock_sidra_estabelecimentos_area(), "estabelecimentos_area"
        )
        assert (df["fonte"] == "ibge_censo_agro_historico").all()
        assert (df["tema"] == "estabelecimentos_area").all()

    def test_pessoal_tratores_categoria_total(self):
        df = _parse_censo_historico_raw(_mock_sidra_pessoal_tratores(), "pessoal_tratores")
        assert list(df.columns) == EXPECTED_COLS
        assert (df["categoria"] == "total").all()

    def test_pessoal_tratores_variaveis(self):
        df = _parse_censo_historico_raw(_mock_sidra_pessoal_tratores(), "pessoal_tratores")
        assert set(df["variavel"]) == {"pessoal_ocupado", "tratores"}

    def test_pessoal_tratores_unidades_from_fixed(self):
        df = _parse_censo_historico_raw(_mock_sidra_pessoal_tratores(), "pessoal_tratores")
        pessoal = df[df["variavel"] == "pessoal_ocupado"]
        assert (pessoal["unidade"] == "Pessoas").all()
        tratores = df[df["variavel"] == "tratores"]
        assert (tratores["unidade"] == "Unidades").all()

    def test_efetivo_animais_unidade_por_categoria(self):
        df = _parse_censo_historico_raw(_mock_sidra_efetivo_animais(), "efetivo_animais")
        bovinos = df[df["categoria"] == "bovinos"]
        assert (bovinos["unidade"] == "Cabeças").all()
        aves = df[df["categoria"] == "aves"]
        assert (aves["unidade"] == "Mil cabeças").all()

    def test_efetivo_animais_not_using_mn(self):
        df = _parse_censo_historico_raw(_mock_sidra_efetivo_animais(), "efetivo_animais")
        aves = df[df["categoria"] == "aves"]
        assert (aves["unidade"] == "Mil cabeças").all()
        assert (aves["unidade"] != "Cabeças").all()

    def test_missing_values_coerced_to_nan(self):
        df = _parse_censo_historico_raw(_mock_sidra_missing_values(), "pessoal_tratores")
        assert df["valor"].isna().all()

    def test_empty_df_returns_empty_with_schema(self):
        df = _parse_censo_historico_raw(pd.DataFrame(), "estabelecimentos_area")
        assert list(df.columns) == EXPECTED_COLS
        assert len(df) == 0

    def test_producao_animal_unidade_por_categoria(self):
        df = _parse_censo_historico_raw(_mock_sidra_producao_animal(), "producao_animal")
        leite = df[df["categoria"] == "leite_vaca"]
        assert (leite["unidade"] == "Mil litros").all()
        ovos = df[df["categoria"] == "ovos_galinha"]
        assert (ovos["unidade"] == "Mil dúzias").all()

    def test_valor_numeric(self):
        df = _parse_censo_historico_raw(
            _mock_sidra_estabelecimentos_area(), "estabelecimentos_area"
        )
        assert df["valor"].iloc[0] == 5801809.0
        assert df["valor"].iloc[1] == 234567.0

    @pytest.mark.asyncio
    @patch("agrobr.ibge.client.fetch_sidra", new_callable=AsyncMock)
    async def test_return_meta(self, mock_fetch):
        mock_fetch.return_value = _mock_sidra_pessoal_tratores()
        result = await censo_agro_historico("pessoal_tratores", ano=1985, return_meta=True)
        assert isinstance(result, tuple)
        df, meta = result
        assert list(df.columns) == EXPECTED_COLS
        assert meta.source == "ibge_censo_agro_historico"
        assert meta.records_count == len(df)

    @pytest.mark.asyncio
    @patch("agrobr.ibge.client.fetch_sidra", new_callable=AsyncMock)
    async def test_as_polars(self, mock_fetch):
        mock_fetch.return_value = _mock_sidra_pessoal_tratores()
        pl = pytest.importorskip("polars")

        result = await censo_agro_historico("pessoal_tratores", ano=1985, as_polars=True)

        assert isinstance(result, pl.DataFrame)

    @pytest.mark.asyncio
    @patch("agrobr.ibge.client.fetch_sidra", new_callable=AsyncMock)
    async def test_sorted_by_ano_localidade(self, mock_fetch):
        df_raw = pd.DataFrame(
            {
                "NC": ["3", "3", "3", "3"],
                "NN": ["UF"] * 4,
                "MC": ["5512"] * 4,
                "MN": ["Pessoas"] * 4,
                "V": ["100", "200", "300", "400"],
                "D1C": ["43", "35", "43", "35"],
                "D1N": [
                    "Rio Grande do Sul",
                    "São Paulo",
                    "Rio Grande do Sul",
                    "São Paulo",
                ],
                "D2C": ["185", "185", "185", "185"],
                "D2N": ["Pessoal"] * 4,
                "D3C": ["1985", "1980", "1980", "1985"],
                "D3N": ["1985", "1980", "1980", "1985"],
            }
        )
        mock_fetch.return_value = df_raw
        df = await censo_agro_historico("pessoal_tratores", ano=[1980, 1985])
        assert df["ano"].iloc[0] == 1980
        assert df["ano"].iloc[-1] == 1985

    @pytest.mark.asyncio
    @patch("agrobr.ibge.client.fetch_sidra", new_callable=AsyncMock)
    async def test_fetch_sidra_called_with_correct_params(self, mock_fetch):
        mock_fetch.return_value = pd.DataFrame()
        await censo_agro_historico("estabelecimentos_area", ano=1985, nivel="brasil")
        mock_fetch.assert_called_once()
        call_kwargs = mock_fetch.call_args
        assert call_kwargs.kwargs["table_code"] == "263"
        assert call_kwargs.kwargs["territorial_level"] == "1"
        assert call_kwargs.kwargs["period"] == "1985"
        assert "183" in call_kwargs.kwargs["variable"]

    @pytest.mark.asyncio
    @patch("agrobr.ibge.client.fetch_sidra", new_callable=AsyncMock)
    async def test_uf_filter(self, mock_fetch):
        mock_fetch.return_value = pd.DataFrame()
        await censo_agro_historico("pessoal_tratores", ano=1985, uf="SP", nivel="uf")
        call_kwargs = mock_fetch.call_args
        assert call_kwargs.kwargs["ibge_territorial_code"] == "35"

    @pytest.mark.asyncio
    @patch("agrobr.ibge.client.fetch_sidra", new_callable=AsyncMock)
    async def test_uf_ignored_for_brasil(self, mock_fetch):
        mock_fetch.return_value = pd.DataFrame()
        await censo_agro_historico("pessoal_tratores", ano=1985, uf="SP", nivel="brasil")
        call_kwargs = mock_fetch.call_args
        assert call_kwargs.kwargs["ibge_territorial_code"] == "all"

    @pytest.mark.asyncio
    @patch("agrobr.ibge.client.fetch_sidra", new_callable=AsyncMock)
    async def test_pessoal_tratores_no_classifications(self, mock_fetch):
        mock_fetch.return_value = pd.DataFrame()
        await censo_agro_historico("pessoal_tratores", ano=1985)
        call_kwargs = mock_fetch.call_args
        assert call_kwargs.kwargs.get("classifications") is None


# ---------------------------------------------------------------------------
# Bloco 3: testes de contrato e dataset
# ---------------------------------------------------------------------------


class TestCensoHistoricoContract:
    def test_contract_registered(self):
        assert has_contract("censo_agropecuario_historico")

    def test_contract_retrievable(self):
        contract = get_contract("censo_agropecuario_historico")
        assert contract is IBGE_CENSO_AGRO_HISTORICO_V1

    def test_contract_has_9_columns(self):
        assert len(IBGE_CENSO_AGRO_HISTORICO_V1.columns) == 9

    def test_contract_column_names(self):
        expected = [
            "ano",
            "localidade",
            "localidade_cod",
            "tema",
            "categoria",
            "variavel",
            "valor",
            "unidade",
            "fonte",
        ]
        assert IBGE_CENSO_AGRO_HISTORICO_V1.list_columns() == expected

    def test_primary_key(self):
        assert IBGE_CENSO_AGRO_HISTORICO_V1.primary_key == [
            "ano",
            "tema",
            "categoria",
            "variavel",
            "localidade",
        ]

    def test_ano_min_value_1920(self):
        ano_col = IBGE_CENSO_AGRO_HISTORICO_V1.get_column("ano")
        assert ano_col is not None
        assert ano_col.min_value == 1920

    def test_fonte_column_not_nullable(self):
        fonte_col = IBGE_CENSO_AGRO_HISTORICO_V1.get_column("fonte")
        assert fonte_col is not None
        assert fonte_col.nullable is False

    def test_valor_nullable(self):
        valor_col = IBGE_CENSO_AGRO_HISTORICO_V1.get_column("valor")
        assert valor_col is not None
        assert valor_col.nullable is True
        assert valor_col.min_value == 0

    def test_effective_from(self):
        assert IBGE_CENSO_AGRO_HISTORICO_V1.effective_from == "0.13.0"

    def test_version(self):
        assert IBGE_CENSO_AGRO_HISTORICO_V1.version == "1.0"

    def test_guarantees_mention_census_years(self):
        years_guarantee = [g for g in IBGE_CENSO_AGRO_HISTORICO_V1.guarantees if "1920" in g]
        assert len(years_guarantee) == 1

    def test_guarantees_mention_fonte(self):
        fonte_guarantee = [
            g for g in IBGE_CENSO_AGRO_HISTORICO_V1.guarantees if "ibge_censo_agro_historico" in g
        ]
        assert len(fonte_guarantee) == 1

    def test_validates_correct_df(self):
        df = pd.DataFrame(
            {
                "ano": pd.array([1985], dtype="Int64"),
                "localidade": ["São Paulo"],
                "localidade_cod": pd.array([35], dtype="Int64"),
                "tema": ["estabelecimentos_area"],
                "categoria": ["total"],
                "variavel": ["estabelecimentos"],
                "valor": [5801809.0],
                "unidade": ["Unidades"],
                "fonte": ["ibge_censo_agro_historico"],
            }
        )
        valid, errors = IBGE_CENSO_AGRO_HISTORICO_V1.validate(df)
        assert valid, errors

    def test_rejects_negative_valor(self):
        df = pd.DataFrame(
            {
                "ano": pd.array([1985], dtype="Int64"),
                "localidade": ["São Paulo"],
                "localidade_cod": pd.array([35], dtype="Int64"),
                "tema": ["estabelecimentos_area"],
                "categoria": ["total"],
                "variavel": ["estabelecimentos"],
                "valor": [-1.0],
                "unidade": ["Unidades"],
                "fonte": ["ibge_censo_agro_historico"],
            }
        )
        valid, errors = IBGE_CENSO_AGRO_HISTORICO_V1.validate(df)
        assert not valid


class TestCensoHistoricoDataset:
    def test_dataset_info_name(self):
        assert CENSO_AGROPECUARIO_HISTORICO_INFO.name == "censo_agropecuario_historico"

    def test_dataset_has_9_products(self):
        assert len(CENSO_AGROPECUARIO_HISTORICO_INFO.products) == 9

    def test_dataset_products_match_temas(self):
        assert set(CENSO_AGROPECUARIO_HISTORICO_INFO.products) == set(client.TEMAS_CENSO_HISTORICO)

    def test_update_frequency_never(self):
        assert CENSO_AGROPECUARIO_HISTORICO_INFO.update_frequency == "never"

    def test_license_livre(self):
        assert CENSO_AGROPECUARIO_HISTORICO_INFO.license == "livre"

    def test_source_institution(self):
        assert CENSO_AGROPECUARIO_HISTORICO_INFO.source_institution == "IBGE"

    def test_source_url(self):
        assert "sidra.ibge.gov.br" in CENSO_AGROPECUARIO_HISTORICO_INFO.source_url

    def test_has_one_source(self):
        assert len(CENSO_AGROPECUARIO_HISTORICO_INFO.sources) == 1
        assert CENSO_AGROPECUARIO_HISTORICO_INFO.sources[0].name == "ibge_censo_agro_historico"

    def test_registered_in_registry(self):
        from agrobr.datasets.registry import list_datasets

        datasets = list_datasets()
        assert "censo_agropecuario_historico" in datasets

    @pytest.mark.asyncio
    @patch("agrobr.ibge.client.fetch_sidra", new_callable=AsyncMock)
    async def test_fetch_via_dataset(self, mock_fetch):
        from agrobr.datasets.censo_agropecuario_historico import censo_agropecuario_historico

        mock_fetch.return_value = _mock_sidra_pessoal_tratores()
        df = await censo_agropecuario_historico("pessoal_tratores", nivel="uf")
        assert list(df.columns) == EXPECTED_COLS

    @pytest.mark.asyncio
    @patch("agrobr.ibge.client.fetch_sidra", new_callable=AsyncMock)
    async def test_fetch_via_dataset_with_meta(self, mock_fetch):
        from agrobr.datasets.censo_agropecuario_historico import censo_agropecuario_historico

        mock_fetch.return_value = _mock_sidra_pessoal_tratores()
        result = await censo_agropecuario_historico(
            "pessoal_tratores", nivel="uf", return_meta=True
        )
        assert isinstance(result, tuple)
        df, meta = result
        assert list(df.columns) == EXPECTED_COLS
        assert "censo_agropecuario_historico" in meta.source


@pytest.mark.integration
class TestCensoHistoricoIntegration:
    @pytest.mark.asyncio
    async def test_estabelecimentos_area_1985_brasil(self):
        df = await censo_agro_historico("estabelecimentos_area", ano=1985, nivel="brasil")
        assert list(df.columns) == EXPECTED_COLS
        assert not df.empty
        assert (df["ano"] == 1985).all()
        assert (df["fonte"] == "ibge_censo_agro_historico").all()

        estab_total = df[(df["variavel"] == "estabelecimentos") & (df["categoria"] == "total")][
            "valor"
        ]
        if not estab_total.empty:
            val = estab_total.iloc[0]
            assert 1_000_000 < val < 10_000_000, f"Expected ~5.8M, got {val}"
