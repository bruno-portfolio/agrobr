from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from agrobr import ibge
from agrobr.exceptions import InvalidParameterError
from agrobr.ibge import client
from agrobr.utils import time as time_utils
from tests import helpers

SIDRA_SEM_OBSERVACOES = pd.DataFrame(
    columns=["NC", "NN", "MC", "MN", "V", "D1C", "D1N", "D2C", "D2N", "D3C", "D3N"]
)
ABATE_MT_202303 = pd.DataFrame(
    [
        {
            "NC": "3",
            "NN": "Unidade da Federação",
            "MC": "24",
            "MN": "Cabeças",
            "V": "100",
            "D1C": "51",
            "D1N": "Mato Grosso",
            "D2C": "284",
            "D2N": "Animais abatidos",
            "D3C": "202303",
            "D3N": "3º trimestre 2023",
        },
        {
            "NC": "3",
            "NN": "Unidade da Federação",
            "MC": "1007",
            "MN": "Quilogramas",
            "V": "25000",
            "D1C": "51",
            "D1N": "Mato Grosso",
            "D2C": "285",
            "D2N": "Peso total das carcaças",
            "D3C": "202303",
            "D3N": "3º trimestre 2023",
        },
    ]
)
UFS_IBGE = [
    "RO",
    "AC",
    "AM",
    "RR",
    "PA",
    "AP",
    "TO",
    "MA",
    "PI",
    "CE",
    "RN",
    "PB",
    "PE",
    "AL",
    "SE",
    "BA",
    "MG",
    "ES",
    "RJ",
    "SP",
    "PR",
    "SC",
    "RS",
    "MS",
    "MT",
    "GO",
    "DF",
]


class TestPamValidation:
    """Testes de validacao da funcao PAM."""

    async def test_validacao_pam(self):
        cases = [
            (
                "test_pam_produto_invalido",
                ibge.pam,
                ("produto_inexistente",),
                {},
                ValueError,
                "Produto não suportado",
            ),
            (
                "test_pam_ano_invalido_antes_da_rede",
                ibge.pam,
                ("soja",),
                {"ano": 1973},
                InvalidParameterError,
                "ano",
            ),
            (
                "test_pam_ano_invalido_antes_da_rede",
                ibge.pam,
                ("soja",),
                {"ano": 9999},
                InvalidParameterError,
                "ano",
            ),
            (
                "test_pam_ano_invalido_antes_da_rede",
                ibge.pam,
                ("soja",),
                {"ano": "invalido"},
                InvalidParameterError,
                "ano",
            ),
            (
                "test_pam_ano_invalido_antes_da_rede",
                ibge.pam,
                ("soja",),
                {"ano": 2023.5},
                InvalidParameterError,
                "ano",
            ),
        ]
        with helpers.collect_failures() as check:
            for case, function, args, kwargs, exception, message in cases:
                with check(case), helpers.isolated_dataset_case((case, kwargs)) as monkeypatch:
                    fetch = AsyncMock()
                    monkeypatch.setattr(client, "fetch_sidra", fetch)
                    with pytest.raises(exception, match=message):
                        await function(*args, **kwargs)
                    fetch.assert_not_awaited()


class TestPolarsSupport:
    """Testes do suporte a Polars."""

    @pytest.fixture
    def mock_response(self):
        """Resposta mockada."""
        return pd.DataFrame(
            {
                "NC": ["3"],
                "NN": ["UF"],
                "MC": ["51"],
                "MN": ["Mato Grosso"],
                "V": ["15000"],
                "D1N": ["2023"],
                "D2N": ["Área plantada"],
            }
        )

    @pytest.mark.asyncio
    async def test_pam_polars_missing_raises(self, mock_response, monkeypatch):
        import builtins

        real_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name == "polars":
                raise ImportError("No module named 'polars'")
            return real_import(name, *args, **kwargs)

        with patch.object(client, "fetch_sidra", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = mock_response

            monkeypatch.setattr(builtins, "__import__", mock_import)

            with pytest.raises(ImportError, match=r"pip install agrobr\[polars\]"):
                await ibge.pam("soja", ano=2023, as_polars=True)


class TestLspaValidation:
    async def test_validacao_lspa(self):
        cases = [("test_lspa_produto_invalido", {"produto": "xyz"}, "Produto não suportado")]
        cases += [
            ("test_lspa_mes_invalido_nao_consulta_outra_referencia", {"mes": mes}, "mes")
            for mes in [0, 13, -1, "0", "13", "", "abc", "7.9", 7.9, True, False]
        ]
        with helpers.collect_failures() as check:
            for case, kwargs, message in cases:
                selection = {"produto": "cafe", "ano": 2026, **kwargs}
                with check((case, kwargs)), helpers.isolated_dataset_case(case) as monkeypatch:
                    fetch = AsyncMock()
                    monkeypatch.setattr(client, "fetch_sidra", fetch)
                    with pytest.raises(InvalidParameterError, match=message):
                        await ibge.lspa(**selection)
                    fetch.assert_not_awaited()


CONSULTA_EM_15_07_2026 = datetime(2026, 7, 15, 15, 0, tzinfo=UTC)


async def test_consulta_sidra_por_selecao():
    cases = [
        ("ppm_sem_ano", ibge.ppm, ("bovino",), {}, {"table_code": "3939", "period": "last"}),
        (
            "abate_sem_trimestre",
            ibge.abate,
            ("bovino",),
            {},
            {"table_code": "1092", "period": "last"},
        ),
        (
            "abate_uf",
            ibge.abate,
            ("bovino",),
            {"trimestre": "202303", "uf": "MT"},
            {"ibge_territorial_code": "51"},
        ),
        (
            "abate_suino_sem_classificacao_bovina",
            ibge.abate,
            ("suino",),
            {"trimestre": "202303"},
            {"table_code": "1093", "classifications": {"12716": "115236", "12529": "118225"}},
        ),
        (
            "leite_uf",
            ibge.leite_trimestral,
            (),
            {"trimestre": "202503", "uf": "MG"},
            {"table_code": "1086", "ibge_territorial_code": "31"},
        ),
        (
            "pib_precos_reais",
            ibge.pib_agro,
            (),
            {"precos": "real_1995"},
            {"table_code": "6612", "variable": "9318"},
        ),
        (
            "ppm_producao_origem_animal",
            ibge.ppm,
            ("leite",),
            {"ano": 2023},
            {"table_code": "74", "variable": "106", "classifications": {"80": "2682"}},
        ),
        (
            "silvicultura_area",
            ibge.silvicultura,
            ("eucalipto",),
            {"ano": 2023, "variavel": "area"},
            {"table_code": "5930", "variable": "6549", "classifications": {"734": "39326"}},
        ),
        (
            "pam_variavel_explicita",
            ibge.pam,
            ("soja",),
            {"ano": 2023, "variaveis": ["producao"]},
            {"variable": "214"},
        ),
        (
            "historico_lista_de_anos",
            ibge.censo_agro_historico,
            ("pessoal_tratores",),
            {"ano": [1980, 1985]},
            {"period": "1980,1985"},
        ),
        ("lspa_sem_ano_usa_ano_corrente", ibge.lspa, ("soja",), {"mes": 7}, {"period": "202607"}),
    ]
    with helpers.collect_failures() as check:
        for case, function, args, kwargs, expected in cases:
            response = ABATE_MT_202303 if function is ibge.abate else SIDRA_SEM_OBSERVACOES
            with check(case), helpers.isolated_dataset_case(case) as monkeypatch:
                fetch = AsyncMock(return_value=response.copy())
                monkeypatch.setattr(client, "fetch_sidra", fetch)
                monkeypatch.setattr(time_utils, "utcnow_aware", lambda: CONSULTA_EM_15_07_2026)
                await function(*args, **kwargs)
                sent = fetch.await_args.kwargs
                assert {key: sent[key] for key in expected} == expected


async def test_resultado_vazio_preserva_colunas():
    censo = ["ano", "localidade", "localidade_cod", "tema", "categoria", "variavel"]
    cases = [
        (
            "pam",
            ibge.pam,
            ("soja",),
            {"ano": 2023},
            [
                "ano",
                "localidade",
                "localidade_cod",
                "produto",
                "area_plantada",
                "area_colhida",
                "producao",
                "rendimento",
                "valor_producao",
                "fonte",
                "unidade_producao",
                "unidade_rendimento",
                "unidade_valor_producao",
                "condicao_produto",
            ],
        ),
        (
            "lspa",
            ibge.lspa,
            ("soja",),
            {"ano": 2026, "mes": 7},
            [
                "ano",
                "mes",
                "localidade",
                "localidade_cod",
                "produto",
                "variavel",
                "variavel_cod",
                "valor",
                "unidade",
                "fonte",
            ],
        ),
        (
            "censo_multitabela_1995",
            ibge.censo_agro,
            ("uso_terra",),
            {"ano": 1995},
            [*censo, "valor", "unidade", "fonte"],
        ),
    ]
    with helpers.collect_failures() as check:
        for case, function, args, kwargs, columns in cases:
            with check(case), helpers.isolated_dataset_case(case) as monkeypatch:
                monkeypatch.setattr(client, "fetch_sidra", AsyncMock(return_value=pd.DataFrame()))
                frame = await function(*args, **kwargs)
                assert list(frame.columns) == columns
                assert frame.empty


async def test_catalogos_publicos():
    cases = [
        ("ufs", ibge.ufs, UFS_IBGE),
        ("especies_abate", ibge.especies_abate, ["bovino", "suino", "frango"]),
        (
            "especies_ppm",
            ibge.especies_ppm,
            [
                "bovino",
                "bubalino",
                "caprino",
                "casulos",
                "codornas",
                "equino",
                "galinaceos_total",
                "galinhas",
                "la",
                "leite",
                "mel",
                "ovino",
                "ovos_codorna",
                "ovos_galinha",
                "suino_matrizes",
                "suino_total",
            ],
        ),
        (
            "temas_censo_agro",
            ibge.temas_censo_agro,
            [
                "efetivo_rebanho",
                "uso_terra",
                "lavoura_temporaria",
                "lavoura_permanente",
                "preparo_solo",
                "adubacao",
                "calagem",
                "agrotoxicos",
                "praticas_agricolas",
                "irrigacao",
                "despesa_adubos",
            ],
        ),
        (
            "temas_censo_agro_historico",
            ibge.temas_censo_agro_historico,
            [
                "estabelecimentos_area",
                "uso_terra",
                "pessoal_tratores",
                "condicao_produtor",
                "efetivo_animais",
                "producao_animal",
                "producao_vegetal",
                "lavoura_permanente",
                "lavoura_temporaria",
            ],
        ),
        (
            "temas_censo_agro_legado",
            ibge.temas_censo_agro_legado,
            [
                "tecnologia",
                "pessoal_ocupado",
                "maquinas",
                "producao_animal",
                "valor_producao",
                "financeiro",
            ],
        ),
        (
            "especies_silvicultura_area",
            ibge.especies_silvicultura_area,
            ["eucalipto", "pinus", "outras"],
        ),
        (
            "produtos_extracao_vegetal",
            ibge.produtos_extracao_vegetal,
            [
                "acai",
                "castanha_caju",
                "castanha_para",
                "erva_mate",
                "mangaba",
                "palmito",
                "pequi_fruto",
                "pinhao",
                "umbu",
                "hevea_coagulado",
                "hevea_liquido",
                "carnauba_cera",
                "carnauba_po",
                "piacava",
                "carvao",
                "lenha",
                "madeira_tora",
                "babacu",
                "copaiba",
                "cumaru",
                "pequi_amendoa",
            ],
        ),
    ]
    with helpers.collect_failures() as check:
        for case, function, expected in cases:
            with check(case):
                assert await function() == expected
        with check("produtos_silvicultura"):
            produtos = await ibge.produtos_silvicultura()
            assert len(produtos) == 14
            assert {
                "carvao",
                "lenha",
                "madeira_tora",
                "madeira_celulose",
                "acacia_negra",
                "eucalipto_folha",
                "resina",
            } <= set(produtos)
        with check("produtos_pam"):
            assert {"soja", "milho", "arroz", "feijao", "trigo", "cafe"} <= set(
                await ibge.produtos_pam()
            )
        with check("produtos_lspa"):
            produtos = await ibge.produtos_lspa()
            assert produtos[:6] == ["soja", "milho_1", "milho_2", "arroz", "feijao_1", "feijao_2"]
            assert {"milho", "feijao"} <= set(produtos)
