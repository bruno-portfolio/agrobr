from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
import xlrd

from agrobr.exceptions import InvalidParameterError
from agrobr.normalize import regions
from agrobr.normalize.municipalities import (
    buscar_municipios,
    coordenada_para_municipio,
    ibge_para_municipio,
    municipio_para_ibge,
    total_municipios,
)
from tests.helpers import collect_failures

DTB_2025 = Path(__file__).resolve().parents[1] / "golden_data/ibge/dtb_2025"


class TestTotalMunicipios:
    def test_total_is_above_5500(self):
        with collect_failures() as check:
            with check("test_total_is_above_5500"):
                assert total_municipios() >= 5500
            with check("test_total_is_below_6000"):
                assert total_municipios() < 6000


class TestMunicipioParaIbge:
    def test_accent_case_variations(self):
        with collect_failures() as check:
            for nome, uf, expected in [
                ("São Paulo", "SP", 3550308),
                ("Rio de Janeiro", "RJ", 3304557),
                ("Belo Horizonte", "MG", 3106200),
                ("Salvador", "BA", 2927408),
                ("Fortaleza", "CE", 2304400),
                ("Brasília", "DF", 5300108),
                ("Curitiba", "PR", 4106902),
                ("Manaus", "AM", 1302603),
                ("Recife", "PE", 2611606),
                ("Porto Alegre", "RS", 4314902),
                ("Belém", "PA", 1501402),
                ("Goiânia", "GO", 5208707),
                ("Campinas", "SP", 3509502),
                ("Guarulhos", "SP", 3518800),
                ("Cuiabá", "MT", 5103403),
                ("Campo Grande", "MS", 5002704),
                ("Florianópolis", "SC", 4205407),
                ("Vitória", "ES", 3205309),
                ("Natal", "RN", 2408102),
                ("João Pessoa", "PB", 2507507),
            ]:
                with check(f"test_capitais[{(nome, uf, expected)!r}]"):
                    assert municipio_para_ibge(nome, uf) == expected
            for nome, uf, expected in [
                ("Sorriso", "MT", 5107925),
                ("Lucas do Rio Verde", "MT", 5105259),
                ("Sinop", "MT", 5107909),
                ("Rondonópolis", "MT", 5107602),
                ("Cascavel", "PR", 4104808),
                ("Londrina", "PR", 4113700),
                ("Maringá", "PR", 4115200),
                ("Ribeirão Preto", "SP", 3543402),
                ("Uberlândia", "MG", 3170206),
                ("Rio Verde", "GO", 5218805),
                ("Dourados", "MS", 5003702),
                ("Luís Eduardo Magalhães", "BA", 2919553),
                ("Barreiras", "BA", 2903201),
                ("Paragominas", "PA", 1505502),
                ("Uberaba", "MG", 3170107),
                ("Chapecó", "SC", 4204202),
                ("Passo Fundo", "RS", 4314100),
                ("Santa Maria", "RS", 4316907),
                ("Presidente Prudente", "SP", 3541406),
                ("Piracicaba", "SP", 3538709),
            ]:
                with check(f"test_cidades_agro[{(nome, uf, expected)!r}]"):
                    assert municipio_para_ibge(nome, uf) == expected
            for nome, uf, expected in [
                ("SAO PAULO", "SP", 3550308),
                ("sao paulo", "SP", 3550308),
                ("São Paulo", "SP", 3550308),
                ("Sao Paulo", "SP", 3550308),
                ("BELO HORIZONTE", "MG", 3106200),
                ("belo horizonte", "MG", 3106200),
                ("CUIABA", "MT", 5103403),
                ("cuiabá", "MT", 5103403),
                ("FLORIANOPOLIS", "SC", 4205407),
                ("florianópolis", "SC", 4205407),
                ("GOIANIA", "GO", 5208707),
                ("goiânia", "GO", 5208707),
                ("BELEM", "PA", 1501402),
                ("belém", "PA", 1501402),
                ("BRASILIA", "DF", 5300108),
                ("brasília", "DF", 5300108),
                ("MARINGA", "PR", 4115200),
                ("maringá", "PR", 4115200),
                ("CHAPECO", "SC", 4204202),
                ("chapecó", "SC", 4204202),
                ("RONDONOPOLIS", "MT", 5107602),
                ("rondonópolis", "MT", 5107602),
                ("UBERLANDIA", "MG", 3170206),
                ("uberlândia", "MG", 3170206),
                ("LUIS EDUARDO MAGALHAES", "BA", 2919553),
                ("Luís Eduardo Magalhães", "BA", 2919553),
            ]:
                with check(f"test_accent_case_variations[{(nome, uf, expected)!r}]"):
                    assert municipio_para_ibge(nome, uf) == expected

    def test_not_found(self):
        with collect_failures() as check:
            with check("test_uf_disambiguation"):
                code_pr = municipio_para_ibge("Cascavel", "PR")
                code_ce = municipio_para_ibge("Cascavel", "CE")
                assert code_pr is not None
                assert code_ce is not None
                assert code_pr != code_ce
            with check("test_not_found"):
                assert municipio_para_ibge("CidadeInexistente123") is None
            with check("test_wrong_uf"):
                assert municipio_para_ibge("Sorriso", "SP") is None
            with check("test_whitespace_handling"):
                assert municipio_para_ibge("  São Paulo  ", "SP") == 3550308


class TestBuscarMunicipios:
    def test_busca_accent_insensitive(self):
        with collect_failures() as check:
            with check("test_busca_parcial"):
                results = buscar_municipios("sorri", uf="MT")
                assert len(results) >= 1
                assert any(m["nome"] == "Sorriso" for m in results)
            with check("test_busca_sem_uf"):
                results = buscar_municipios("campinas")
                assert len(results) >= 1
            with check("test_busca_com_limite"):
                results = buscar_municipios("santo", limite=5)
                assert len(results) <= 5
            with check("test_busca_vazia"):
                results = buscar_municipios("zzzinexistente999")
                assert results == []
            with check("test_busca_accent_insensitive"):
                results = buscar_municipios("goiania", uf="GO")
                assert any(m["nome"] == "Goiânia" for m in results)


class TestCoordenadaParaMunicipio:
    def test_capitais(self):
        with collect_failures() as check:
            for lat, lon, expected_nome, expected_uf in [
                (-15.7801, -47.9292, "Brasília", "DF"),
                (-23.5505, -46.6333, "São Paulo", "SP"),
                (-22.93, -43.46, "Rio de Janeiro", "RJ"),
                (-12.9714, -38.5124, "Salvador", "BA"),
                (-2.63, -60.26, "Manaus", "AM"),
                (-25.4284, -49.2733, "Curitiba", "PR"),
                (-19.9167, -43.9345, "Belo Horizonte", "MG"),
                (-8.04, -34.93, "Recife", "PE"),
                (-30.0346, -51.2177, "Porto Alegre", "RS"),
                (-15.51, -55.88, "Cuiabá", "MT"),
            ]:
                with check(f"test_capitais[{(lat, lon, expected_nome, expected_uf)!r}]"):
                    info = coordenada_para_municipio(lat, lon)
                    assert info is not None
                    assert info["nome"] == expected_nome
                    assert info["uf"] == expected_uf
            for lat, lon, expected_nome, expected_uf in [
                (-12.74, -55.68, "Sorriso", "MT"),
                (-13.05, -55.91, "Lucas do Rio Verde", "MT"),
                (-17.7928, -50.9297, "Rio Verde", "GO"),
                (-12.0964, -45.7897, "Luís Eduardo Magalhães", "BA"),
                (-11.87, -55.51, "Sinop", "MT"),
            ]:
                with check(f"test_cidades_agro[{(lat, lon, expected_nome, expected_uf)!r}]"):
                    info = coordenada_para_municipio(lat, lon)
                    assert info is not None
                    assert info["nome"] == expected_nome
                    assert info["uf"] == expected_uf

    @pytest.mark.parametrize(
        "lat,lon",
        [
            (0, -30),
            (-40, -60),
            (10, -80),
        ],
        ids=["atlantic_equator", "south_atlantic", "caribbean"],
    )
    def test_oceano_retorna_none(self, lat, lon):
        assert coordenada_para_municipio(lat, lon) is None

    def test_consistencia_com_ibge_para_municipio(self):
        info = coordenada_para_municipio(-23.5505, -46.6333)
        assert info is not None
        reverse = ibge_para_municipio(info["codigo_ibge"])
        assert reverse is not None
        assert reverse["nome"] == info["nome"]
        assert reverse["uf"] == info["uf"]


def _municipios_dtb() -> dict[int, tuple[str, int, str]]:
    procedencia = json.loads((DTB_2025 / "PROVENANCE.json").read_text(encoding="utf-8"))
    arquivo = procedencia["files"][0]
    conteudo = (DTB_2025 / arquivo["file"]).read_bytes()
    assert hashlib.sha256(conteudo).hexdigest() == arquivo["sha256"]
    planilha = xlrd.open_workbook(file_contents=conteudo).sheet_by_index(0)
    cabecalho = [planilha.cell_value(6, coluna) for coluna in range(planilha.ncols)]
    assert (cabecalho[0], cabecalho[1], cabecalho[7], cabecalho[8]) == (
        "UF",
        "Nome_UF",
        "Código Município Completo",
        "Nome_Município",
    )
    return {
        int(planilha.cell_value(linha, 7)): (
            planilha.cell_value(linha, 8),
            int(planilha.cell_value(linha, 0)),
            planilha.cell_value(linha, 1),
        )
        for linha in range(7, planilha.nrows)
    }


def test_tabela_de_municipios_confere_a_dtb_2025():
    dtb = _municipios_dtb()
    assert len(dtb) == total_municipios() == 5571
    divergentes = []
    for codigo, (nome, codigo_uf, _) in dtb.items():
        info = ibge_para_municipio(codigo)
        if info is None or info["nome"] != nome or regions.uf_para_ibge(info["uf"]) != codigo_uf:
            divergentes.append((codigo, nome, codigo_uf, info))
    assert divergentes == []


def test_ufs_conferem_a_dtb_2025():
    ufs = {codigo_uf: nome_uf for _, codigo_uf, nome_uf in _municipios_dtb().values()}
    assert {dados["ibge"]: dados["nome"] for dados in regions.UFS.values()} == ufs


@pytest.mark.parametrize(
    ("nome", "uf", "codigo"),
    [
        ("Assú", "RN", 2400208),
        ("Açu", "RN", 2400208),
        ("Arez", "RN", 2401206),
        ("Arês", "RN", 2401206),
        ("São Luiz do Anauá", "RR", 1400605),
        ("São Luiz", "RR", 1400605),
    ],
)
def test_nome_atual_e_nome_anterior_encontram_o_municipio(nome, uf, codigo):
    assert municipio_para_ibge(nome, uf) == codigo


@pytest.mark.parametrize(
    ("termo", "uf", "codigo"),
    [("São Luiz", "RR", 1400605), ("a", "RN", 2400208), ("are", "RN", 2401206)],
)
def test_busca_que_casa_nome_atual_e_anterior_devolve_o_municipio_uma_vez(termo, uf, codigo):
    nome, codigo_uf, _ = _municipios_dtb()[codigo]
    resultados = buscar_municipios(termo, uf=uf, limite=total_municipios())
    assert [m for m in resultados if m["codigo_ibge"] == codigo] == [
        {"codigo_ibge": codigo, "nome": nome, "uf": uf}
    ]
    assert regions.uf_para_ibge(uf) == codigo_uf
    assert len({m["codigo_ibge"] for m in resultados}) == len(resultados)


@pytest.mark.parametrize(
    ("kwargs", "motivo"),
    [
        ({"uf": "ZZ"}, "UF inválida"),
        ({"uf": ""}, "UF inválida"),
        ({"limite": -1}, "limite deve ser inteiro não negativo"),
        ({"limite": True}, "limite deve ser inteiro não negativo"),
    ],
)
def test_buscar_municipios_recusa_filtro_invalido(kwargs, motivo):
    with pytest.raises(InvalidParameterError, match=motivo):
        buscar_municipios("sao", **kwargs)


@pytest.mark.parametrize(
    "consulta",
    [
        lambda: buscar_municipios("sorriso", uf="MT")[0],
        lambda: ibge_para_municipio(5107925),
        lambda: coordenada_para_municipio(-12.5425, -55.7211),
    ],
    ids=["buscar_municipios", "ibge_para_municipio", "coordenada_para_municipio"],
)
def test_retorno_nao_compartilha_o_indice(consulta):
    consulta()["nome"] = "Alterado"
    assert consulta()["nome"] == "Sorriso"
