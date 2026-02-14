from __future__ import annotations

import pytest

from agrobr.normalize.municipalities import (
    buscar_municipios,
    ibge_para_municipio,
    municipio_para_ibge,
    total_municipios,
)


class TestTotalMunicipios:
    def test_total_is_above_5500(self):
        assert total_municipios() >= 5500

    def test_total_is_below_6000(self):
        assert total_municipios() < 6000


class TestMunicipioParaIbge:
    @pytest.mark.parametrize(
        "nome,uf,expected",
        [
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
        ],
        ids=[
            "sao_paulo",
            "rio_de_janeiro",
            "belo_horizonte",
            "salvador",
            "fortaleza",
            "brasilia",
            "curitiba",
            "manaus",
            "recife",
            "porto_alegre",
            "belem",
            "goiania",
            "campinas",
            "guarulhos",
            "cuiaba",
            "campo_grande",
            "florianopolis",
            "vitoria",
            "natal",
            "joao_pessoa",
        ],
    )
    def test_capitais(self, nome, uf, expected):
        assert municipio_para_ibge(nome, uf) == expected

    @pytest.mark.parametrize(
        "nome,uf,expected",
        [
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
        ],
        ids=[
            "sorriso",
            "lucas_rio_verde",
            "sinop",
            "rondonopolis",
            "cascavel",
            "londrina",
            "maringa",
            "ribeirao_preto",
            "uberlandia",
            "rio_verde",
            "dourados",
            "luis_eduardo",
            "barreiras",
            "paragominas",
            "uberaba",
            "chapeco",
            "passo_fundo",
            "santa_maria",
            "presidente_prudente",
            "piracicaba",
        ],
    )
    def test_cidades_agro(self, nome, uf, expected):
        assert municipio_para_ibge(nome, uf) == expected

    @pytest.mark.parametrize(
        "nome,uf,expected",
        [
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
        ],
        ids=[
            "upper_sao_paulo",
            "lower_sao_paulo",
            "accent_sao_paulo",
            "no_accent_sao_paulo",
            "upper_bh",
            "lower_bh",
            "upper_cuiaba",
            "accent_cuiaba",
            "upper_floripa",
            "accent_floripa",
            "upper_goiania",
            "accent_goiania",
            "upper_belem",
            "accent_belem",
            "upper_brasilia",
            "accent_brasilia",
            "upper_maringa",
            "accent_maringa",
            "upper_chapeco",
            "accent_chapeco",
            "upper_rondonopolis",
            "accent_rondonopolis",
            "upper_uberlandia",
            "accent_uberlandia",
            "upper_luis_eduardo",
            "accent_luis_eduardo",
        ],
    )
    def test_accent_case_variations(self, nome, uf, expected):
        assert municipio_para_ibge(nome, uf) == expected

    def test_uf_disambiguation(self):
        code_pr = municipio_para_ibge("Cascavel", "PR")
        code_ce = municipio_para_ibge("Cascavel", "CE")
        assert code_pr is not None
        assert code_ce is not None
        assert code_pr != code_ce

    def test_not_found(self):
        assert municipio_para_ibge("CidadeInexistente123") is None

    def test_wrong_uf(self):
        assert municipio_para_ibge("Sorriso", "SP") is None

    def test_whitespace_handling(self):
        assert municipio_para_ibge("  São Paulo  ", "SP") == 3550308


class TestIbgeParaMunicipio:
    def test_known_code(self):
        info = ibge_para_municipio(3550308)
        assert info is not None
        assert info["nome"] == "São Paulo"
        assert info["uf"] == "SP"
        assert info["codigo_ibge"] == 3550308

    def test_sorriso(self):
        info = ibge_para_municipio(5107925)
        assert info is not None
        assert info["nome"] == "Sorriso"
        assert info["uf"] == "MT"

    def test_unknown_code(self):
        assert ibge_para_municipio(9999999) is None


class TestBuscarMunicipios:
    def test_busca_parcial(self):
        results = buscar_municipios("sorri", uf="MT")
        assert len(results) >= 1
        assert any(m["nome"] == "Sorriso" for m in results)

    def test_busca_sem_uf(self):
        results = buscar_municipios("campinas")
        assert len(results) >= 1

    def test_busca_com_limite(self):
        results = buscar_municipios("santo", limite=5)
        assert len(results) <= 5

    def test_busca_vazia(self):
        results = buscar_municipios("zzzinexistente999")
        assert results == []

    def test_busca_accent_insensitive(self):
        results = buscar_municipios("goiania", uf="GO")
        assert any(m["nome"] == "Goiânia" for m in results)


class TestMunicipios100Amostra:
    AMOSTRA = [
        ("São Paulo", "SP"),
        ("Rio de Janeiro", "RJ"),
        ("Belo Horizonte", "MG"),
        ("Salvador", "BA"),
        ("Fortaleza", "CE"),
        ("Brasília", "DF"),
        ("Curitiba", "PR"),
        ("Manaus", "AM"),
        ("Recife", "PE"),
        ("Porto Alegre", "RS"),
        ("Belém", "PA"),
        ("Goiânia", "GO"),
        ("Campinas", "SP"),
        ("Guarulhos", "SP"),
        ("Cuiabá", "MT"),
        ("Campo Grande", "MS"),
        ("Florianópolis", "SC"),
        ("Vitória", "ES"),
        ("Natal", "RN"),
        ("João Pessoa", "PB"),
        ("Sorriso", "MT"),
        ("Lucas do Rio Verde", "MT"),
        ("Sinop", "MT"),
        ("Rondonópolis", "MT"),
        ("Cascavel", "PR"),
        ("Londrina", "PR"),
        ("Maringá", "PR"),
        ("Ribeirão Preto", "SP"),
        ("Uberlândia", "MG"),
        ("Rio Verde", "GO"),
        ("Dourados", "MS"),
        ("Luís Eduardo Magalhães", "BA"),
        ("Barreiras", "BA"),
        ("Paragominas", "PA"),
        ("Uberaba", "MG"),
        ("Chapecó", "SC"),
        ("Passo Fundo", "RS"),
        ("Santa Maria", "RS"),
        ("Presidente Prudente", "SP"),
        ("Piracicaba", "SP"),
        ("Feira de Santana", "BA"),
        ("Juiz de Fora", "MG"),
        ("Joinville", "SC"),
        ("Sorocaba", "SP"),
        ("Osasco", "SP"),
        ("Santo André", "SP"),
        ("São José dos Campos", "SP"),
        ("Ribeirão das Neves", "MG"),
        ("Aparecida de Goiânia", "GO"),
        ("Maceió", "AL"),
        ("São Luís", "MA"),
        ("Teresina", "PI"),
        ("Aracaju", "SE"),
        ("Palmas", "TO"),
        ("Macapá", "AP"),
        ("Rio Branco", "AC"),
        ("Porto Velho", "RO"),
        ("Boa Vista", "RR"),
        ("Jundiaí", "SP"),
        ("Niterói", "RJ"),
        ("Santos", "SP"),
        ("São José do Rio Preto", "SP"),
        ("Caxias do Sul", "RS"),
        ("Pelotas", "RS"),
        ("Canoas", "RS"),
        ("Foz do Iguaçu", "PR"),
        ("Ponta Grossa", "PR"),
        ("Blumenau", "SC"),
        ("São José", "SC"),
        ("Criciúma", "SC"),
        ("Anápolis", "GO"),
        ("Itumbiara", "GO"),
        ("Jataí", "GO"),
        ("Tangará da Serra", "MT"),
        ("Primavera do Leste", "MT"),
        ("Nova Mutum", "MT"),
        ("Sapezal", "MT"),
        ("Formosa", "GO"),
        ("Catalão", "GO"),
        ("Itajaí", "SC"),
        ("Toledo", "PR"),
        ("Patos de Minas", "MG"),
        ("Montes Claros", "MG"),
        ("Divinópolis", "MG"),
        ("Ipatinga", "MG"),
        ("Caruaru", "PE"),
        ("Petrolina", "PE"),
        ("Campina Grande", "PB"),
        ("Imperatriz", "MA"),
        ("Marabá", "PA"),
        ("Santarém", "PA"),
        ("Ji-Paraná", "RO"),
        ("Araguaína", "TO"),
        ("Boa Vista", "RR"),
        ("Macapá", "AP"),
        ("Cruzeiro do Sul", "AC"),
        ("Lages", "SC"),
        ("Carazinho", "RS"),
        ("Erechim", "RS"),
        ("Não-Me-Toque", "RS"),
    ]

    @pytest.mark.parametrize("nome,uf", AMOSTRA)
    def test_municipio_encontrado(self, nome, uf):
        code = municipio_para_ibge(nome, uf)
        assert code is not None, f"Municipio '{nome}/{uf}' not found"
        assert code > 0

    @pytest.mark.parametrize("nome,uf", AMOSTRA)
    def test_municipio_upper_sem_acento(self, nome, uf):
        import unicodedata

        upper_no_accent = "".join(
            c for c in unicodedata.normalize("NFKD", nome.upper()) if not unicodedata.combining(c)
        )
        code = municipio_para_ibge(upper_no_accent, uf)
        assert code is not None, f"Municipio '{upper_no_accent}/{uf}' not found (original: {nome})"

    @pytest.mark.parametrize("nome,uf", AMOSTRA)
    def test_municipio_lower(self, nome, uf):
        code = municipio_para_ibge(nome.lower(), uf)
        assert code is not None, f"Municipio '{nome.lower()}/{uf}' not found"
