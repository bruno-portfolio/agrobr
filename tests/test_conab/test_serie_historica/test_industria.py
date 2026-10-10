import warnings
from io import BytesIO
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from agrobr import conab
from agrobr.conab._serie_historica import api, industria
from agrobr.exceptions import InvalidParameterError, ParseError
from agrobr.utils.result import ATRIBUTO_AVISOS
from tests import helpers

MANIFEST = helpers.load_cana_industria_manifest()
COLUNAS = [
    "safra",
    "regiao",
    "uf",
    "acucar_mil_ton",
    "etanol_anidro_cana_mil_l",
    "etanol_hidratado_cana_mil_l",
    "etanol_anidro_milho_mil_l",
    "etanol_hidratado_milho_mil_l",
    "etanol_total_mil_l",
    "atr_kg_t",
]


def _parse(**kwargs) -> pd.DataFrame:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return industria.parse_cana_industria(MANIFEST["raw"], **kwargs)


def _linha(frame: pd.DataFrame, safra: str, uf: str) -> pd.Series:
    linhas = frame[(frame["safra"] == safra) & (frame["uf"] == uf)]
    assert len(linhas) == 1, (safra, uf)
    return linhas.iloc[0]


_DESCONTINUACAO = (DeprecationWarning, PendingDeprecationWarning, FutureWarning)


def _avisos_do_agrobr(capturados: list[warnings.WarningMessage]) -> list[str]:
    assert any(issubclass(aviso.category, FutureWarning) for aviso in capturados)
    return [
        str(aviso.message)
        for aviso in capturados
        if not issubclass(aviso.category, _DESCONTINUACAO)
    ]


@pytest.fixture
def descontinuacao_de_terceiro(monkeypatch):
    original = industria.parser._excel_file

    def com_aviso(raw: bytes) -> pd.ExcelFile:
        warnings.warn("motor de leitura descontinuado (simulado)", FutureWarning, stacklevel=2)
        return original(raw)

    monkeypatch.setattr(industria.parser, "_excel_file", com_aviso)


@pytest.fixture(scope="module")
def abas_golden() -> dict[str, pd.DataFrame]:
    return pd.read_excel(BytesIO(MANIFEST["raw"]), sheet_name=None, header=None)


def _xlsx(abas: dict[str, pd.DataFrame]) -> bytes:
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        for nome, aba in abas.items():
            aba.to_excel(writer, sheet_name=nome, index=False, header=False)
    return buffer.getvalue()


def _celula(aba: pd.DataFrame, rotulo: str, safra: str) -> tuple[int, int]:
    linha = aba.index[aba[0].astype(str).str.strip() == rotulo][0]
    coluna = next(c for c in aba.columns if str(aba.iat[5, c]).strip() == safra)
    return linha, coluna


def test_golden_forma_tipos_so_ufs_e_estimativa_fora():
    frame = _parse()
    assert list(frame.columns) == COLUNAS
    assert len(frame) == len(MANIFEST["safras"]) * len(MANIFEST["ufs"])
    assert not frame.duplicated(["safra", "uf"]).any()
    assert sorted(frame["safra"].unique()) == MANIFEST["safras"]
    assert sorted(frame["uf"].unique()) == MANIFEST["ufs"]
    assert set(frame["regiao"]) == {"NORTE", "NORDESTE", "CENTRO-OESTE", "SUDESTE", "SUL"}
    assert {str(frame[c].dtype) for c in COLUNAS[3:]} == {"float64"}
    assert MANIFEST["estimativa"][0][:7] not in set(frame["safra"])


def test_golden_celulas_do_oraculo_zero_traco_erro_e_vazio():
    frame = _parse()
    tipos = {celula["tipo"] for celula in MANIFEST["celulas"]}
    assert tipos == {"numero", "texto", "erro", "vazio"}
    assert any(c["tipo"] == "numero" and c["bruto"] == 0 for c in MANIFEST["celulas"])
    for celula in MANIFEST["celulas"]:
        valor = _linha(frame, celula["safra"], celula["uf"])[celula["campo"]]
        if celula["esperado"] is None:
            assert pd.isna(valor), celula
        else:
            assert valor == pytest.approx(celula["esperado"], rel=1e-12, abs=1e-9), celula


def test_golden_traco_e_erro_do_excel_saem_nan_e_zero_publicado_sai_zero():
    frame = _parse()
    rr_traco = _linha(frame, "2021/22", "RR")
    assert rr_traco[COLUNAS[3:]].isna().all()
    rr_zero = _linha(frame, "2020/21", "RR")
    assert rr_zero[["acucar_mil_ton", "etanol_total_mil_l", "atr_kg_t"]].tolist() == [0, 0, 0]
    assert rr_zero[["etanol_anidro_milho_mil_l", "etanol_hidratado_milho_mil_l"]].isna().all()
    df_na = _linha(frame, "2019/20", "DF")
    assert df_na[["etanol_total_mil_l", "etanol_hidratado_cana_mil_l"]].isna().all()
    assert df_na["etanol_anidro_cana_mil_l"] == 0


def test_filtros_de_ano_e_uf_e_resultado_vazio():
    frame = _parse(inicio=2018, fim=2019, uf="mt")
    assert frame["safra"].tolist() == ["2018/19", "2019/20"]
    assert set(frame["uf"]) == {"MT"}
    assert frame["etanol_anidro_milho_mil_l"].notna().all()
    vazio = _parse(inicio=2030)
    assert vazio.empty
    assert list(vazio.columns) == COLUNAS
    assert vazio.attrs[ATRIBUTO_AVISOS] == []


@pytest.mark.usefixtures("descontinuacao_de_terceiro")
def test_divergencias_publicadas_viram_aviso_sem_mudar_os_numeros():
    with warnings.catch_warnings(record=True) as capturados:
        warnings.simplefilter("always")
        frame = industria.parse_cana_industria(MANIFEST["raw"])
    avisos = frame.attrs[ATRIBUTO_AVISOS]
    assert _avisos_do_agrobr(capturados) == avisos
    divergencias = MANIFEST["divergencias"]
    assert len(avisos) == (
        len(divergencias["total_componentes"])
        + len(divergencias["brasil_soma_ufs"])
        + len(MANIFEST["erros_excel"])
        + 1
    )
    for item in MANIFEST["erros_excel"]:
        aviso = f"{item['safra']} {item['uf']} (série histórica industrial), a aba {item['aba']!r}"
        assert any(aviso in a and "erro do Excel" in a for a in avisos), item
        assert pd.isna(_linha(frame, item["safra"], item["uf"])[item["campo"]])
    for item in divergencias["total_componentes"]:
        assert any(f"{item['safra']} {item['uf']} (" in a and "etanol total" in a for a in avisos)
        total = _linha(frame, item["safra"], item["uf"])["etanol_total_mil_l"]
        assert pd.isna(total) if item["total"] is None else total == item["total"]
    for item in divergencias["brasil_soma_ufs"]:
        assert any(f"{item['safra']} (" in a and item["campo"] in a for a in avisos)
    assert all(aviso.endswith(("publicados", "publicados.", "devolve NaN")) for aviso in avisos)


def test_avisos_de_celula_seguem_o_recorte_pedido():
    item = MANIFEST["erros_excel"][0]
    ano = int(item["safra"][:4])
    na_uf = _parse(inicio=ano, fim=ano, uf=item["uf"]).attrs[ATRIBUTO_AVISOS]
    assert any("erro do Excel" in aviso for aviso in na_uf)
    for recorte in ({"uf": "SP"}, {"inicio": ano + 1}, {"fim": ano - 1}):
        avisos = _parse(**recorte).attrs[ATRIBUTO_AVISOS]
        assert not any("erro do Excel" in aviso for aviso in avisos), recorte


def test_safra_fechada_mais_recente_avisa_revisao_so_quando_sai():
    ultima = MANIFEST["safras"][-1]
    antes = _parse(fim=int(ultima[:4]) - 1, uf="SP")
    assert not any(a.startswith("Safra ") for a in antes.attrs[ATRIBUTO_AVISOS])
    frame = _parse(uf="SP")
    revisao = [a for a in frame.attrs[ATRIBUTO_AVISOS] if a.startswith("Safra ")]
    assert revisao == [
        f"Safra {ultima} de açúcar e etanol: é a mais recente fechada na série histórica da "
        "CONAB, que pode revisá-la nos levantamentos quadrimestrais da safra de cana; o agrobr "
        "repassa os números publicados."
    ]


def test_planilha_convertida_sem_mudanca_le_igual(abas_golden):
    pd.testing.assert_frame_equal(
        _parse(), industria.parse_cana_industria(_xlsx(abas_golden)), check_dtype=False
    )


def _trocar(aba: str, rotulo: str, safra: str, valor: object):
    def mudar(abas: dict[str, pd.DataFrame]) -> None:
        abas[aba].iat[_celula(abas[aba], rotulo, safra)] = valor

    return mudar


def _cabecalho(aba: str, antes: str, depois: object):
    def mudar(abas: dict[str, pd.DataFrame]) -> None:
        abas[aba].iat[_celula(abas[aba], "REGIÃO/UF", antes)] = depois

    return mudar


def _rotulo(aba: str, antes: str, depois: object):
    def mudar(abas: dict[str, pd.DataFrame]) -> None:
        abas[aba].iat[_celula(abas[aba], antes, "REGIÃO/UF")] = depois

    return mudar


def _celula_fixa(aba: str, linha: int, valor: object):
    def mudar(abas: dict[str, pd.DataFrame]) -> None:
        abas[aba].iat[linha, 0] = valor

    return mudar


MUDANCAS_DE_LAYOUT = {
    "aba_faltando": (
        lambda abas: abas.pop("Etanol Anidro (Milho)"),
        r"abas ausentes: etanol anidro \(milho\)",
    ),
    "aba_desconhecida": (
        lambda abas: abas.update({"Etanol de Beterraba": abas["Açúcar"]}),
        r"aba desconhecida 'Etanol de Beterraba'",
    ),
    "aba_repetida": (
        lambda abas: abas.update({"ACUCAR": abas["Açúcar"]}),
        r"mesmo nome normalizado: 'Açúcar' e 'ACUCAR'",
    ),
    "unidade_trocada": (
        _celula_fixa("Etanol Anidro", 4, "Em mil m³"),
        r"aba 'Etanol Anidro': unidade publicada 'Em mil m³'",
    ),
    "titulo_de_outra_aba": (
        _celula_fixa("Etanol Anidro", 2, "Série Histórica de Produção Etanol Anidro de Milho"),
        r"aba 'Etanol Anidro': título sem 'etanol anidro de cana-de-acucar'",
    ),
    "texto_com_numero": (
        _trocar("Açúcar", "AM", "2005/06", "22.704"),
        r"aba 'Açúcar', AM 2005/06: texto '22.704' no lugar de número",
    ),
    "valor_negativo": (
        _trocar("Açúcar", "SP", "2005/06", -1.0),
        r"valor fora do domínio",
    ),
    "uf_faltando": (
        _rotulo("Etanol Hidratado", "RR", None),
        r"aba 'Etanol Hidratado': linhas de UF .* ≠ as 27 UFs",
    ),
    "uf_repetida": (
        _rotulo("Açúcar", "AC", "AM"),
        r"aba 'Açúcar': linhas de UF .* ≠ as 27 UFs",
    ),
    "uf_sem_regiao": (
        _rotulo("Açúcar", "NORTE", None),
        r"valor fora do domínio",
    ),
    "nota_em_safra_fechada": (
        _cabecalho("ATR Médio", "2024/25", "2024/25 (²)"),
        r"aba 'ATR Médio': coluna marcada antes da última safra",
    ),
    "estimativa_sem_marca": (
        _cabecalho("Açúcar", "2026/27 (¹)", "2026/27"),
        r"aba 'Açúcar': rodapé anuncia estimativa sem coluna marcada",
    ),
    "safra_a_menos_numa_aba": (
        _cabecalho("ATR Médio", "2005/06", None),
        r"aba 'ATR Médio' com safras .* diferentes de",
    ),
    "safra_repetida": (
        _cabecalho("Açúcar", "2006/07", "2005/06"),
        r"aba 'Açúcar': safra repetida no cabeçalho",
    ),
}


@pytest.mark.parametrize("cenario", list(MUDANCAS_DE_LAYOUT))
def test_mudanca_de_layout_e_parse_error(abas_golden, cenario):
    mudar, erro = MUDANCAS_DE_LAYOUT[cenario]
    abas = {nome: aba.copy() for nome, aba in abas_golden.items()}
    mudar(abas)
    with pytest.raises(ParseError, match=erro) as excinfo:
        industria.parse_cana_industria(_xlsx(abas))
    assert excinfo.value.source == industria.FONTE


@pytest.mark.usefixtures("descontinuacao_de_terceiro")
def test_erro_do_excel_sai_nan_com_aviso(abas_golden):
    abas = {nome: aba.copy() for nome, aba in abas_golden.items()}
    abas["Açúcar"].iat[_celula(abas["Açúcar"], "AM", "2005/06")] = "#N/A"
    with warnings.catch_warnings(record=True) as capturados:
        warnings.simplefilter("always")
        frame = industria.parse_cana_industria(_xlsx(abas))
    avisos = frame.attrs[ATRIBUTO_AVISOS]
    assert _avisos_do_agrobr(capturados) == avisos
    assert pd.isna(_linha(frame, "2005/06", "AM")["acucar_mil_ton"])
    assert any("2005/06 AM" in a and "'Açúcar' publica erro do Excel" in a for a in avisos)


def test_zero_publicado_em_todas_as_ufs_da_safra_sai_zero(abas_golden):
    abas = {nome: aba.copy() for nome, aba in abas_golden.items()}
    milho = abas["Etanol Anidro (Milho)"]
    coluna = _celula(milho, "MT", "2019/20")[1]
    for uf in MANIFEST["ufs"]:
        milho.iat[_celula(milho, uf, "2019/20")[0], coluna] = 0
    frame = _parse_xlsx(abas)
    zeros = frame.loc[frame["safra"] == "2019/20", "etanol_anidro_milho_mil_l"]
    assert zeros.tolist() == [0.0] * len(MANIFEST["ufs"])
    assert not any("não levantada" in aviso for aviso in frame.attrs[ATRIBUTO_AVISOS])


def _parse_xlsx(abas: dict[str, pd.DataFrame]) -> pd.DataFrame:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return industria.parse_cana_industria(_xlsx(abas))


async def test_api_vazio_tem_os_dtypes_do_cheio(monkeypatch):
    helpers.install_cana_industria_http(monkeypatch)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        cheio = await conab.cana_industria(2024, 2024)
        vazio = await conab.cana_industria(ano_inicio=2100)
    assert vazio.empty and not cheio.empty
    pd.testing.assert_series_equal(vazio.dtypes, cheio.dtypes)


def test_motor_sem_leitura_de_erro_de_celula_e_parse_error(monkeypatch):
    monkeypatch.setattr(
        industria.parser,
        "_excel_file",
        lambda raw: pd.ExcelFile(BytesIO(raw), engine="calamine"),
    )
    with pytest.raises(ParseError, match="motor sem leitura de erro de célula"):
        industria.parse_cana_industria(MANIFEST["raw"])


@pytest.mark.usefixtures("descontinuacao_de_terceiro")
async def test_api_le_a_golden_com_metainfo_e_avisos(monkeypatch):
    requests = helpers.install_cana_industria_http(monkeypatch)
    with warnings.catch_warnings(record=True) as capturados:
        warnings.simplefilter("always")
        frame, meta = await conab.cana_industria(2024, 2025, return_meta=True)
    arquivo = MANIFEST["arquivo"]
    assert requests == [arquivo["url"]]
    assert frame["safra"].unique().tolist() == ["2024/25", "2025/26"]
    assert meta.source == meta.selected_source == "conab_cana_industria"
    assert meta.attempted_sources == ["conab_cana_industria"]
    assert meta.source_url == arquivo["url"]
    assert meta.parser_version == industria.PARSER_VERSION
    assert (meta.raw_content_hash, meta.raw_content_size) == (arquivo["sha256"], arquivo["bytes"])
    assert meta.records_count == len(frame) == 2 * len(MANIFEST["ufs"])
    assert meta.validation_warnings == _avisos_do_agrobr(capturados)
    assert [a.split(" (")[0] for a in meta.validation_warnings] == [
        "CONAB: em cana_industria 2025/26",
        "Safra 2025/26 de açúcar e etanol: é a mais recente fechada na série histórica da CONAB, "
        "que pode revisá-la nos levantamentos quadrimestrais da safra de cana; o agrobr repassa "
        "os números publicados.",
    ]


async def test_api_as_polars(monkeypatch):
    pl = pytest.importorskip("polars")
    helpers.install_cana_industria_http(monkeypatch)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        frame = await conab.cana_industria(2030, as_polars=True)
    assert frame.is_empty()
    assert frame.columns == COLUNAS
    assert frame.schema["safra"] == pl.Utf8


@pytest.mark.parametrize(
    ("kwargs", "erro"),
    [
        ({"ano_inicio": "2020"}, "ano_inicio deve ser um ano inteiro"),
        ({"ano_fim": True}, "ano_fim deve ser um ano inteiro"),
        ({"ano_inicio": 2021, "ano_fim": 2020}, "posterior a ano_fim"),
        ({"uf": "XX"}, "XX"),
    ],
)
async def test_parametros_invalidos_falham_antes_da_rede(kwargs, erro):
    with (
        patch.object(api.client, "download_xls", new_callable=AsyncMock) as download,
        pytest.raises(InvalidParameterError, match=erro),
    ):
        await conab.cana_industria(**kwargs)
    assert download.await_count == 0


async def test_serie_historica_aponta_a_api_industrial_antes_da_rede():
    with (
        patch.object(api.client, "download_xls", new_callable=AsyncMock) as download,
        pytest.raises(InvalidParameterError) as excinfo,
    ):
        await conab.serie_historica("cana_industria")
    assert download.await_count == 0
    assert "conab.cana_industria()" in str(excinfo.value)
    assert "datasets.producao_acucar_etanol()" in str(excinfo.value)
    assert "cana_industria" not in {item["produto"] for item in conab.produtos_serie_historica()}
