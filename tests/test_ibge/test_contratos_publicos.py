from __future__ import annotations

import inspect
import json
import warnings
from pathlib import Path
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from agrobr import conab, contracts, datasets, ibge
from agrobr.exceptions import InvalidParameterError, ParseError, SourceUnavailableError
from agrobr.ibge import agregados, censo_municipal_1985, client, ftp_client, legacy_api
from tests.helpers import assert_replay_samples, assert_replay_served
from tests.test_ibge import test_agregados as replay

DATASET_CALLS = {
    "pam_soja_uf_2023": lambda: datasets.producao_anual("soja", 2023, return_meta=True),
    "pam_milho_brasil_2022_2023": lambda: datasets.producao_anual(
        "milho", [2022, 2023], nivel="brasil", return_meta=True
    ),
    "ppm_bovino_uf_2023": lambda: datasets.pecuaria_municipal("bovino", 2023, return_meta=True),
    "abate_bovino_2024T1": lambda: datasets.abate_trimestral("bovino", "2024T1", return_meta=True),
    "silvicultura_2023": lambda: datasets.silvicultura("carvao", 2023, return_meta=True),
    "extracao_vegetal_2023": lambda: datasets.extrativismo_vegetal("acai", 2023, return_meta=True),
    "leite_2024T1": lambda: datasets.leite_industrial("2024T1", return_meta=True),
    "pib_agro_2024T1": lambda: datasets.pib_agro(trimestre="2024T1", return_meta=True),
    "censo_efetivo_2017_uf": lambda: datasets.censo_agropecuario(
        "efetivo_rebanho", ano=2017, return_meta=True
    ),
    "censo_historico_estab": lambda: datasets.censo_agropecuario_historico(
        "estabelecimentos_area", ano=2006, return_meta=True
    ),
}


@pytest.mark.parametrize("variaveis", [[], ["xx"], ["area_plantada", "xx"], "producao", [None]])
async def test_pam_variaveis_invalidas_nao_consultam_sidra(monkeypatch, variaveis):
    fetch = AsyncMock()
    monkeypatch.setattr(client, "fetch_sidra", fetch)
    with pytest.raises(InvalidParameterError, match="Disponíveis.*area_plantada"):
        await ibge.pam("soja", 2023, variaveis=variaveis)
    fetch.assert_not_awaited()


@pytest.mark.parametrize("ano", [[], (), "abc", 2030, True, [2023, False]])
@pytest.mark.parametrize(
    "funcao,produto",
    [
        (ibge.pam, "soja"),
        (ibge.ppm, "bovino"),
        (ibge.silvicultura, "carvao"),
        (ibge.extracao_vegetal, "acai"),
    ],
)
async def test_ano_invalido_nao_consulta_sidra(monkeypatch, funcao, produto, ano):
    fetch = AsyncMock()
    monkeypatch.setattr(client, "fetch_sidra", fetch)
    with pytest.raises(InvalidParameterError, match="ano"):
        await funcao(produto, ano=ano)
    fetch.assert_not_awaited()


@pytest.mark.parametrize("ano", [[], "1995", True, [1995, True], [1995.0], {}])
async def test_censo_historico_recusa_tipo_de_ano_sem_iterar_texto(monkeypatch, ano):
    fetch = AsyncMock()
    monkeypatch.setattr(client, "fetch_sidra", fetch)
    with pytest.raises(InvalidParameterError, match="[Aa]no.*Disponíveis"):
        await ibge.censo_agro_historico("uso_terra", ano=ano)
    fetch.assert_not_awaited()


async def test_pib_posicional_antigo_falha_com_setores_validos(monkeypatch):
    fetch = AsyncMock()
    monkeypatch.setattr(client, "fetch_sidra", fetch)
    with pytest.raises(InvalidParameterError, match="Setor.*agropecuaria.*industria"):
        await ibge.pib_agro("2024T1")
    fetch.assert_not_awaited()


async def test_pib_dataset_normaliza_setor_e_precos_sem_mudar_valor_publicado(monkeypatch):
    caso = "pib_agro_2024T1"
    servido = replay._fallback_frame(caso, monkeypatch)
    frame = await datasets.pib_agro(" AGROPECUÁRIA ", trimestre="2024T1", precos=" CORRENTE ")
    assert_replay_served(servido)
    assert_replay_samples(frame, replay.ORACLE_CASES[caso])
    assert set(frame["precos"]) == {"corrente"}
    assert set(frame["setor"]) == {"agropecuaria"}


@pytest.mark.parametrize("camada", [ibge, datasets], ids=["fonte", "dataset"])
@pytest.mark.parametrize("return_meta", [False, True])
async def test_abate_polars_preserva_contagem_e_valor_publicado(monkeypatch, camada, return_meta):
    pl = pytest.importorskip("polars")
    caso = "abate_bovino_2024T1"
    servido = replay._fallback_frame(caso, monkeypatch)
    funcao = ibge.abate if camada is ibge else datasets.abate_trimestral
    resultado = await funcao("bovino", "2024T1", as_polars=True, return_meta=return_meta)
    frame = resultado[0] if return_meta else resultado
    assert_replay_served(servido)
    assert isinstance(frame, pl.DataFrame)
    assert frame.schema["animais_abatidos"] == pl.Int64
    assert frame.schema["peso_carcacas"] == pl.Float64
    assert_replay_samples(pd.DataFrame(frame.to_dicts()), replay.ORACLE_CASES[caso])
    if return_meta:
        assert resultado[1].contract_version == "2.0"


@pytest.mark.parametrize("uf", [51, [], {}, "", "  ", "XX"])
@pytest.mark.parametrize("funcao,args", [(ibge.abate, ("bovino",)), (ibge.leite_trimestral, ())])
async def test_uf_invalida_nao_consulta_sidra(monkeypatch, funcao, args, uf):
    fetch = AsyncMock()
    monkeypatch.setattr(client, "fetch_sidra", fetch)
    with pytest.raises(InvalidParameterError, match="UF.*AC.*SP"):
        await funcao(*args, uf=uf)
    fetch.assert_not_awaited()


@pytest.mark.parametrize("uf", ["mt", " MT ", "51", " 51 "])
def test_conversor_uf_preserva_codigo_e_remove_espacos(uf):
    assert client.uf_to_ibge_code(uf) == "51"


async def test_vazio_avisa_em_cada_consulta_e_registra_no_meta(monkeypatch):
    fetch = AsyncMock(return_value=pd.DataFrame())
    monkeypatch.setattr(client, "_fetch_sidra_em_fatias", fetch)
    chamadas = [
        lambda: ibge.ppm("galinhas", 2024, uf="SP", return_meta=True),
        lambda: ibge.ppm("bovino", 2024, uf="MT", return_meta=True),
        lambda: ibge.ppm("bovino", 2024, uf="MT", return_meta=True),
        lambda: datasets.pecuaria_municipal("bovino", 2024, uf="MT", return_meta=True),
    ]
    for chamada in chamadas:
        with warnings.catch_warnings(record=True) as avisos:
            warnings.simplefilter("always")
            frame, meta = await chamada()
        vazios = [str(a.message) for a in avisos if "IBGE sem dado" in str(a.message)]
        assert frame.empty
        assert len(vazios) == 1
        assert meta.validation_warnings == vazios
    assert fetch.await_count == 4


@pytest.mark.parametrize(
    "caso,camada",
    [
        (caso, camada)
        for caso in sorted(replay.CALLS)
        for camada in ("fonte", "dataset")
        if camada == "fonte" or caso in DATASET_CALLS
    ],
)
async def test_dtype_vazio_igual_ao_cheio_de_captura_oficial(monkeypatch, caso, camada):
    monkeypatch.setattr(agregados, "_periodos_cache", {})
    servido = replay._fallback_frame(caso, monkeypatch)
    chamada = replay.CALLS[caso] if camada == "fonte" else DATASET_CALLS[caso]
    cheio, _ = await chamada()
    assert_replay_served(servido)
    assert_replay_samples(cheio, replay.ORACLE_CASES[caso])
    monkeypatch.setattr(client, "_fetch_sidra_em_fatias", AsyncMock(return_value=pd.DataFrame()))
    with pytest.warns(UserWarning, match="IBGE sem dado"):
        vazio, meta = await chamada()
    assert vazio.empty
    assert not cheio.empty
    assert vazio.dtypes.to_dict() == cheio.dtypes.to_dict()
    assert list(vazio.columns) == list(cheio.columns)
    assert any("IBGE sem dado" in aviso for aviso in meta.validation_warnings)
    if caso.startswith("abate"):
        assert str(cheio["animais_abatidos"].dtype) == "Int64"
        assert str(cheio["peso_carcacas"].dtype) == "float64"


@pytest.mark.parametrize("camada", [ibge, datasets], ids=["fonte", "dataset"])
async def test_censo_1985_tipos_reais_e_vazio_do_pacote(monkeypatch, camada):
    golden = (
        Path(__file__).resolve().parents[1]
        / "golden_data/censo_1985_municipal/oraculo_amostra.json"
    )
    celula = json.loads(golden.read_text(encoding="utf-8"))["celulas"][0]
    tema = censo_municipal_1985.TABELAS_CENSO_MUNICIPAL_1985[celula["tabela"]]
    nome = "censo_agro_municipal_1985" if camada is ibge else "censo_agropecuario_municipal_1985"
    funcao = getattr(camada, nome)
    cheio = await funcao(tema, uf=celula["uf"])
    casa = cheio.set_index(["volume", "pagina_pdf", "linha", "coluna"]).loc[
        (celula["volume"], celula["pagina_pdf"], celula["linha"], celula["coluna"])
    ]
    assert casa["valor"] == celula["valor"]
    assert casa["localidade"] == celula["localidade"]
    consultar = censo_municipal_1985._consultar

    def consultar_vazio(sql, params):
        frame = consultar(sql, params)
        return frame.iloc[:0] if params["caminho"] == str(censo_municipal_1985._PACOTE) else frame

    monkeypatch.setattr(censo_municipal_1985, "_consultar", consultar_vazio)
    vazio = await funcao(tema, uf=celula["uf"])
    assert vazio.empty
    assert vazio.dtypes.to_dict() == cheio.dtypes.to_dict()
    assert str(cheio["ano"].dtype) == "Int64"
    assert cheio["uf"].dtype == pd.Series(["AC"]).dtype


@pytest.mark.parametrize("camada", [ibge, datasets], ids=["fonte", "dataset"])
async def test_censo_legado_tipos_reais_e_vazio_filtrado(monkeypatch, camada):
    golden = Path(__file__).resolve().parents[1] / "golden_data/ibge/censo_legado_oficial"

    async def download(tabela, **_kwargs):
        return (golden / f"Brasil_{tabela}.zip").read_bytes()

    monkeypatch.setattr(ftp_client, "download_legacy_zip", download)
    nome = "censo_agro_legado" if camada is ibge else "censo_agropecuario_legado"
    funcao = getattr(camada, nome)
    cheio = await funcao("financeiro", nivel="brasil")
    assert 26880228.229 in cheio["valor"].tolist()
    fetch = legacy_api._fetch_tables

    async def tabelas_vazias(tema, uf):
        frames, urls = await fetch(tema, uf)
        return [frame.iloc[:0] for frame in frames], urls

    monkeypatch.setattr(legacy_api, "_fetch_tables", tabelas_vazias)
    vazio = await funcao("financeiro", nivel="brasil")
    assert vazio.empty
    assert vazio.dtypes.to_dict() == cheio.dtypes.to_dict()


PARES = [
    ("pam", "producao_anual", ("produto", "ano")),
    ("ppm", "pecuaria_municipal", ("especie", "ano")),
    ("abate", "abate_trimestral", ("especie", "trimestre")),
    ("silvicultura", "silvicultura", ("produto", "ano")),
    ("extracao_vegetal", "extrativismo_vegetal", ("produto", "ano")),
    ("leite_trimestral", "leite_industrial", ("trimestre",)),
    ("pib_agro", "pib_agro", ("setor",)),
    ("censo_agro", "censo_agropecuario", ("tema",)),
    ("censo_agro_historico", "censo_agropecuario_historico", ("tema",)),
    ("censo_agro_legado", "censo_agropecuario_legado", ("tema",)),
    ("censo_agro_municipal_1985", "censo_agropecuario_municipal_1985", ("tema",)),
]


@pytest.mark.parametrize("fonte,dataset,posicionais", PARES)
def test_fonte_dataset_alinham_posicionais_e_flags(fonte, dataset, posicionais):
    for camada, nome in ((ibge, fonte), (datasets, dataset)):
        assinatura = inspect.signature(getattr(camada, nome))
        esperados = tuple(
            "produto" if p in ("especie", "setor") and camada is datasets else p
            for p in posicionais
        )
        assert (
            tuple(
                p.name for p in assinatura.parameters.values() if p.kind is p.POSITIONAL_OR_KEYWORD
            )
            == esperados
        )
        assert all(
            assinatura.parameters[p].kind is inspect.Parameter.KEYWORD_ONLY
            for p in ("return_meta", "as_polars")
        )


@pytest.mark.parametrize("_fonte,dataset,_posicionais", PARES)
async def test_dataset_normaliza_produto_antes_de_chamar_a_fonte(_fonte, dataset, _posicionais):
    instancia = datasets.get_dataset(dataset)
    produto = instancia.info.products[0]
    fetch = AsyncMock(return_value=(contracts.get_contract(dataset).empty_frame(), None))
    instancia.info.sources[0].fetch_fn = fetch
    instancia.info.sources = instancia.info.sources[:1]
    await instancia.fetch(f" {produto.upper()} ")
    assert fetch.await_args.args == (produto,)


async def test_lista_anos_no_fallback_conab_e_indisponibilidade_explicita(monkeypatch):
    monkeypatch.setattr(
        ibge,
        "pam",
        AsyncMock(side_effect=SourceUnavailableError(source="ibge", last_error="fora do ar")),
    )
    fallback = AsyncMock()
    monkeypatch.setattr(conab, "safras", fallback)
    with pytest.raises(SourceUnavailableError, match="não cobre lista de anos"):
        await datasets.producao_anual("soja", [2022, 2023])
    fallback.assert_not_awaited()


async def test_abate_fracionario_e_erro_de_layout_sem_truncar(monkeypatch):
    corpo = json.loads((replay.GOLDEN / "agregados/agregados_008.json").read_bytes())
    assert corpo[0]["id"] == "284"
    assert corpo[0]["unidade"] == "Cabeças"
    corpo[0]["resultados"][0]["series"][0]["serie"]["202401"] = "793855.5"
    resposta = agregados.to_sidra_frame(corpo, variable="284,285", classifications=None)
    monkeypatch.setattr(client, "fetch_sidra", AsyncMock(return_value=resposta))
    with pytest.raises(ParseError, match="quantidade fracionária"):
        await ibge.abate("bovino", trimestre="202401")
