from __future__ import annotations

import json
from pathlib import Path

import pytest

from agrobr.conab.ceasa import models, parser
from agrobr.exceptions import ParseError
from agrobr.utils.result import datas_em_ns

GOLDEN_DIR = Path(__file__).parent.parent / "golden_data" / "conab_ceasa" / "precos_sample"


def _precos_json() -> dict:
    return json.loads(GOLDEN_DIR.joinpath("precos_response.json").read_text(encoding="utf-8"))


class TestParseEdgeCases:
    @pytest.mark.parametrize("resposta", [{}, {"metadata": []}, {"resultset": None}, []])
    def test_resposta_sem_resultset_nao_vira_vazio(self, resposta):
        with pytest.raises(ParseError, match="sem a lista 'resultset'"):
            parser.parse_precos(resposta)

    def test_empty_resultset(self):
        df = parser.parse_precos({"resultset": [], "metadata": []})
        assert len(df) == 0
        assert list(df.columns) == models.COLUNAS_SAIDA
        assert df.dtypes.equals(datas_em_ns(parser.parse_precos(_precos_json())).dtypes)

    def test_linhas_sem_preco_saem_vazio_tipado(self):
        resposta = _precos_json()
        resposta["resultset"] = [
            [linha[0]] + [None] * (len(linha) - 1) for linha in resposta["resultset"]
        ]
        df = parser.parse_precos(resposta)
        assert len(df) == 0
        assert df.dtypes.equals(datas_em_ns(parser.parse_precos(_precos_json())).dtypes)

    @pytest.mark.parametrize(
        "cabecalho",
        [
            "",
            None,
            "CEAGESP - SAO PAULO",
            "CEAGESP - SAO PAULO\r(13/02/2026)/Preco (R$)",
            " \rSAO PAULO\r(13/02/2026)/Preco (R$)",
            "CEAGESP \r \r(13/02/2026)/Preco (R$)",
            "CEAGESP \rSAO PAULO\r(13/02/2026)/Volume (KG)",
            "CEAGESP \rSAO PAULO\r(13/02/2026)/Preco (R$) extra",
            "CEAGESP \rSAO PAULO\r(2026-02-13)/Preco (R$)",
            "CEAGESP \nSAO PAULO\n(13/02/2026)/Preco (R$)",
            "CEAGESP\n \rSAO PAULO\r(13/02/2026)/Preco (R$)",
        ],
    )
    def test_cabecalho_fora_do_formato_publicado(self, cabecalho):
        resposta = _precos_json()
        resposta["metadata"][1]["colName"] = cabecalho
        with pytest.raises(ParseError, match="Cabeçalho de preço fora do formato publicado"):
            parser.parse_precos(resposta)

    def test_cabecalho_com_data_invalida(self):
        resposta = _precos_json()
        resposta["metadata"][1]["colName"] = "CEAGESP \rSAO PAULO\r(31/02/2026)/Preco (R$)"
        with pytest.raises(ParseError, match="data inválida"):
            parser.parse_precos(resposta)

    @pytest.mark.parametrize("espacos", [False, True], ids=["identico", "normalizado"])
    def test_ceasa_duplicada_no_cabecalho(self, espacos):
        resposta = _precos_json()
        duplicado = resposta["metadata"][1]["colName"]
        if espacos:
            duplicado = duplicado.replace("AMA/BA ", " AMA/BA   ").replace("JUAZEIRO", " JUAZEIRO ")
        resposta["metadata"][2]["colName"] = duplicado
        with pytest.raises(ParseError, match="duplicadas=.*AMA/BA - JUAZEIRO"):
            parser.parse_precos(resposta)

    @pytest.mark.parametrize("metadata", [[], [{"colName": "ProdutoUnid"}], None])
    def test_precos_sem_cabecalho_de_ceasa(self, metadata):
        resposta = _precos_json()
        resposta["metadata"] = metadata
        with pytest.raises(ParseError, match="Cabeçalhos de preço"):
            parser.parse_precos(resposta)

    @pytest.mark.parametrize("coluna", [{}, None])
    def test_coluna_sem_nome_no_cabecalho(self, coluna):
        resposta = _precos_json()
        resposta["metadata"][1] = coluna
        with pytest.raises(ParseError, match="Cabeçalho de preço fora do formato publicado"):
            parser.parse_precos(resposta)

    @pytest.mark.parametrize("variacao", [-1, 1])
    def test_largura_dos_precos_diverge_dos_cabecalhos(self, variacao):
        resposta = _precos_json()
        if variacao < 0:
            resposta["resultset"][0].pop()
        else:
            resposta["resultset"][0].append(1.0)
        with pytest.raises(ParseError, match="Quantidade de preços por linha diverge"):
            parser.parse_precos(resposta)
