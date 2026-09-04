from __future__ import annotations

import pytest

from agrobr.exceptions import InvalidParameterError
from agrobr.unica.models import (
    MIX_LABELS,
    PRODUTOS_HISTORICO,
    PRODUTOS_QUINZENAL,
    REGIOES_QUINZENAL,
    RESUMO_LABELS,
    resolve_produto,
)


class TestProdutosQuinzenal:
    def test_cinco_produtos_nas_tabelas_3_a_7(self):
        tabelas = sorted(num for num, _ in PRODUTOS_QUINZENAL.values())
        assert tabelas == [3, 4, 5, 6, 7]

    def test_unidades(self):
        assert PRODUTOS_QUINZENAL["cana"] == (3, "t")
        assert PRODUTOS_QUINZENAL["etanol_total"] == (5, "m3")

    def test_tres_regioes(self):
        assert REGIOES_QUINZENAL == ["sao_paulo", "centro_sul", "demais_estados"]


class TestResumoLabels:
    def test_labels_principais_mapeadas(self):
        produtos = {produto for produto, _ in RESUMO_LABELS.values()}
        assert {"cana", "acucar", "etanol_anidro", "etanol_hidratado", "etanol_total"} <= produtos
        assert {"atr", "atr_por_tonelada"} <= produtos

    def test_mix_labels(self):
        assert MIX_LABELS == {"acucar": "mix_acucar", "etanol": "mix_etanol"}


class TestResolveProduto:
    def test_canonico(self):
        assert resolve_produto("cana", PRODUTOS_QUINZENAL) == "cana"

    def test_alias_com_acento(self):
        assert resolve_produto("açúcar", PRODUTOS_QUINZENAL) == "acucar"

    def test_alias_ingles(self):
        assert resolve_produto("sugarcane", PRODUTOS_QUINZENAL) == "cana"

    def test_etanol_total_com_espaco(self):
        assert resolve_produto("etanol total", PRODUTOS_QUINZENAL) == "etanol_total"

    def test_etanol_resolve_para_hidratado(self):
        assert resolve_produto("etanol", PRODUTOS_QUINZENAL) == "etanol_hidratado"

    def test_produto_fora_do_dominio_raises(self):
        with pytest.raises(InvalidParameterError, match="inválido para UNICA"):
            resolve_produto("soja", PRODUTOS_QUINZENAL)

    def test_historico_mesmos_produtos(self):
        assert resolve_produto("cana", PRODUTOS_HISTORICO) == "cana"
        with pytest.raises(InvalidParameterError, match="inválido para UNICA"):
            resolve_produto("milho", PRODUTOS_HISTORICO)
