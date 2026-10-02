import inspect

from agrobr import b3, sync


def test_posicoes_abertas_historico_exporta_o_nome_publico_em_portugues():
    funcao = getattr(b3, "posicoes_abertas_historico", None)
    assert callable(funcao)
    assert "posicoes_abertas_historico" in b3.__all__
    assert not hasattr(b3, "oi_historico")
    assert callable(sync.b3.posicoes_abertas_historico)
    parametros = inspect.signature(funcao).parameters
    assert list(parametros) == [
        "contrato",
        "inicio",
        "fim",
        "vencimento",
        "tipo",
        "as_polars",
        "return_meta",
    ]
    assert all(p.kind is inspect.Parameter.KEYWORD_ONLY for p in parametros.values())
