import csv
import hashlib
import io
import json
import re
from pathlib import Path
from unittest.mock import Mock

import pytest

from agrobr.bcb import models
from agrobr.exceptions import InvalidParameterError
from tests.helpers import collect_failures, sem_excecao

ORACULO = Path(__file__).parents[1] / "golden_data/bcb/oraculo_20260923"
ORACULO_MANIFEST = json.loads((ORACULO / "manifest.json").read_text(encoding="utf-8"))
DOMINIOS = Path(__file__).parents[1] / "golden_data/bcb/sicor_dominios_20260927"
DOMINIOS_MANIFEST = json.loads((DOMINIOS / "manifest.json").read_text(encoding="utf-8"))


def tabela_oficial(nome: str, delimitador: str) -> list[list[str]]:
    recurso = next(item for item in ORACULO_MANIFEST["resources"] if item["file"] == nome)
    corpo = (ORACULO / nome).read_bytes()
    assert hashlib.sha256(corpo).hexdigest() == recurso["sha256"]
    return list(csv.reader(io.StringIO(corpo.decode("cp1252")), delimiter=delimitador))[1:]


def nome_publicado(descricao: str) -> str:
    return descricao.split(" - ", 1)[0].replace('"', "").strip()


def test_dicionarios_sicor_seguem_as_tabelas_oficiais_do_bcb():
    programas = {
        codigo: nome_publicado(descricao)
        for codigo, descricao, *_vigencia in tabela_oficial("dominio_Programa.csv", ";")
    }
    seguros = dict(tabela_oficial("dominio_TipoGarantiaEmpreendimento.csv", ","))

    assert programas == models.SICOR_PROGRAMAS
    assert seguros == models.SICOR_TIPOS_SEGURO
    with sem_excecao():
        resolvidos = {codigo: models.resolve_programa(codigo) for codigo in programas}
        resolvidos_seguro = {codigo: models.resolve_tipo_seguro(codigo) for codigo in seguros}
    assert (resolvidos, resolvidos_seguro) == (programas, seguros)
    assert (programas["0153"], programas["0156"], programas["0162"], programas["0222"]) == (
        "MODERAGRO",
        "ABC + Programa para a Adaptação à Mudança do Clima e Baixa Emissão de Carbono",
        "INOVAGRO",
        "RenovAgro",
    )
    assert (seguros["2"], seguros["9"]) == ("Proagro mais", "Sem adesão a seguro")


def test_fonte_modalidade_e_atividade_seguem_as_tabelas_oficiais_do_bcb():
    def dominio(nome: str, delimitador: str) -> list[list[str]]:
        recurso = next(item for item in DOMINIOS_MANIFEST["resources"] if item["file"] == nome)
        corpo = (DOMINIOS / nome).read_bytes()
        assert hashlib.sha256(corpo).hexdigest() == recurso["sha256"]
        return list(csv.reader(io.StringIO(corpo.decode("cp1252")), delimiter=delimitador))[1:]

    fontes = {
        codigo: descricao.strip()
        for codigo, descricao, *_vigencia in dominio("dominio_FonteRecursos.csv", ";")
    }
    modalidades: dict[str, set[str]] = {}
    for *_finalidade_atividade, codigo, nome in dominio("dominio_Modalidade.csv", ";"):
        modalidades.setdefault(codigo, set()).add(nome.strip())
    atividades = dict(dominio("dominio_Atividade.csv", ","))

    assert fontes == models.SICOR_FONTES_RECURSO
    assert modalidades == {codigo: {nome} for codigo, nome in models.SICOR_MODALIDADES.items()}
    assert atividades == models.SICOR_ATIVIDADES
    assert [
        models.resolve_fonte_recurso(codigo) for codigo in ["0201", "0303", "0403", "0430", "0505"]
    ] == [
        "OBRIGATÓRIOS - MCR 6.2 - DIRECIONADA/CONTROLADA",
        "POUPANÇA RURAL - DIRECIONADA/NÃO CONTROLADA",
        "RECURSOS LIVRES - EQUALIZADA - LIVRE/CONTROLADA",
        "LETRA DE CRÉDITO DO AGRONEGÓCIO (LCA) - DIRECIONADA/NÃO CONTROLADA",
        "BNDES/FINAME - EQUALIZADA - DIRECIONADA/CONTROLADA",
    ]
    assert [models.resolve_modalidade(codigo) for codigo in ["01", "16", "20", "25", "27"]] == [
        "LAVOURA",
        "AQUISIÇÃO DE ANIMAIS DE SERVIÇO (USO AGRICULTURA)",
        "FEE (EX-LEC)",
        "ESTOCAGEM",
        "AQUISIÇÃO DE ANIMAIS",
    ]
    assert [models.resolve_atividade(codigo) for codigo in ["1", "2"]] == [
        "Agrícola",
        "Pecuário(a)",
    ]
    assert (
        models.resolve_fonte_recurso("0999"),
        models.resolve_modalidade("88"),
        models.resolve_atividade("7"),
    ) == (None, None, None)


def test_safra_e_produto_viram_a_grafia_do_sicor():
    with sem_excecao():
        safras = [
            models.normalize_safra_sicor(safra)
            for safra in ["2023/2024", "2023/24", "2024", "  2023/24  ", "2099/00"]
        ]
    assert safras == ["2023/2024", "2023/2024", "2023/2024", "2023/2024", "2099/2100"]
    grafias = {
        "soja": "SOJA",
        "SOJA": "SOJA",
        "Soja": "SOJA",
        "milho": "MILHO",
        "cafe": "CAFÉ",
        "café": "CAFÉ",
        "feijao": "FEIJÃO",
        "feijão": "FEIJÃO",
        "algodao": "ALGODÃO",
        "algodão": "ALGODÃO",
        "cana": "CANA-DE-AÇUCAR",
        "quinoa": "QUINOA",
        ' "bovinos" ': "BOVINOS",
        '"cafe"': "CAFÉ",
    }
    assert {alias: models.resolve_produto_sicor(alias) for alias in grafias} == {
        alias: f'"{nome}"' for alias, nome in grafias.items()
    }
    with collect_failures() as check:
        for produto, motivo in [
            ("", "produto deve ser uma string não vazia"),
            (" ", "produto deve ser uma string não vazia"),
            (None, "produto deve ser uma string não vazia"),
            ("cafe_arabica", "O SICOR não distingue café arábica/conilon; use 'cafe'"),
            ("Café_Conilon", "O SICOR não distingue café arábica/conilon; use 'cafe'"),
        ]:
            with check(produto), pytest.raises(InvalidParameterError, match=re.escape(motivo)):
                models.resolve_produto_sicor(produto)


def test_codigos_das_dimensoes_viram_nome_ou_desconhecido(monkeypatch: pytest.MonkeyPatch):
    avisos = Mock()
    monkeypatch.setattr(models, "logger", avisos)
    conhecidos = [
        (models.resolve_programa, "0050", "PRONAMP"),
        (models.resolve_tipo_seguro, "1", "Proagro tradicional"),
    ]
    assert [resolver(codigo) for resolver, codigo, _nome in conhecidos] == [
        nome for *_resto, nome in conhecidos
    ]
    avisos.warning.assert_not_called()
    desconhecidos = [
        (models.resolve_programa, "9999", "programa"),
        (models.resolve_tipo_seguro, "7", "tipo_seguro"),
    ]
    assert [resolver(codigo) for resolver, codigo, _dominio in desconhecidos] == [
        f"Desconhecido ({codigo})" for _resolver, codigo, _dominio in desconhecidos
    ]
    assert [chamada.kwargs for chamada in avisos.warning.call_args_list] == [
        {"dominio": dominio, "codigo": codigo} for _resolver, codigo, dominio in desconhecidos
    ]
