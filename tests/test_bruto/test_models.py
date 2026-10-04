from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from importlib import resources

import pytest
from pydantic import ValidationError

from agrobr import bruto
from agrobr.bruto import models
from tests.test_bruto.conftest import ARQUIVO, PAGINADO


def _sem_tipo_ao_lado_de_const(no):
    """O pydantic do piso (2.5.0) não publica o ``type`` ao lado do ``const``; o atual publica."""
    if isinstance(no, dict):
        return {
            chave: _sem_tipo_ao_lado_de_const(valor)
            for chave, valor in no.items()
            if not (chave == "type" and "const" in no)
        }
    if isinstance(no, list):
        return [_sem_tipo_ao_lado_de_const(valor) for valor in no]
    return no


@pytest.mark.usefixtures("arquivo")
def test_schema_json_instalado_e_o_do_modelo():
    instalado = resources.files("agrobr.bruto").joinpath("manifesto.schema.json").read_text("utf-8")

    assert _sem_tipo_ao_lado_de_const(json.loads(instalado)) == _sem_tipo_ao_lado_de_const(
        models.RecursoBruto.model_json_schema()
    )
    assert sorted(json.loads(instalado)["required"]) == sorted(models.RecursoBruto.model_fields)
    assert len(models.RecursoBruto.model_fields) == 36


@pytest.mark.usefixtures("arquivo", "paginado")
async def test_linha_canonica_e_todas_as_chaves(tmp_path):
    for chave, kwargs in ((ARQUIVO, {"uf": "AL"}), (PAGINADO, {})):
        entrada = (await bruto.coletar(*chave, destino=tmp_path, **kwargs)).entrada
        linha = entrada.linha()
        assert json.loads(linha) == entrada.model_dump(mode="json")
        canonica = json.dumps(
            json.loads(linha), ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
        assert linha == canonica and chr(10) not in linha
        assert models.RecursoBruto.model_validate_json(linha) == entrada


@pytest.fixture
async def ok(paginado, tmp_path):
    paginado.ids = [10, 20, 30, 40, 50]
    entrada = (await bruto.coletar(*PAGINADO, destino=tmp_path, tamanho_pagina=2)).entrada
    return entrada.model_dump(mode="json")


def _mudar(dado, caminho, valor):
    alvo = dado
    for parte in caminho[:-1]:
        alvo = alvo[parte]
    if valor is KeyError:
        del alvo[caminho[-1]]
    else:
        alvo[caminho[-1]] = valor
    return dado


@pytest.mark.parametrize(
    ("caminho", "valor", "trecho"),
    [
        (("feicoes",), KeyError, "Field required"),
        (("extra",), 1, "Extra inputs"),
        (("feicoes",), True, "valid integer"),
        (("selecao", "bbox"), [0, 0, float("nan"), 1], "finite number"),
        (("consulta_id",), "0" * 64, "consulta_id"),
        (("status",), "erro", "erro deve ser null"),
        (
            ("erro",),
            {"tipo": "ParseError", "mensagem": "x", "http_status": None, "url": None},
            "erro deve ser null",
        ),
        (("cobertura", "total_depois"), 4, "total_antes = total_depois"),
        (("cobertura", "ids_repetidos"), 1, "ids_repetidos 0"),
        (("paginas", 1, "numero"), 3, "contíguos"),
        (
            ("paginas", 0, "arquivo"),
            "ibge/malha_municipal/brasil/x/../p.geojson",
            "caminho de artefato",
        ),
        (("paginas", 0, "arquivo"), "C:/abs/p.geojson", "caminho de artefato"),
        (("paginas", 0, "cabecalhos"), {"set-cookie": "a"}, "cabeçalhos fora"),
        (("paginas", 0, "sha256"), "ABC", "pattern"),
        (("http_status",), 200, "http_status, http_inicio"),
        (("arquivo",), "ibge/malha_municipal/brasil/x/original.zip", "todos preenchidos"),
        (("crs",), None, "ambos null"),
        (("registrado_em",), "2000-01-01T00:00:00Z", "fora de ordem"),
        (("cobertura", "snapshot_transacional"), True, "snapshot_transacional"),
        (("url_solicitada",), "http://fonte.test/wfs", "pattern"),
    ],
)
def test_invariante_violado_e_recusado(ok, caminho, valor, trecho):
    with pytest.raises(ValidationError, match=trecho):
        models.RecursoBruto.model_validate(_mudar(ok, caminho, valor))


def test_nucleo_importa_e_coleta_sem_geo_polars_e_parquet(tmp_path):
    codigo = f"""
import asyncio, sys
from importlib.abc import MetaPathFinder
class Bloqueio(MetaPathFinder):
    def find_spec(self, nome, caminho=None, alvo=None):
        if nome.split(".")[0] in ("geopandas", "polars", "pyarrow", "shapely", "pyogrio", "fiona"):
            raise ImportError(nome)
sys.meta_path.insert(0, Bloqueio())
sys.path.insert(0, {os.getcwd()!r})
import pytest
from tests.test_bruto import conftest
from agrobr import bruto
from agrobr.bruto import registry
import dataclasses, types
fonte = conftest.FonteFalsa()
modulo = types.ModuleType("falso"); modulo.adaptador = conftest.AdaptadorPaginas(fonte)
sys.modules["falso"] = modulo
registry.RECURSOS[conftest.PAGINADO] = dataclasses.replace(registry.RECURSOS[conftest.PAGINADO], modulo="falso", habilitado=True)
r = asyncio.run(bruto.coletar(*conftest.PAGINADO, destino=sys.argv[1]))
print(r.entrada.status, sorted(m for m in ("geopandas", "polars", "pyarrow", "shapely", "pyogrio") if m in sys.modules))
"""
    saida = subprocess.run(
        [sys.executable, "-c", codigo, str(tmp_path / "coleta")],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        encoding="utf-8",
        env={**os.environ, "AGROBR_HTTP_RATE_LIMIT_IBGE": "0.001"},
    )

    assert saida.returncode == 0, saida.stderr[-2000:]
    assert saida.stdout.strip() == "ok []"


@pytest.fixture
async def arquivo_ok(arquivo, tmp_path):
    entrada = (await bruto.coletar(*ARQUIVO, uf="AL", destino=tmp_path / "a")).entrada
    assert arquivo.pedidos
    return entrada.model_dump(mode="json")


@pytest.mark.parametrize("campo", list(models.LimitesBrutos.model_fields))
def test_limite_omitido_no_manifesto_e_recusado(arquivo_ok, campo):
    del arquivo_ok["opcoes"]["limites"][campo]

    with pytest.raises(ValidationError):
        models.RecursoBruto.model_validate_json(json.dumps(arquivo_ok))


@pytest.mark.parametrize(
    ("formato", "trecho"),
    [("csv", "arquivo fora do layout"), ("geojson", "modo arquivo exige formato zip ou csv")],
)
def test_formato_do_arquivo_segue_o_layout_original(arquivo_ok, formato, trecho):
    arquivo_ok["formato"] = formato

    with pytest.raises(ValidationError, match=trecho):
        models.RecursoBruto.model_validate_json(json.dumps(arquivo_ok))


def test_paginado_nao_usa_formato_de_arquivo(ok):
    ok["formato"] = "csv"

    with pytest.raises(ValidationError, match="modo paginado não usa formato de arquivo"):
        models.RecursoBruto.model_validate_json(json.dumps(ok))


def test_contagem_do_controle_diferente_da_cobertura_e_recusada(ok):
    ok["controles"][0]["valor_declarado"] += 1

    with pytest.raises(ValidationError, match="contagens dos controles"):
        models.RecursoBruto.model_validate_json(json.dumps(ok))


def test_ids_distintos_da_pagina_incompativeis_com_a_cobertura_sao_recusados(ok):
    ok["paginas"][0]["ids_distintos"] = 0

    with pytest.raises(ValidationError, match="ids_distintos"):
        models.RecursoBruto.model_validate_json(json.dumps(ok))


@pytest.fixture
async def com_bbox(paginado, tmp_path):
    entrada = (
        await bruto.coletar(
            *PAGINADO, bbox=(-48.1, -16.1, -47.9, -15.9), nome="area", destino=tmp_path / "b"
        )
    ).entrada
    assert paginado.pedidos
    return entrada.model_dump(mode="json")


@pytest.mark.parametrize("caso", ["bbox_textual", "booleano_numerico", "data_inexistente"])
def test_tipos_e_data_do_manifesto_seguem_o_contrato(com_bbox, caso):
    if caso == "bbox_textual":
        com_bbox["selecao"]["bbox"][0] = str(com_bbox["selecao"]["bbox"][0])
    elif caso == "booleano_numerico":
        com_bbox["cobertura"]["snapshot_transacional"] = 0
    else:
        com_bbox["registrado_em"] = "2026-99-99T25:61:61Z"

    with pytest.raises(ValidationError):
        models.RecursoBruto.model_validate_json(json.dumps(com_bbox))


def test_schema_exige_os_limites_e_prazo_positivo():
    esquema = models.RecursoBruto.model_json_schema()
    limites = esquema["$defs"]["LimitesManifesto"]

    assert sorted(limites["required"]) == sorted(models.LimitesBrutos.model_fields)
    assert limites["properties"]["max_segundos"]["exclusiveMinimum"] == 0
    assert esquema["$defs"]["Cobertura"]["properties"]["snapshot_transacional"]["const"] is False


CAMPOS_UTC = {
    (modelo, campo)
    for modelo, campos in (
        ("RecursoBruto", ("inicio", "fim", "registrado_em", "http_inicio", "http_fim")),
        ("PaginaBruta", ("inicio", "fim")),
        ("ControleBruto", ("inicio", "fim")),
    )
    for campo in campos
}


def _datas_publicadas(esquema):
    for modelo, definicao in {"RecursoBruto": esquema, **esquema["$defs"]}.items():
        for campo, propriedade in definicao.get("properties", {}).items():
            for opcao in propriedade.get("anyOf", [propriedade]):
                if opcao.get("format") == "date-time":
                    yield (modelo, campo), opcao["pattern"]


def test_schema_publica_formato_e_padrao_utc_em_toda_data():
    padroes = dict(_datas_publicadas(models.RecursoBruto.model_json_schema()))

    assert set(padroes) == CAMPOS_UTC
    for padrao in padroes.values():
        assert re.search(padrao, "2026-10-03T13:32:19.5Z")
        assert not re.search(padrao, "2026-99-99T25:61:61Z")
        assert not re.search(padrao, "ontem")


@pytest.mark.parametrize("valor", ["2026-99-99T25:61:61Z", "2026-02-30T00:00:00Z", "ontem"])
def test_schema_com_format_checker_recusa_data_utc_impossivel(com_bbox, valor):
    jsonschema = pytest.importorskip("jsonschema")
    pytest.importorskip("rfc3339_validator")
    validador = jsonschema.Draft202012Validator(
        models.RecursoBruto.model_json_schema(),
        format_checker=jsonschema.Draft202012Validator.FORMAT_CHECKER,
    )
    assert validador.is_valid(com_bbox)

    com_bbox["registrado_em"] = valor
    assert not validador.is_valid(com_bbox)
