from __future__ import annotations

import re
from collections.abc import Iterator
from pathlib import Path

from agrobr import constants, contracts, ibge, zarc
from agrobr.conab._serie_historica import client
from agrobr.contracts import _legacy
from agrobr.datasets import registry
from tests.helpers import collect_failures

ROOT = Path(__file__).resolve().parents[1]
LANDINGS = (ROOT / "index.html", ROOT / "en/index.html")
SOURCE_COUNT = r"(?<!\+ )\b(\d+) (?:fontes|(?:public |agricultural data )?sources)\b"
CONTRACT_COUNT = (
    r"\b(\d+) (?:contratos (?:versionados|registrados)|(?:versioned|registered) contracts)\b"
)
CONTRACT_PAGES = tuple(
    path
    for path in sorted((ROOT / "docs/contracts").glob("*.md"))
    if path.name.removesuffix(".md").removesuffix(".en") not in {"index", "semver", "bruto"}
)
MULTI_CONTRACT_PAGES = {
    "bcb_ptax",
    "clima",
    "desmatamento",
    "futuros_agricolas",
    "uso_do_solo",
}
ADDITIONAL_PRIMARY_KEYS = {
    "desmatamento": ("desmatamento_deter",),
    "uso_do_solo": ("mapbiomas_transicao",),
}

SCHEMA_CONTRACTS = {
    "bcb_ptax": (
        (r"Cotações|Quotes", "bcb_ptax"),
        (r"Catálogo de moedas|Currency catalogue", "bcb_ptax_moedas"),
    ),
    "clima": (
        (r"\bCLIMA_V\d+\b", "clima"),
        (r"\bCLIMA_ESTACAO_V\d+\b", "clima_estacao"),
        (r"\bCLIMA_ESTACAO_HORARIA_V\d+\b", "clima_estacao_horaria"),
    ),
    "desmatamento": (
        (r"PRODES", "desmatamento_prodes"),
        (r"DETER", "desmatamento_deter"),
    ),
    "futuros_agricolas": (
        (r"\bAJUSTE_DIARIO_V\d+\b", "ajuste_diario"),
        (r"\bPOSICOES_ABERTAS_V\d+\b", "posicoes_abertas"),
    ),
    "seguro_rural": (
        (r"Apólices|Policies", "mapa_psr_apolices"),
        (r"Sinistros|Claims", "mapa_psr_sinistros"),
    ),
    "uso_do_solo": (
        (r"Schema: Cobertura|Schema: Cover", "mapbiomas_cobertura"),
        (r"Schema: Transição|Schema: Transition", "mapbiomas_transicao"),
        (r"Nível municipal|Municipal level", "mapbiomas_cobertura_municipal"),
    ),
}
TYPE_HEADERS = {
    "tipo",
    "type",
    "tipo pandas",
    "pandas type",
    "pandas dtype",
    "dtype pandas",
    "tipo físico pandas",
    "physical pandas type",
}
TYPE_ALIASES = {
    **dict.fromkeys(("str", "string", "texto", "text"), "STRING"),
    **dict.fromkeys(("int", "integer", "int64"), "INTEGER"),
    **dict.fromkeys(("float", "float64"), "FLOAT"),
    "date": "DATE",
    **dict.fromkeys(("datetime", "datetime64"), "DATETIME"),
    **dict.fromkeys(("bool", "boolean"), "BOOLEAN"),
    "decimal": "DECIMAL",
}
NULL_HEADERS = {"nullable", "nulo", "nula", "nulável", "anulável", "obrigatório", "required"}
NULL_ALIASES = {
    **dict.fromkeys(("sim", "yes", "✅", "s", "y"), True),
    **dict.fromkeys(("não", "nao", "no", "❌", "n"), False),
}


def _pair(name: str) -> tuple[Path, Path]:
    return ROOT / f"{name}.md", ROOT / f"{name}.en.md"


def _page_id(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


def _assert_equal(path: Path, claim: str, expected: object, found: object) -> None:
    assert found == expected, (
        f"{_page_id(path)}: {claim}; esperado={expected!r}; encontrado={found!r}"
    )


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _tables(text: str) -> Iterator[tuple[list[str], list[list[str]]]]:
    lines = text.splitlines()
    for index, line in enumerate(lines[:-1]):
        if not line.strip().startswith("|"):
            continue
        separator = lines[index + 1].strip().strip("|").split("|")
        if not all(re.fullmatch(r":?-{3,}:?", cell.strip()) for cell in separator):
            continue
        header = [cell.strip() for cell in line.strip().strip("|").split("|")]
        rows = []
        for body in lines[index + 2 :]:
            if not body.strip().startswith("|"):
                break
            rows.append([cell.strip() for cell in body.strip().strip("|").split("|")])
        yield header, rows


def _column_names(cell: str) -> set[str]:
    names = set(re.findall(r"`([^`]+)`", cell))
    for match in re.finditer(r"`([a-z_]+)(\d+)`\s*\.{2,3}\s*`\1(\d+)`", cell):
        prefix, first, last = match.groups()
        names.update(f"{prefix}{index}" for index in range(int(first), int(last) + 1))
    return names


def _schema_names(text: str) -> set[str]:
    return {
        name
        for header, rows in _tables(text)
        if header[0].casefold() in {"coluna", "column", "campo", "field"}
        for row in rows
        for name in _column_names(row[0])
    }


def _contract(path: Path) -> contracts.Contract:
    name = path.name.removesuffix(".md").removesuffix(".en")
    resolved = name if contracts.has_contract(name) else None
    if resolved is None and name in registry.list_datasets():
        resolved = registry.get_dataset(name)._contract_name()
    _assert_equal(
        path, "contrato resolvível", True, bool(resolved and contracts.has_contract(resolved))
    )
    assert resolved is not None
    return contracts.get_contract(resolved)


def _key_names(line: str) -> list[str] | None:
    declaration = re.split(r"[.;]", line, maxsplit=1)[0]
    if re.search(r"\b(?:none|nenhuma|não definida|not defined)\b", declaration, re.IGNORECASE):
        return []
    quoted = re.findall(r"`([^`]+)`|\[([^\]]*)\]", declaration)
    if not quoted:
        return None
    return [name for parts in quoted for name in re.findall(r"[a-zA-Z_]\w*", "".join(parts))]


def _primary_keys(text: str) -> list[list[str]]:
    lines = text.splitlines()
    keys = []
    for index, line in enumerate(lines):
        marker = re.match(r"\*\*(?:PK|Chave primária|Primary key):\*\*(.*)", line, re.IGNORECASE)
        if marker:
            declaration = marker[1]
        elif re.fullmatch(r"## Primary Key\s*", line, re.IGNORECASE):
            declaration = next((value for value in lines[index + 1 :] if value.strip()), "")
        else:
            continue
        names = _key_names(declaration)
        if names is not None:
            keys.append(names)
    return keys


def _products_section(path: Path) -> str:
    match = re.search(r"(?ms)^## (?:Produtos|Products)\s*\n(.*?)(?=^## |\Z)", _read(path))
    _assert_equal(path, "seção Produtos/Products presente", True, match is not None)
    assert match is not None
    return match[1]


def _assert_count(path: Path, text: str, expected: int) -> None:
    found = [
        int(value)
        for value in re.findall(
            r"(\d+)\s+(?:ZARC\s+)?(?:culturas|crops|produtos|products)\b", text, re.IGNORECASE
        )
    ]
    _assert_equal(path, "contagem publicada presente", True, bool(found))
    _assert_equal(path, "contagens publicadas", [expected] * len(found), found)


def test_contagem_de_datasets_nos_catalogos():
    with collect_failures() as check:
        for path in [
            ROOT / "README.md",
            ROOT / "README.pt-BR.md",
            *_pair("docs/index"),
            *_pair("docs/contracts/index"),
            *LANDINGS,
        ]:
            with check(f"test_contagem_de_datasets_nos_catalogos[{(path,)!r}]"):
                found = [int(value) for value in re.findall(r"\b(\d+) datasets\b", _read(path))]
                _assert_equal(path, "contagem de datasets publicada", True, bool(found))
                _assert_equal(
                    path,
                    "datasets registrados",
                    [len(registry.list_datasets())] * len(found),
                    found,
                )
        for path in _pair("docs/contracts/custo_sociobiodiversidade"):
            with check(f"test_sociobiodiversidade_produtos_documentados[{(path,)!r}]"):
                text = _products_section(path)
                _assert_count(path, text, len(registry.list_products("custo_sociobiodiversidade")))
                found = re.findall(r"\| `([^`]+)` \|", text)
                _assert_equal(
                    path,
                    "produtos",
                    sorted(registry.list_products("custo_sociobiodiversidade")),
                    sorted(found),
                )
        for path in _pair("docs/contracts/serie_historica_safra"):
            with check(f"test_serie_historica_contagem_no_contrato[{(path,)!r}]"):
                _assert_count(path, _products_section(path), len(client._PRODUCT_REGISTRY))
        for path in _pair("docs/contracts/serie_historica_safra"):
            with check(f"test_serie_historica_lista_de_produtos[{(path,)!r}]"):
                found = re.findall(r"`([^`]+)`", _products_section(path))
                _assert_equal(
                    path,
                    "produtos da série histórica",
                    sorted(client._PRODUCT_REGISTRY),
                    sorted(found),
                )
        for path in (
            *_pair("docs/contracts/index"),
            *_pair("docs/index"),
            *_pair("docs/api/conab"),
            *_pair("docs/sources/conab"),
            ROOT / "README.md",
            ROOT / "README.pt-BR.md",
        ):
            with check(f"test_serie_historica_contagem_nos_catalogos[{(path,)!r}]"):
                lines = "\n".join(
                    line for line in _read(path).splitlines() if "serie_historica" in line
                )
                _assert_count(path, lines, len(client._PRODUCT_REGISTRY))
        for path in _pair("docs/contracts/condicao_lavouras"):
            with check(f"test_condicao_lavouras_contagem[{(path,)!r}]"):
                _assert_count(
                    path, _products_section(path), len(registry.list_products("condicao_lavouras"))
                )
        for path in _pair("docs/contracts/condicao_lavouras"):
            with check(f"test_condicao_lavouras_lista_de_produtos[{(path,)!r}]"):
                match = re.search(r"\d+ (?:culturas|crops): ([^\n]+)", _products_section(path))
                _assert_equal(path, "lista de produtos presente", True, match is not None)
                assert match is not None
                found = [item.strip() for item in match[1].rstrip(".").split(",")]
                _assert_equal(
                    path,
                    "produtos",
                    sorted(registry.list_products("condicao_lavouras")),
                    sorted(found),
                )
        for path in _pair("docs/contracts/custo_producao"):
            with check(f"test_custo_producao_lista_de_produtos[{(path,)!r}]"):
                found = [
                    name
                    for header, rows in _tables(_products_section(path))
                    if header[0] in {"Código", "Code"}
                    for row in rows
                    for name in re.findall(r"`([^`]+)`", row[0])
                ]
                _assert_equal(
                    path,
                    "produtos",
                    sorted(registry.list_products("custo_producao")),
                    sorted(found),
                )
        for path in _pair("docs/contracts/destinos_anec"):
            with check(f"test_destinos_anec_produtos_presentes[{(path,)!r}]"):
                documented = set(re.findall(r"`([^`]+)`", _read(path)))
                expected = set(registry.list_products("destinos_anec"))
                _assert_equal(
                    path,
                    "produtos anunciados presentes",
                    sorted(expected),
                    sorted(expected & documented),
                )
        for path in _pair("docs/contracts/destinos_anec"):
            with check(f"test_destinos_anec_ddgs_sorgo_apenas_na_exclusao[{(path,)!r}]"):
                sentences = re.split(r"(?<=[.!?])\s+|\n+", _read(path))
                mentions = [
                    line
                    for line in sentences
                    if re.search(r"\b(ddgs|sorgo|sorghum)\b", line, re.IGNORECASE)
                ]
                _assert_equal(path, "exclusão de DDGS/sorgo presente", True, bool(mentions))
                unexpected = [
                    line
                    for line in mentions
                    if not (
                        ("InvalidParameterError" in line and re.search(r"recusados|raise", line))
                        or (
                            "embarques_mensais_anec" in line
                            and "comparacao_anual_anec" in line
                            and re.search(r"continuam|remain", line)
                        )
                    )
                ]
                _assert_equal(path, "menções a DDGS/sorgo fora da exclusão", [], unexpected)
        for path in (
            ROOT / "README.md",
            ROOT / "README.pt-BR.md",
            *_pair("docs/sources/zarc"),
            *_pair("docs/api/zarc"),
        ):
            with check(f"test_zarc_contagem_de_culturas[{(path,)!r}]"):
                text = _read(path)
                if path.name.startswith("README"):
                    text = "\n".join(
                        line for line in text.splitlines() if "zarc.culturas()" in line
                    )
                _assert_count(path, text, len(zarc.culturas()))


def test_contagem_de_fontes_e_contratos_nos_catalogos():
    sources = len(constants.Fonte)
    with collect_failures() as check:
        for path in [
            ROOT / "README.md",
            ROOT / "README.pt-BR.md",
            *_pair("docs/index"),
            *_pair("docs/sources/index"),
            *LANDINGS,
        ]:
            with check(f"test_contagem_de_fontes_nos_catalogos[{(path,)!r}]"):
                found = [int(value) for value in re.findall(SOURCE_COUNT, _read(path))]
                _assert_equal(path, "contagem de fontes publicada", True, bool(found))
                _assert_equal(path, "fontes do enum Fonte", [sources] * len(found), found)
        for path in LANDINGS:
            with check(f"test_contagem_de_fontes_alem_das_destacadas[{(path,)!r}]"):
                footnote = re.search(r'<p class="hero-footnote">(.*?)</p>', _read(path))
                _assert_equal(path, "rodapé do hero presente", True, footnote is not None)
                assert footnote is not None
                spans = re.findall(r"<span>([^<]+)</span>", footnote[1])
                featured = [span for span in spans if not span.startswith("+")]
                remaining = [
                    int(match[1])
                    for span in spans
                    if (match := re.fullmatch(r"\+ (\d+) (?:fontes|sources)", span))
                ]
                _assert_equal(
                    path, "fontes além das destacadas", [sources - len(featured)], remaining
                )
        for path in [
            ROOT / "README.md",
            ROOT / "README.pt-BR.md",
            *_pair("docs/index"),
            *_pair("docs/contracts/index"),
            *LANDINGS,
        ]:
            with check(f"test_contagem_de_contratos_nos_catalogos[{(path,)!r}]"):
                found = [int(value) for value in re.findall(CONTRACT_COUNT, _read(path))]
                _assert_equal(path, "contagem de contratos publicada", True, bool(found))
                _assert_equal(
                    path,
                    "contratos registrados",
                    [len(contracts.list_contracts())] * len(found),
                    found,
                )


def test_contrato_colunas_documentadas():
    with collect_failures() as check:
        for path in [
            path for path in CONTRACT_PAGES if re.match("^# \\S+ v(\\d+\\.\\d+)", _read(path))
        ]:
            with check(f"test_contrato_versao_documentada[{(path,)!r}]"):
                match = re.match(r"^# \S+ v(\d+\.\d+)", _read(path))
                found = match[1] if match else None
                _assert_equal(path, "versão no título", _contract(path).version, found)
        for path in CONTRACT_PAGES:
            with check(f"test_contrato_sem_simbolos_historicos[{(path,)!r}]"):
                found = set(re.findall(r"\b[A-Z_]+_V\d+\b", _read(path)))
                _assert_equal(
                    path, "símbolos de contratos históricos", [], sorted(found & set(dir(_legacy)))
                )
        for path in CONTRACT_PAGES:
            with check(f"test_contrato_colunas_documentadas[{(path,)!r}]"):
                contract = _contract(path)
                expected = {column.name for column in contract.columns}
                found = _schema_names(_read(path))
                name = path.name.removesuffix(".md").removesuffix(".en")
                if name in MULTI_CONTRACT_PAGES:
                    registered = {
                        column.name
                        for name in contracts.list_contracts()
                        for column in contracts.get_contract(name).columns
                    }
                    _assert_equal(path, "colunas ausentes", [], sorted(expected - found))
                    _assert_equal(
                        path, "colunas sem contrato registrado", [], sorted(found - registered)
                    )
                else:
                    _assert_equal(path, "colunas do contrato", sorted(expected), sorted(found))
        for path in CONTRACT_PAGES:
            with check(f"test_contrato_chaves_primarias_documentadas[{(path,)!r}]"):
                contract = _contract(path)
                found = _primary_keys(_read(path))
                if not found:
                    continue
                name = path.name.removesuffix(".md").removesuffix(".en")
                expected = [contract.primary_key] + [
                    contracts.get_contract(additional).primary_key
                    for additional in ADDITIONAL_PRIMARY_KEYS.get(name, ())
                ]
                _assert_equal(
                    path, "chaves primárias, na ordem dos modos documentados", expected, found
                )
        for path in CONTRACT_PAGES:
            with check(f"test_contrato_tipos_e_nulabilidade_documentados[{(path,)!r}]"):
                tables = list(_schema_tables(path))
                _assert_equal(path, "tabela de colunas presente", True, bool(tables))
                for contract, header, rows in tables:
                    type_indexes = [
                        i for i, name in enumerate(header) if name.casefold() in TYPE_HEADERS
                    ]
                    _assert_equal(path, "cabeçalho de tipo reconhecido", 1, len(type_indexes))
                    type_index = type_indexes[0]
                    physical = "pandas" in header[type_index].casefold()
                    null_indexes = [
                        i for i, name in enumerate(header) if name.casefold() in NULL_HEADERS
                    ]
                    columns = {column.name: column for column in contract.columns}
                    dtypes = contract.empty_frame().dtypes
                    for row in rows:
                        _assert_equal(path, f"células de {row[0]}", len(header), len(row))
                        names = _column_names(row[0])
                        _assert_equal(path, f"coluna identificada em {row[0]!r}", True, bool(names))
                        for name in sorted(names):
                            _assert_equal(
                                path, f"{name} pertence a {contract.name}", True, name in columns
                            )
                            column = columns[name]
                            _assert_column_type(
                                path, column, row[type_index], physical, str(dtypes[name])
                            )
                            for index in null_indexes:
                                _assert_column_nullable(path, column, row[index], header[index])


async def test_censo_1985_docs_declaram_pacote_e_cobertura():
    with collect_failures() as check:
        for path in _pair("docs/contracts/censo_agropecuario_municipal_1985"):
            with check(f"test_censo_1985_cobertura_por_tema[{(path,)!r}]"):
                coverage = await ibge.cobertura_censo_agro_municipal_1985()
                states = set().union(*map(set, coverage.values()))
                expected = sorted(
                    (theme, len(available), sorted(states - set(available)))
                    for theme, available in coverage.items()
                )
                found = sorted(
                    (row[0].strip("`"), int(row[1]), re.findall(r"\b[A-Z]{2}\b", row[2]))
                    for header, rows in _tables(_read(path))
                    if header in (["Tema", "UFs", "Faltantes"], ["Theme", "States", "Missing"])
                    for row in rows
                )
                _assert_equal(path, "tema, número de UFs e UFs faltantes", expected, found)
        for language in ["", ".en"]:
            with check(f"test_censo_1985_docs_declaram_pacote_e_cobertura[{(language,)!r}]"):
                count = len(await ibge.temas_censo_agro_municipal_1985())
                for relative in (
                    "contracts/censo_agropecuario_municipal_1985",
                    "sources/ibge",
                    "api/ibge",
                ):
                    text = _read(ROOT / f"docs/{relative}{language}.md")
                    assert str(count) in text
                    assert "valor_lido" in text
                    assert "status" in text
                    assert "quarantin" not in text.lower()
                    assert "quarentena" not in text.lower()


def _table_contract(path: Path, heading: str) -> contracts.Contract:
    name = path.name.removesuffix(".md").removesuffix(".en")
    if name not in SCHEMA_CONTRACTS:
        return _contract(path)
    found = [
        registered for pattern, registered in SCHEMA_CONTRACTS[name] if re.search(pattern, heading)
    ]
    _assert_equal(path, f"contrato da seção {heading!r}", 1, len(found))
    return contracts.get_contract(found[0])


def _schema_tables(
    path: Path,
) -> Iterator[tuple[contracts.Contract, list[str], list[list[str]]]]:
    sections = re.split(r"(?m)^(#{1,6} .*)$", _read(path))
    for heading, body in zip(sections[1::2], sections[2::2], strict=True):
        for header, rows in _tables(body):
            if header[0].casefold() in {"coluna", "column"}:
                yield _table_contract(path, heading), header, rows


def _assert_column_type(
    path: Path,
    column: contracts.Column,
    cell: str,
    physical: bool,
    pandas_dtype: str,
) -> None:
    token = re.split(r"[\s(,\[]", cell.strip("`"), maxsplit=1)[0].casefold()
    claim = f"{column.name}: tipo {cell!r}"
    if physical and token in {"object", "datetime64"}:
        _assert_equal(path, claim, pandas_dtype.split("[")[0].casefold(), token)
        return
    _assert_equal(path, f"{claim}, grafia conhecida", True, token in TYPE_ALIASES)
    _assert_equal(path, claim, column.type.name, TYPE_ALIASES[token])


def _assert_column_nullable(path: Path, column: contracts.Column, cell: str, header: str) -> None:
    if not cell:
        return
    token = cell.casefold()
    claim = f"{column.name}: {header} {cell!r}"
    _assert_equal(path, f"{claim}, grafia conhecida", True, token in NULL_ALIASES)
    nullable = NULL_ALIASES[token]
    if header.casefold() in {"obrigatório", "required"}:
        nullable = not nullable
    _assert_equal(path, claim, column.nullable, nullable)


def test_toda_pagina_tem_as_cercas_de_codigo_em_par():
    paginas = [*sorted((ROOT / "docs").rglob("*.md")), ROOT / "README.md", ROOT / "README.pt-BR.md"]
    impares = [
        pagina.relative_to(ROOT).as_posix()
        for pagina in paginas
        if sum(
            linha.lstrip().startswith("```")
            for linha in pagina.read_text(encoding="utf-8").splitlines()
        )
        % 2
    ]

    assert len(paginas) > 300
    assert impares == []


def test_contagem_em_negrito_da_pagina_de_contrato_confere_com_o_contrato():
    contagens = {
        path.relative_to(ROOT).as_posix(): (int(match[1]), len(_contract(path).columns))
        for path in CONTRACT_PAGES
        if (match := re.search(r"\*\*(\d+) (?:colunas|columns)\*\*", _read(path)))
    }

    assert len(contagens) == 4
    assert {pagina: par for pagina, par in contagens.items() if par[0] != par[1]} == {}
