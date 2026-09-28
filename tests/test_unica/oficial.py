from __future__ import annotations

import calendar
import hashlib
import io
import json
import re
from pathlib import Path
from typing import Any

import openpyxl
import pandas as pd

GOLDEN = Path(__file__).parents[1] / "golden_data" / "unica"
OFICIAL = GOLDEN / "oficial_20260923"
LISTAGEM = "https://unicadata.com.br/listagem.php?idMn=63"
PDF_URL = "https://unicadata.com.br/arquivos/pdfs/2026/08/c849215ab49b6cc499a9811e3b469342.pdf"
EDICOES = {
    "01/07/2026": OFICIAL / "relatorio_20260701.pdf",
    "01/05/2026": GOLDEN / "relatorio_quinzenal.pdf",
}
COLUNAS_RESUMO = [
    "produto",
    "regiao",
    "safra",
    "periodo",
    "data_inicio",
    "data_fim",
    "valor",
    "valor_safra_anterior",
    "variacao_pct",
    "unidade",
]
COLUNAS_SERIES = [
    "data",
    "quinzena",
    "safra",
    "produto",
    "regiao",
    "valor",
    "valor_safra_anterior",
    "variacao_pct",
    "unidade",
]
MESES = {
    "janeiro": 1,
    "fevereiro": 2,
    "marco": 3,
    "março": 3,
    "abril": 4,
    "maio": 5,
    "junho": 6,
    "julho": 7,
    "agosto": 8,
    "setembro": 9,
    "outubro": 10,
    "novembro": 11,
    "dezembro": 12,
}
PRODUTOS_RESUMO = {
    "Cana-de-açúcar": "cana",
    "Açúcar": "acucar",
    "Etanol anidro": "etanol_anidro",
    "Etanol hidratado": "etanol_hidratado",
    "Etanol total": "etanol_total",
    "ATR": "atr",
    "ATR/ tonelada de cana": "atr_por_tonelada",
    "Mix (%) açúcar": "mix_acucar",
    "Mix (%) etanol": "mix_etanol",
    "Litros etanol/ tonelada de cana": "litros_etanol_por_tonelada",
    "Kg açúcar/ tonelada de cana": "kg_acucar_por_tonelada",
}
NOTAS = {"¹": "mil_t", "²": "mi_litros", "³": "kg_t"}
PRODUTOS_SERIE = {
    3: "cana",
    4: "acucar",
    5: "etanol_total",
    6: "etanol_anidro",
    7: "etanol_hidratado",
}
UFS = {
    "Acre": "AC",
    "Alagoas": "AL",
    "Amapá": "AP",
    "Amazonas": "AM",
    "Bahia": "BA",
    "Ceará": "CE",
    "Distrito Federal": "DF",
    "Espírito Santo": "ES",
    "Goiás": "GO",
    "Maranhão": "MA",
    "Mato Grosso": "MT",
    "Mato Grosso do Sul": "MS",
    "Minas Gerais": "MG",
    "Pará": "PA",
    "Paraíba": "PB",
    "Paraná": "PR",
    "Pernambuco": "PE",
    "Piauí": "PI",
    "Rio de Janeiro": "RJ",
    "Rio Grande do Norte": "RN",
    "Rio Grande do Sul": "RS",
    "Rondônia": "RO",
    "Roraima": "RR",
    "Santa Catarina": "SC",
    "São Paulo": "SP",
    "Sergipe": "SE",
    "Tocantins": "TO",
    "Região Centro-Sul": "centro_sul",
    "Região Norte-Nordeste": "norte_nordeste",
    "Brasil": "brasil",
}


def manifest() -> dict[str, Any]:
    dados: dict[str, Any] = json.loads((OFICIAL / "manifest.json").read_bytes())
    for resposta in dados["respostas"]:
        corpo = (OFICIAL / resposta["arquivo"]).read_bytes()
        assert hashlib.sha256(corpo).hexdigest() == resposta["sha256"], resposta["arquivo"]
    for anterior in dados["goldens_anteriores"]:
        corpo = (OFICIAL / anterior["arquivo"]).read_bytes()
        assert hashlib.sha256(corpo).hexdigest() == anterior["sha256"], anterior["arquivo"]
    return dados


def corpo(arquivo: str) -> bytes:
    manifest()
    return (OFICIAL / arquivo).read_bytes()


def oraculo(edicao: str) -> dict[str, Any]:
    sha = hashlib.sha256(EDICOES[edicao].read_bytes()).hexdigest()
    dados = json.loads((OFICIAL / "oraculo_20260923.json").read_bytes())
    encontrada: dict[str, Any] = next(e for e in dados["edicoes"] if e["sha256"] == sha)
    return encontrada


def numero(texto: str | None) -> float | None:
    if texto is None:
        return None
    return float(texto.rstrip("%").replace(".", "").replace(",", "."))


def _data(dia: str, mes: str, ano: str) -> pd.Timestamp:
    return pd.Timestamp(int(ano), MESES[mes.lower()], int(dia))


def periodo(titulo: str) -> tuple[str, pd.Timestamp, pd.Timestamp]:
    data = r"(\d{1,2})º? de (\w+) de (\d{4})"
    if achado := re.search(rf"ACUMULADA entre {data} até {data}", titulo):
        dia1, mes1, ano1, dia2, mes2, ano2 = achado.groups()
        return "acumulado", _data(dia1, mes1, ano1), _data(dia2, mes2, ano2)
    if achado := re.search(r"MENSAL referente a (\w+) de (\d{4})", titulo):
        mes, ano = MESES[achado[1].lower()], int(achado[2])
        return (
            "mensal",
            pd.Timestamp(ano, mes, 1),
            pd.Timestamp(ano, mes, calendar.monthrange(ano, mes)[1]),
        )
    achado = re.search(r"QUINZENAL referente à ([12])ª quinzena de (\w+) de (\d{4})", titulo)
    assert achado, titulo
    mes, ano = MESES[achado[2].lower()], int(achado[3])
    if achado[1] == "1":
        return "quinzena", pd.Timestamp(ano, mes, 1), pd.Timestamp(ano, mes, 15)
    return (
        "quinzena",
        pd.Timestamp(ano, mes, 16),
        pd.Timestamp(ano, mes, calendar.monthrange(ano, mes)[1]),
    )


def esperado_resumo(edicao: str, nome_periodo: str) -> list[dict[str, Any]]:
    dados = oraculo(edicao)
    safra = re.search(r"\d{4}/\d{4}", dados["capa"]).group(0)
    linhas: list[dict[str, Any]] = []
    for tabela in dados["tabelas"]:
        if tabela["tabela"] not in (1, 2):
            continue
        rotulo_periodo, inicio, fim = periodo(tabela["titulo"])
        if rotulo_periodo != nome_periodo:
            continue
        for linha in tabela["linhas"]:
            rotulo = linha["rotulo"].rstrip("¹²³ ").strip()
            if rotulo not in PRODUTOS_RESUMO:
                continue
            nota = linha["rotulo"][-1]
            produto = PRODUTOS_RESUMO[rotulo]
            if produto.startswith("mix_"):
                unidade = "pct"
            elif nota in NOTAS:
                unidade = NOTAS[nota]
            else:
                unidade = "l_t" if rotulo.startswith("Litros") else "kg_t"
            celulas = linha["celulas"]
            for regiao, base in (("centro_sul", 0), ("sao_paulo", 3), ("demais_estados", 6)):
                linhas.append(
                    {
                        "produto": produto,
                        "regiao": regiao,
                        "safra": safra,
                        "periodo": rotulo_periodo,
                        "data_inicio": inicio,
                        "data_fim": fim,
                        "valor": numero(celulas[base + 1]),
                        "valor_safra_anterior": numero(celulas[base]),
                        "variacao_pct": numero(celulas[base + 2]),
                        "unidade": unidade,
                    }
                )
    return linhas


def periodos(edicao: str) -> list[str]:
    return sorted(
        periodo(t["titulo"])[0] for t in oraculo(edicao)["tabelas"] if t["tabela"] in (1, 2)
    )


def _data_quinzena(quinzena: str, safra: str) -> pd.Timestamp:
    dia, mes = (int(p) for p in quinzena.split("/"))
    inicio = int(safra[:4])
    return pd.Timestamp(inicio if (mes, dia) >= (4, 16) else inicio + 1, mes, dia)


def esperado_series(edicao: str, produto: str) -> list[dict[str, Any]]:
    dados = oraculo(edicao)
    safra = re.search(r"\d{4}/\d{4}", dados["capa"]).group(0)
    tabela = next(t for t in dados["tabelas"] if PRODUTOS_SERIE.get(t["tabela"]) == produto)
    unidade = "t" if "(toneladas)" in tabela["cabecalho_unidade"] else "m3"
    linhas: list[dict[str, Any]] = []
    for linha in tabela["linhas"]:
        celulas = linha["celulas"]
        for regiao, base in (("sao_paulo", 0), ("centro_sul", 3), ("demais_estados", 6)):
            linhas.append(
                {
                    "data": _data_quinzena(linha["quinzena"], safra),
                    "quinzena": linha["quinzena"],
                    "safra": safra,
                    "produto": produto,
                    "regiao": regiao,
                    "valor": numero(celulas[base + 1]),
                    "valor_safra_anterior": numero(celulas[base]),
                    "variacao_pct": numero(celulas[base + 2]),
                    "unidade": unidade,
                }
            )
    return sorted(linhas, key=lambda r: (r["data"], r["regiao"]))


def esperado_historico(arquivo: Path, produto: str) -> list[dict[str, Any]]:
    livro = openpyxl.load_workbook(io.BytesIO(arquivo.read_bytes()), data_only=True, read_only=True)
    grade = [list(linha) for linha in livro.active.iter_rows(values_only=True)]
    livro.close()
    unidade_texto = next(
        str(c) for linha in grade for c in linha if c is not None and "Unidade:" in str(c)
    )
    unidade = "mil_t" if "tonelada" in unidade_texto.lower() else "mil_m3"
    cabecalho = next(
        i for i, linha in enumerate(grade) if str(linha[0]).strip().startswith("Estado")
    )
    safras = [
        (j, str(c).strip())
        for j, c in enumerate(grade[cabecalho])
        if c and re.fullmatch(r"\d{4}/\d{4}", str(c).strip())
    ]
    linhas: list[dict[str, Any]] = []
    for linha in grade[cabecalho + 1 :]:
        nome = str(linha[0]).strip() if linha[0] is not None else ""
        if not nome:
            continue
        if nome.lower().startswith(("source", "fonte")):
            break
        for j, safra in safras:
            valor = linha[j]
            linhas.append(
                {
                    "safra": safra,
                    "localidade": UFS[nome],
                    "produto": produto,
                    "valor": float(valor) if isinstance(valor, (int, float)) else None,
                    "unidade": unidade,
                }
            )
    return sorted(linhas, key=lambda r: (r["safra"], r["localidade"]))


def publicado(frame: pd.DataFrame) -> list[dict[str, Any]]:
    linhas = []
    for registro in frame.to_dict("records"):
        linhas.append(
            {
                str(coluna): None
                if valor is None or (not isinstance(valor, (str, pd.Timestamp)) and pd.isna(valor))
                else valor.item()
                if hasattr(valor, "item") and not isinstance(valor, pd.Timestamp)
                else valor
                for coluna, valor in registro.items()
            }
        )
    return linhas
