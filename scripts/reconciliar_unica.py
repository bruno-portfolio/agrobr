from __future__ import annotations

import argparse
import asyncio
import calendar
import hashlib
import io
import json
import re
import warnings
from collections.abc import Awaitable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlencode

import httpx
import openpyxl
import pandas as pd

BASE = "https://unicadata.com.br"
LISTAGEM = f"{BASE}/listagem.php?idMn=63"
HISTORICO = f"{BASE}/xlsHPM.php"
UFS_FORM = "RS,SC,PR,SP,RJ,MG,ES,MS,MT,GO,DF,BA,SE,AL,PE,PB,RN,CE,PI,MA,TO,PA,AP,RO,AM,AC,RR"
AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:143.0) Gecko/20100101 Firefox/143.0"
TITULO = re.compile(r"^Tabela (\d+)\.")
QUINZENA = re.compile(r"^\d{2}/\d{2}$")
AUSENTE = {"n/d", "nd", "-", "–", "—"}
MESES = {
    "janeiro": 1,
    "fevereiro": 2,
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
PRODUTOS_HISTORICO = ("cana", "acucar", "etanol_anidro", "etanol_hidratado", "etanol_total")
PERIODOS: tuple[Literal["acumulado", "quinzena", "mensal"], ...] = (
    "acumulado",
    "quinzena",
    "mensal",
)
LOCALIDADES = {
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


def linhas(pagina: Any) -> list[list[dict[str, Any]]]:
    grupos: list[tuple[float, list[dict[str, Any]]]] = []
    for palavra in sorted(pagina.extract_words(), key=lambda w: (w["top"], w["x0"])):
        for topo, grupo in grupos:
            if abs(topo - palavra["top"]) < 2.5:
                grupo.append(palavra)
                break
        else:
            grupos.append((palavra["top"], [palavra]))
    return [sorted(grupo, key=lambda w: w["x0"]) for _, grupo in grupos]


def faixas(cabecalho: list[dict[str, Any]]) -> list[tuple[float, float]]:
    ancoras = [w["x0"] for w in cabecalho if w["text"] in ("2025/2026", "2026/2027", "Var.")]
    if len(ancoras) != 9:
        ancoras = [w["x0"] for w in cabecalho if re.fullmatch(r"\d{4}/\d{4}|Var\.", w["text"])]
    cortes = [(a + b) / 2 for a, b in zip(ancoras, ancoras[1:], strict=False)]
    return list(zip([ancoras[0] - 40, *cortes], [*cortes, ancoras[-1] + 100], strict=True))


def celulas(palavras: list[dict[str, Any]], limites: list[tuple[float, float]]) -> list[str | None]:
    return [
        " ".join(w["text"] for w in palavras if inicio <= w["x0"] < fim) or None
        for inicio, fim in limites
    ]


def cabecalho_de_colunas(palavras: list[dict[str, Any]]) -> bool:
    return sum(bool(re.fullmatch(r"\d{4}/\d{4}", w["text"])) for w in palavras) == 6


def extrair(corpo: bytes) -> dict[str, Any]:
    import pdfplumber

    tabelas: dict[int, dict[str, Any]] = {}
    with pdfplumber.open(io.BytesIO(corpo)) as pdf:
        capa = pdf.pages[0].extract_text() or ""
        for pagina in pdf.pages:
            atual: dict[str, Any] | None = None
            limites: list[tuple[float, float]] = []
            for palavras in linhas(pagina):
                texto = " ".join(w["text"] for w in palavras)
                titulo = TITULO.match(texto)
                if titulo and int(titulo.group(1)) <= 7:
                    atual = {"titulo": texto, "linhas": [], "unidade": ""}
                    tabelas[int(titulo.group(1))] = atual
                    continue
                if atual is None or texto.startswith("Fonte:"):
                    atual = None
                    continue
                if "(toneladas)" in texto or "(m³)" in texto:
                    atual["unidade"] = texto
                    continue
                if cabecalho_de_colunas(palavras):
                    limites = faixas(palavras)
                    continue
                if not limites:
                    continue
                if QUINZENA.match(palavras[0]["text"]):
                    if len(palavras) > 1:
                        atual["linhas"].append(
                            (palavras[0]["text"], celulas(palavras[1:], limites))
                        )
                    continue
                rotulo = " ".join(w["text"] for w in palavras if w["x0"] < limites[0][0])
                valores = celulas(palavras, limites)
                if any(valores):
                    rotulo = f"Mix (%) {rotulo}" if rotulo in ("açúcar", "etanol") else rotulo
                    atual["linhas"].append((rotulo, valores))
    safra = re.search(r"\d{4}/\d{4}", capa)
    return {"safra": safra.group(0) if safra else "", "tabelas": tabelas}


def numero(texto: str | None) -> float | None:
    if texto is None or texto.lower() in AUSENTE:
        return None
    return float(texto.rstrip("%").replace(".", "").replace(",", "."))


def periodo(titulo: str) -> tuple[str, pd.Timestamp, pd.Timestamp] | None:
    data = r"(\d{1,2})º? de (\w+) de (\d{4})"
    if achado := re.search(rf"ACUMULADA entre {data} até {data}", titulo):
        d1, m1, a1, d2, m2, a2 = achado.groups()
        return (
            "acumulado",
            pd.Timestamp(int(a1), MESES[m1.lower()], int(d1)),
            pd.Timestamp(int(a2), MESES[m2.lower()], int(d2)),
        )
    if achado := re.search(r"MENSAL referente a (\w+) de (\d{4})", titulo):
        ano, mes = int(achado[2]), MESES[achado[1].lower()]
        return (
            "mensal",
            pd.Timestamp(ano, mes, 1),
            pd.Timestamp(ano, mes, calendar.monthrange(ano, mes)[1]),
        )
    if achado := re.search(r"QUINZENAL referente à ([12])ª quinzena de (\w+) de (\d{4})", titulo):
        ano, mes = int(achado[3]), MESES[achado[2].lower()]
        inicio, fim = (1, 15) if achado[1] == "1" else (16, calendar.monthrange(ano, mes)[1])
        return "quinzena", pd.Timestamp(ano, mes, inicio), pd.Timestamp(ano, mes, fim)
    return None


def esperado_resumo(
    extracao: dict[str, Any], numero_tabela: int
) -> tuple[str, list[dict[str, Any]]]:
    tabela = extracao["tabelas"][numero_tabela]
    lido = periodo(tabela["titulo"])
    if lido is None:
        return "desconhecido", []
    rotulo_periodo, inicio, fim = lido
    saida: list[dict[str, Any]] = []
    for rotulo_bruto, valores in tabela["linhas"]:
        rotulo = rotulo_bruto.rstrip("¹²³ ").strip()
        if rotulo not in PRODUTOS_RESUMO:
            continue
        produto = PRODUTOS_RESUMO[rotulo]
        if produto.startswith("mix_"):
            unidade = "pct"
        elif rotulo_bruto[-1] in NOTAS:
            unidade = NOTAS[rotulo_bruto[-1]]
        else:
            unidade = "l_t" if rotulo.startswith("Litros") else "kg_t"
        for regiao, base in (("centro_sul", 0), ("sao_paulo", 3), ("demais_estados", 6)):
            saida.append(
                {
                    "produto": produto,
                    "regiao": regiao,
                    "safra": extracao["safra"],
                    "periodo": rotulo_periodo,
                    "data_inicio": inicio,
                    "data_fim": fim,
                    "valor": numero(valores[base + 1]),
                    "valor_safra_anterior": numero(valores[base]),
                    "variacao_pct": numero(valores[base + 2]),
                    "unidade": unidade,
                }
            )
    return rotulo_periodo, saida


def esperado_serie(extracao: dict[str, Any], numero_tabela: int) -> list[dict[str, Any]]:
    tabela = extracao["tabelas"][numero_tabela]
    unidade = "t" if "(toneladas)" in tabela["unidade"] else "m3"
    inicio = int(extracao["safra"][:4])
    saida: list[dict[str, Any]] = []
    for quinzena, valores in tabela["linhas"]:
        dia, mes = (int(p) for p in quinzena.split("/"))
        data = pd.Timestamp(inicio if (mes, dia) >= (4, 16) else inicio + 1, mes, dia)
        for regiao, base in (("sao_paulo", 0), ("centro_sul", 3), ("demais_estados", 6)):
            saida.append(
                {
                    "data": data,
                    "quinzena": quinzena,
                    "safra": extracao["safra"],
                    "produto": PRODUTOS_SERIE[numero_tabela],
                    "regiao": regiao,
                    "valor": numero(valores[base + 1]),
                    "valor_safra_anterior": numero(valores[base]),
                    "variacao_pct": numero(valores[base + 2]),
                    "unidade": unidade,
                }
            )
    return sorted(saida, key=lambda r: (r["data"], r["regiao"]))


def esperado_historico(corpo: bytes, produto: str) -> list[dict[str, Any]]:
    livro = openpyxl.load_workbook(io.BytesIO(corpo), data_only=True, read_only=True)
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
    saida: list[dict[str, Any]] = []
    for linha in grade[cabecalho + 1 :]:
        nome = str(linha[0]).strip() if linha[0] is not None else ""
        if not nome:
            continue
        if nome.lower().startswith(("source", "fonte")):
            break
        for j, safra in safras:
            valor = linha[j]
            saida.append(
                {
                    "safra": safra,
                    "localidade": LOCALIDADES.get(nome, f"?{nome}"),
                    "produto": produto,
                    "valor": float(valor) if isinstance(valor, (int, float)) else None,
                    "unidade": unidade,
                }
            )
    return sorted(saida, key=lambda r: (r["safra"], r["localidade"]))


def publicado(frame: pd.DataFrame) -> list[dict[str, Any]]:
    saida = []
    for registro in frame.to_dict("records"):
        linha: dict[str, Any] = {}
        for coluna, valor in registro.items():
            if valor is None or (not isinstance(valor, (str, pd.Timestamp)) and pd.isna(valor)):
                linha[str(coluna)] = None
            elif hasattr(valor, "item") and not isinstance(valor, pd.Timestamp):
                linha[str(coluna)] = valor.item()
            else:
                linha[str(coluna)] = valor
        saida.append(linha)
    return saida


def comparar(observado: list[dict[str, Any]], esperado: list[dict[str, Any]]) -> dict[str, Any]:
    problemas = []
    if observado != esperado:
        divergentes = sum(1 for a, b in zip(observado, esperado, strict=False) if a != b)
        problemas.append(
            f"agrobr {len(observado)} × oficial {len(esperado)} linhas; {divergentes} divergentes"
        )
    return {
        "status": "ok" if not problemas else "mismatch",
        "problems": problemas,
        "linhas": len(esperado),
        "celulas": sum(len(r) for r in esperado),
    }


async def tentar(chamada: Awaitable[pd.DataFrame]) -> pd.DataFrame | Exception:
    from agrobr.exceptions import AgrobrError

    try:
        return await chamada
    except AgrobrError as exc:
        return exc


async def saidas(periodos: list[str]) -> dict[str, Any]:
    from agrobr import unica

    resultado: dict[str, Any] = {}
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        for nome in PERIODOS:
            if nome in periodos:
                resultado[f"resumo_{nome}"] = await tentar(unica.safra_resumo(periodo=nome))
        for produto in PRODUTOS_SERIE.values():
            resultado[f"serie_{produto}"] = await tentar(unica.moagem_quinzenal(produto))
        for produto in PRODUTOS_HISTORICO:
            resultado[f"historico_{produto}"] = await tentar(unica.producao_historica(produto))
    return resultado


def conferir(
    publicado_ou_erro: pd.DataFrame | Exception, esperado: list[dict[str, Any]], chave: list[str]
) -> dict[str, Any]:
    if isinstance(publicado_ou_erro, Exception):
        return {
            "status": "mismatch",
            "problems": [f"{type(publicado_ou_erro).__name__}: {str(publicado_ou_erro)[:200]}"],
            "linhas": len(esperado),
            "celulas": sum(len(r) for r in esperado),
        }
    observado = sorted(publicado(publicado_ou_erro), key=lambda r: tuple(r[c] for c in chave))
    return comparar(observado, esperado)


def run(saida: Path) -> int:
    resultados: dict[str, Any] = {}
    with httpx.Client(
        headers={"User-Agent": AGENT}, timeout=httpx.Timeout(60, read=180), follow_redirects=True
    ) as cliente:
        pagina = cliente.get(LISTAGEM)
        pagina.raise_for_status()
        caminhos = re.findall(
            r"arquivos/pdfs/\d{4}/\d{2}/[0-9a-f]{32}\.pdf", pagina.content.decode("latin-1")
        )
        pdf = cliente.get(f"{BASE}/{caminhos[0]}")
        pdf.raise_for_status()
        extracao = extrair(pdf.content)
        planilhas = {}
        for produto in PRODUTOS_HISTORICO:
            params = {
                "idioma": "1",
                "tipoHistorico": "2",
                "idTabela": "2494",
                "produto": produto,
                "safra": "",
                "safraIni": "1980/1981",
                "safraFim": "2020/2021",
                "estado": UFS_FORM,
            }
            resposta = cliente.get(f"{HISTORICO}?{urlencode(params)}")
            resposta.raise_for_status()
            planilhas[produto] = resposta.content
    esperados_resumo = dict(esperado_resumo(extracao, n) for n in (1, 2))
    resultados["edicao"] = {
        "status": "ok" if "desconhecido" not in esperados_resumo else "mismatch",
        "problems": []
        if "desconhecido" not in esperados_resumo
        else ["título de período desconhecido"],
        "pdf": f"{BASE}/{caminhos[0]}",
        "sha256": hashlib.sha256(pdf.content).hexdigest(),
        "last_modified": pdf.headers.get("last-modified"),
        "periodos": sorted(esperados_resumo),
        "titulos": [extracao["tabelas"][n]["titulo"] for n in (1, 2)],
    }
    publicados = asyncio.run(saidas(sorted(p for p in esperados_resumo if p != "desconhecido")))
    for nome, linhas_esperadas in esperados_resumo.items():
        if nome != "desconhecido":
            resultados[f"resumo_{nome}"] = conferir(
                publicados[f"resumo_{nome}"], linhas_esperadas, ["periodo"]
            )
    for numero_tabela, produto in PRODUTOS_SERIE.items():
        resultados[f"serie_{produto}"] = conferir(
            publicados[f"serie_{produto}"],
            esperado_serie(extracao, numero_tabela),
            ["data", "regiao"],
        )
    for produto in PRODUTOS_HISTORICO:
        resultados[f"historico_{produto}"] = conferir(
            publicados[f"historico_{produto}"],
            esperado_historico(planilhas[produto], produto),
            ["safra", "localidade"],
        )
    for nome, check in resultados.items():
        print(nome, check["status"], check["problems"][:3], flush=True)
    relatorio = {
        "checked_at": datetime.now(UTC).isoformat(),
        "scope": (
            "PDF corrente da listagem idMn=63 lido por posição de palavra (colunas pelo cabeçalho) × safra_resumo "
            "(cada período da edição) e moagem_quinzenal (5 produtos); XLSX do histórico lido com openpyxl × "
            "producao_historica (5 produtos, 1980/1981–2020/2021)"
        ),
        "checks": resultados,
    }
    saida.parent.mkdir(parents=True, exist_ok=True)
    saida.write_text(
        json.dumps(relatorio, indent=2, ensure_ascii=False, default=str), encoding="utf-8"
    )
    falhas = sum(check["status"] != "ok" for check in resultados.values())
    print(f"{len(resultados) - falhas} ok / {falhas} mismatch")
    return int(falhas > 0)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Confere ao vivo o relatório e o histórico da UNICA"
    )
    parser.add_argument("--output", required=True, type=Path)
    return run(parser.parse_args().output)


if __name__ == "__main__":
    raise SystemExit(main())
