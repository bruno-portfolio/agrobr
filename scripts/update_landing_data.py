"""Regenera os dados vivos das landings (PT e EN) via agrobr.

Substitui as regiões entre âncoras <!-- agrobr:X --> ... <!-- /agrobr:X -->
em index.html e en/index.html. Aborta sem tocar os arquivos se a coleta
falhar nos sanity checks.
"""

from __future__ import annotations

import argparse
import asyncio
import re
import sys
from pathlib import Path
from typing import Any, cast

import pandas as pd

from agrobr.utils.atomic import atomic_output

ROOT = Path(__file__).resolve().parent.parent

PRODUTOS_TICKER = ["soja", "milho", "boi", "cafe", "trigo", "algodao"]

LOCALES: dict[str, dict[str, Any]] = {
    "pt": {
        "index": ROOT / "index.html",
        "labels": {
            "soja": "soja",
            "milho": "milho",
            "boi": "boi gordo",
            "cafe": "café",
            "trigo": "trigo",
            "algodao": "algodão",
        },
        "meses": {
            1: "jan",
            2: "fev",
            3: "mar",
            4: "abr",
            5: "mai",
            6: "jun",
            7: "jul",
            8: "ago",
            9: "set",
            10: "out",
            11: "nov",
            12: "dez",
        },
        "data_fmt": "{dia:02d} {mes} {ano}",
        "stamp": "indicadores CEPEA · {data} · coletados via agrobr",
        "spark_aria": "Últimos {n} pregões da soja",
        "decimal": ",",
        "milhar": ".",
        "unidades": {
            "BRL/sc60kg": "/ sc 60kg",
            "BRL/sc50kg": "/ sc 50kg",
            "BRL/ton": "/ ton",
            "BRL/@": "/ @",
            "BRL/kg": "/ kg",
            "BRL/L": "/ L",
            "BRL/lb": "/ lb",
        },
    },
    "en": {
        "index": ROOT / "en" / "index.html",
        "labels": {
            "soja": "soybean",
            "milho": "corn",
            "boi": "live cattle",
            "cafe": "coffee",
            "trigo": "wheat",
            "algodao": "cotton",
        },
        "meses": {
            1: "Jan",
            2: "Feb",
            3: "Mar",
            4: "Apr",
            5: "May",
            6: "Jun",
            7: "Jul",
            8: "Aug",
            9: "Sep",
            10: "Oct",
            11: "Nov",
            12: "Dec",
        },
        "data_fmt": "{mes} {dia:02d}, {ano}",
        "stamp": "CEPEA indicators · {data} · fetched via agrobr",
        "spark_aria": "Last {n} soybean trading days",
        "decimal": ".",
        "milhar": ",",
        "unidades": {
            "BRL/sc60kg": "/ 60kg bag",
            "BRL/sc50kg": "/ 50kg bag",
            "BRL/ton": "/ ton",
            "BRL/@": "/ @ (15kg)",
            "BRL/kg": "/ kg",
            "BRL/L": "/ L",
            "BRL/lb": "/ lb",
        },
    },
}


def fmt_valor(valor: float, loc: dict[str, Any]) -> str:
    s = f"{valor:,.2f}"
    return s.replace(",", "\x00").replace(".", loc["decimal"]).replace("\x00", loc["milhar"])


def fmt_data(data: Any, loc: dict[str, Any]) -> str:
    return str(loc["data_fmt"]).format(dia=data.day, mes=loc["meses"][data.month], ano=data.year)


def fmt_preco(valor: float, unidade: str, loc: dict[str, Any]) -> str:
    if unidade == "cBRL/lb":
        valor /= 100
        unidade = "BRL/lb"
    if unidade not in loc["unidades"]:
        raise ValueError(f"Unidade de preco nao suportada: {unidade}")
    return f"R$ {fmt_valor(valor, loc)} {loc['unidades'][unidade]}"


async def coletar() -> tuple[list[dict[str, Any]], dict[str, Any]]:
    from agrobr import datasets

    ticker: list[dict[str, Any]] = []
    soja: dict[str, Any] = {}

    for key in PRODUTOS_TICKER:
        df = cast(pd.DataFrame, await datasets.preco_diario(key))
        df = df.sort_values("data").reset_index(drop=True)
        if len(df) < 2:
            raise RuntimeError(f"{key}: menos de 2 pregoes ({len(df)})")

        atual = float(df["valor"].iloc[-1])
        anterior = float(df["valor"].iloc[-2])
        unidade = str(df["unidade"].iloc[-1])
        if str(df["unidade"].iloc[-2]) != unidade:
            raise RuntimeError(f"{key}: unidades diferentes nos dois ultimos pregoes")
        if not (0 < atual < 100_000):
            raise RuntimeError(f"{key}: valor implausivel {atual}")

        ticker.append(
            {
                "key": key,
                "valor": atual,
                "unidade": unidade,
                "var_pct": (atual / anterior - 1) * 100,
                "data": df["data"].iloc[-1],
            }
        )

        if key == "soja":
            serie = df.tail(30)
            soja = {
                "valores": [float(v) for v in serie["valor"]],
                "datas": list(serie["data"]),
                "unidade": str(serie["unidade"].iloc[-1]),
                "total_pregoes": len(df),
            }

    if len(ticker) != len(PRODUTOS_TICKER):
        raise RuntimeError("coleta incompleta do ticker")
    if len(soja.get("valores", [])) < 10:
        raise RuntimeError("serie da soja curta demais para o sparkline")

    return ticker, soja


def render_ticker(ticker: list[dict[str, Any]], loc: dict[str, Any]) -> str:
    english = loc["decimal"] == "."
    rows = []
    for item in ticker:
        variation = item["var_pct"]
        arrow, direction = ("↑", "up") if variation >= 0 else ("↓", "down")
        percentage = fmt_valor(abs(variation), loc)
        rows.append(
            f'<span class="ticker-item"><span class="tk-name">{loc["labels"][item["key"]].upper()}</span>'
            f'<span class="tk-price">{fmt_preco(item["valor"], item["unidade"], loc)}</span>'
            f'<span class="{direction}">{arrow} {percentage}%</span></span>'
        )
    date = fmt_data(max(item["data"] for item in ticker), loc)
    stamp = str(loc["stamp"]).format(data=date)
    title = "FETCHED VIA AGROBR" if english else "COLETADO VIA AGROBR"
    hint = (
        "Price indicators; animation pauses on hover"
        if english
        else "Indicadores de preços; a animação pausa ao passar o mouse"
    )
    pause = "Pause quotes" if english else "Pausar cotações"
    return (
        f'<div class="market-strip" aria-label="{stamp}"><div class="container market-inner">\n'
        f'<p class="market-label"><strong>{title}</strong><span>CEPEA · {date}</span></p>\n'
        f'<div class="ticker-window" tabindex="0" aria-label="{hint}">'
        '<div class="ticker-track" id="tickerTrack"><div class="ticker-set">\n'
        + "\n".join(rows)
        + "\n</div></div></div>\n"
        f'<button class="motion-toggle" id="tickerToggle" type="button" aria-label="{pause}" aria-pressed="false">'
        '<svg class="icon" aria-hidden="true"><use href="#i-pause"/></svg></button>\n'
        "</div></div>"
    )


def render_proof(soja: dict[str, Any], loc: dict[str, Any]) -> str:
    values = soja["valores"]
    low, high = min(values), max(values)
    spread = high - low or 1.0
    points = [
        f"{index * 300 / (len(values) - 1):.1f},{76 - (value - low) / spread * 64:.1f}"
        for index, value in enumerate(values)
    ]
    last_x, last_y = points[-1].split(",")
    area = "M" + " L".join(point.replace(",", " ") for point in points) + " V84 H0 Z"
    rows = [
        f"<tr><td>{fmt_data(date, loc)}</td><td>{fmt_valor(value, loc)}</td></tr>"
        for date, value in list(zip(soja["datas"], values))[-4:]
    ]
    english = loc["decimal"] == "."
    unit = loc["unidades"][soja["unidade"]]
    date = fmt_data(soja["datas"][-1], loc)
    iso_date = str(soja["datas"][-1])[:10]
    aria = str(loc["spark_aria"]).format(n=len(values))
    variation = (values[-1] / values[-2] - 1) * 100
    direction, arrow = ("up", "↑") if variation >= 0 else ("down", "↓")
    change = "on the latest sampled trading day" if english else "no último pregão da amostra"
    caption = (
        f"{len(values)} trading days in sample" if english else f"{len(values)} pregões na amostra"
    )
    title = "Soybean · CEPEA/ESALQ" if english else "Soja · CEPEA/ESALQ"
    table_caption = (
        "Last four sampled soybean prices"
        if english
        else "Últimos quatro preços da amostra de soja"
    )
    date_label, price_label = ("Date", "Price") if english else ("Data", "Preço")
    return (
        '<figure class="output-panel">\n'
        f'<div class="output-heading"><figcaption class="eyebrow">{title}</figcaption>'
        f'<time class="output-date" datetime="{iso_date}">{date}</time></div>\n'
        f'<p class="proof-price">R$ {fmt_valor(values[-1], loc)} <span class="unit">{unit}</span></p>\n'
        f'<p class="price-change {direction}">{arrow} {fmt_valor(abs(variation), loc)}% {change}</p>\n'
        f'<svg class="sparkline" viewBox="0 0 300 84" preserveAspectRatio="none" role="img" aria-label="{aria}">\n'
        '<defs><linearGradient id="chart-fill" x1="0" y1="0" x2="0" y2="1"><stop stop-color="#d7b879" stop-opacity=".2"/>'
        '<stop offset="1" stop-color="#d7b879" stop-opacity="0"/></linearGradient></defs>\n'
        f'<path class="spark-area" d="{area}"/><polyline class="spark-path" points="{" ".join(points)}"/>\n'
        f'<circle cx="{last_x}" cy="{last_y}" r="2.7" fill="#ebd2a0"/></svg>\n'
        f'<div class="chart-caption"><span>{caption}</span><span>R$ {unit}</span></div>\n'
        f'<table class="proof-table"><caption class="sr-only">{table_caption}, R$ {unit}</caption>'
        f'<thead class="sr-only"><tr><th scope="col">{date_label}</th><th scope="col">{price_label}</th></tr></thead>'
        "<tbody>" + "".join(rows) + "</tbody></table>\n</figure>"
    )


def substituir(html: str, tag: str, conteudo: str) -> str:
    pattern = re.compile(rf"(<!-- agrobr:{tag} -->).*?(<!-- /agrobr:{tag} -->)", re.DOTALL)
    if len(pattern.findall(html)) != 1:
        raise RuntimeError(f"Esperada uma única âncora agrobr:{tag}")
    return pattern.sub(lambda match: match[1] + "\n" + conteudo + "\n" + match[2], html, count=1)


def render_page(
    html: str, loc: dict[str, Any], ticker: list[dict[str, Any]], soja: dict[str, Any]
) -> str:
    html = substituir(html, "ticker", render_ticker(ticker, loc))
    return substituir(html, "proof", render_proof(soja, loc))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT, help="Diretório das duas landings")
    parser.add_argument(
        "--check", action="store_true", help="Coletar e validar sem gravar arquivos"
    )
    options = parser.parse_args()
    pages = []
    for language, loc in LOCALES.items():
        path = options.root / ("en/index.html" if language == "en" else "index.html")
        html = path.read_text(encoding="utf-8")
        for anchor in ("ticker", "proof"):
            substituir(html, anchor, "")
        pages.append((path, html, loc))
    ticker, soja = asyncio.run(coletar())
    updates = [(path, render_page(html, loc, ticker, soja)) for path, html, loc in pages]
    for path, html in updates:
        if not options.check:
            with atomic_output(path) as temporary:
                temporary.write_text(html, encoding="utf-8")
        print(f"{path.relative_to(options.root)}: {'validado' if options.check else 'atualizado'}")
    print(f"soja R$ {soja['valores'][-1]:.2f} ({str(soja['datas'][-1])[:10]})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
