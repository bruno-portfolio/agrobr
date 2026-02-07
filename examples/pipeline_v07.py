#!/usr/bin/env python3
"""
Pipeline v0.7.0 — Dados Agrícolas Completos
============================================

Pipeline demonstrando todas as fontes do agrobr v0.7.0:
- CEPEA: indicadores de preço
- CONAB: safras + custos de produção
- IBGE: PAM
- INMET: dados meteorológicos
- BCB/SICOR: crédito rural
- ComexStat: exportações
- ANDA: entregas de fertilizantes (requer pip install agrobr[pdf])

Demonstra:
- Coleta paralela de múltiplas fontes
- MetaInfo com proveniência completa
- Cache hit na segunda execução
- Exportação em Parquet

Uso:
    python pipeline_v07.py
    python pipeline_v07.py  # segunda execução → cache hit
"""

from __future__ import annotations

import asyncio
import time
from pathlib import Path

import pandas as pd


async def coletar_precos() -> tuple[pd.DataFrame, dict]:
    """Coleta preço diário da soja (CEPEA)."""
    from agrobr import cepea

    df, meta = await cepea.indicador("soja", return_meta=True)
    return df, meta.to_dict()


async def coletar_safras() -> tuple[pd.DataFrame, dict]:
    """Coleta safra de soja (CONAB)."""
    from agrobr import conab

    df, meta = await conab.safras("soja", safra="2024/25", return_meta=True)
    return df, meta.to_dict()


async def coletar_custo_producao() -> tuple[pd.DataFrame, dict] | None:
    """Coleta custos de produção da soja (CONAB)."""
    from agrobr import conab

    try:
        df, meta = await conab.custo_producao(
            cultura="soja", uf="MT", safra="2024/25", return_meta=True
        )
        return df, meta.to_dict()
    except Exception as e:
        print(f"  [!] Custo produção indisponível: {e}")
        return None


async def coletar_exportacao() -> tuple[pd.DataFrame, dict]:
    """Coleta exportações de soja (ComexStat)."""
    from agrobr import comexstat

    df, meta = await comexstat.exportacao(
        "soja", ano=2024, agregacao="mensal", return_meta=True
    )
    return df, meta.to_dict()


async def coletar_credito() -> tuple[pd.DataFrame, dict]:
    """Coleta crédito rural para soja (BCB/SICOR)."""
    from agrobr import bcb

    df, meta = await bcb.credito_rural(
        produto="soja", safra="2024/25", finalidade="custeio", return_meta=True
    )
    return df, meta.to_dict()


async def coletar_clima() -> tuple[pd.DataFrame, dict]:
    """Coleta dados climáticos de MT (INMET)."""
    from agrobr import inmet

    df, meta = await inmet.clima_uf("MT", ano=2024, return_meta=True)
    return df, meta.to_dict()


async def coletar_fertilizantes() -> tuple[pd.DataFrame, dict] | None:
    """Coleta entregas de fertilizantes (ANDA). Requer pdfplumber."""
    try:
        from agrobr import anda

        df, meta = await anda.entregas(ano=2024, uf="MT", return_meta=True)
        return df, meta.to_dict()
    except ImportError:
        print("  [!] ANDA: pdfplumber não instalado (pip install agrobr[pdf])")
        return None
    except Exception as e:
        print(f"  [!] ANDA indisponível: {e}")
        return None


def print_meta(nome: str, meta: dict) -> None:
    """Exibe proveniência de um dataset."""
    print(f"\n  [{nome}]")
    print(f"    source:       {meta.get('source', '?')}")
    print(f"    method:       {meta.get('source_method', '?')}")
    print(f"    records:      {meta.get('records_count', 0)}")
    print(f"    fetch_ms:     {meta.get('fetch_duration_ms', 0)} ms")
    print(f"    parse_ms:     {meta.get('parse_duration_ms', 0)} ms")
    print(f"    from_cache:   {meta.get('from_cache', False)}")
    print(f"    parser_v:     {meta.get('parser_version', '?')}")
    attempted = meta.get("attempted_sources", [])
    selected = meta.get("selected_source", "?")
    if len(attempted) > 1:
        print(f"    fallback:     tentou {attempted} → selecionou {selected}")


def transformar_precos(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Resample preços para visões semanal e mensal."""
    if df.empty or "data" not in df.columns:
        return {"diario": df, "semanal": pd.DataFrame(), "mensal": pd.DataFrame()}

    df = df.copy()
    df["data"] = pd.to_datetime(df["data"])
    df = df.set_index("data").sort_index()

    semanal = df[["valor"]].resample("W").agg(
        valor_medio=("valor", "mean"),
        valor_max=("valor", "max"),
        valor_min=("valor", "min"),
    ).dropna()

    mensal = df[["valor"]].resample("ME").agg(
        valor_medio=("valor", "mean"),
        valor_max=("valor", "max"),
        valor_min=("valor", "min"),
    ).dropna()

    df = df.reset_index()
    semanal = semanal.reset_index()
    mensal = mensal.reset_index()

    return {"diario": df, "semanal": semanal, "mensal": mensal}


def salvar_parquet(dados: dict[str, pd.DataFrame], output_dir: Path) -> list[str]:
    """Salva DataFrames em Parquet."""
    output_dir.mkdir(parents=True, exist_ok=True)
    salvos: list[str] = []

    for nome, df in dados.items():
        if df is not None and not df.empty:
            path = output_dir / f"{nome}.parquet"
            df.to_parquet(path, index=False)
            salvos.append(f"  {path.name} ({len(df)} rows)")

    return salvos


async def main() -> None:
    """Pipeline principal."""
    print("=" * 70)
    print("agrobr v0.7.0 — Pipeline de Dados Agrícolas Completo")
    print("=" * 70)

    t0 = time.monotonic()

    # ── Coleta paralela ──────────────────────────────────────
    print("\n1. COLETA (paralela, todas as fontes)")
    print("-" * 70)

    results = await asyncio.gather(
        coletar_precos(),
        coletar_safras(),
        coletar_exportacao(),
        coletar_credito(),
        coletar_clima(),
        coletar_fertilizantes(),
        coletar_custo_producao(),
        return_exceptions=True,
    )

    nomes = ["precos", "safras", "exportacao", "credito", "clima", "fertilizantes", "custo"]
    datasets: dict[str, pd.DataFrame] = {}
    metas: dict[str, dict] = {}

    for nome, result in zip(nomes, results):
        if isinstance(result, Exception):
            print(f"  [!] {nome}: {result}")
        elif result is None:
            print(f"  [~] {nome}: não disponível")
        else:
            df, meta = result
            datasets[nome] = df
            metas[nome] = meta
            print(f"  [ok] {nome}: {len(df)} registros")

    elapsed_coleta = time.monotonic() - t0

    # ── MetaInfo proveniência ────────────────────────────────
    print(f"\n2. PROVENIÊNCIA (MetaInfo) — coleta em {elapsed_coleta:.1f}s")
    print("-" * 70)

    for nome, meta in metas.items():
        print_meta(nome, meta)

    # ── Transformação ────────────────────────────────────────
    print(f"\n3. TRANSFORMAÇÃO")
    print("-" * 70)

    precos_views = {}
    if "precos" in datasets:
        precos_views = transformar_precos(datasets["precos"])
        for view_name, view_df in precos_views.items():
            if not view_df.empty:
                print(f"  precos_{view_name}: {len(view_df)} rows")

    # ── Salvar Parquet ───────────────────────────────────────
    print(f"\n4. EXPORTAÇÃO (Parquet)")
    print("-" * 70)

    output_dir = Path("./output/v07_pipeline")
    all_data = {**{f"precos_{k}": v for k, v in precos_views.items()}, **datasets}
    salvos = salvar_parquet(all_data, output_dir)

    if salvos:
        print(f"  Diretório: {output_dir}")
        for s in salvos:
            print(s)
    else:
        print("  Nenhum dado para salvar")

    # ── Resumo ───────────────────────────────────────────────
    elapsed_total = time.monotonic() - t0
    total_records = sum(len(df) for df in datasets.values())

    print(f"\n{'=' * 70}")
    print(f"Pipeline concluído em {elapsed_total:.1f}s")
    print(f"  Fontes coletadas: {len(datasets)}/{len(nomes)}")
    print(f"  Total de registros: {total_records:,}")
    print(f"  Arquivos Parquet: {len(salvos)}")
    print(f"\nDica: execute novamente para ver cache hit!")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    asyncio.run(main())
