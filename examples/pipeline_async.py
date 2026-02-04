#!/usr/bin/env python3
"""
Pipeline Async de Dados Agrícolas
=================================

Exemplo de pipeline assíncrono que coleta dados de múltiplas fontes
em paralelo e consolida em um único DataFrame.

Uso:
    python pipeline_async.py
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pandas as pd


async def coletar_precos() -> pd.DataFrame:
    """Coleta indicadores de preços do CEPEA."""
    from agrobr import cepea

    produtos = ["soja", "milho", "boi_gordo", "cafe"]

    async def get_produto(produto: str) -> pd.DataFrame:
        try:
            df = await cepea.indicador(produto)
            return df
        except Exception as e:
            print(f"Erro ao coletar {produto}: {e}")
            return pd.DataFrame()

    dfs = await asyncio.gather(*[get_produto(p) for p in produtos])

    return pd.concat([df for df in dfs if not df.empty], ignore_index=True)


async def coletar_safras() -> pd.DataFrame:
    """Coleta dados de safras da CONAB."""
    from agrobr import conab

    try:
        df = await conab.safras("soja", safra="2024/25")
        return df
    except Exception as e:
        print(f"Erro ao coletar safras: {e}")
        return pd.DataFrame()


async def coletar_pam() -> pd.DataFrame:
    """Coleta dados da PAM/IBGE."""
    from agrobr import ibge

    try:
        df = await ibge.pam("soja", ano=2023, nivel="uf")
        return df
    except Exception as e:
        print(f"Erro ao coletar PAM: {e}")
        return pd.DataFrame()


async def main():
    """Pipeline principal."""
    print("=" * 60)
    print("Pipeline Async de Dados Agrícolas")
    print("=" * 60)

    print("\nColetando dados em paralelo...")

    precos, safras, pam = await asyncio.gather(
        coletar_precos(),
        coletar_safras(),
        coletar_pam(),
    )

    print("\n" + "-" * 60)
    print("INDICADORES DE PREÇOS (CEPEA)")
    print("-" * 60)
    if not precos.empty:
        print(f"Total de registros: {len(precos)}")
        print("\nÚltimos valores por produto:")
        for produto in precos["produto"].unique():
            ultimo = precos[precos["produto"] == produto].iloc[-1]
            print(f"  {produto}: R$ {ultimo['valor']:.2f} ({ultimo['data']})")
    else:
        print("Nenhum dado coletado")

    print("\n" + "-" * 60)
    print("SAFRAS (CONAB)")
    print("-" * 60)
    if not safras.empty:
        print(f"Total de registros: {len(safras)}")
        print(safras.head())
    else:
        print("Nenhum dado coletado")

    print("\n" + "-" * 60)
    print("PAM (IBGE)")
    print("-" * 60)
    if not pam.empty:
        print(f"Total de registros: {len(pam)}")
        print(pam.head())
    else:
        print("Nenhum dado coletado")

    output_dir = Path("./output")
    output_dir.mkdir(exist_ok=True)

    if not precos.empty:
        precos.to_csv(output_dir / "precos.csv", index=False)
        print(f"\nSalvo: {output_dir / 'precos.csv'}")

    if not safras.empty:
        safras.to_csv(output_dir / "safras.csv", index=False)
        print(f"Salvo: {output_dir / 'safras.csv'}")

    if not pam.empty:
        pam.to_csv(output_dir / "pam.csv", index=False)
        print(f"Salvo: {output_dir / 'pam.csv'}")

    print("\n" + "=" * 60)
    print("Pipeline concluído!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
