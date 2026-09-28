#!/usr/bin/env python3
"""
Análise de Soja - Brasil
========================

Exemplo de análise combinando dados de preços (CEPEA),
safras (CONAB) e produção municipal (IBGE).

Uso:
    python analise_soja.py
"""

from __future__ import annotations

import asyncio


async def main() -> None:
    """Análise completa de soja."""
    from agrobr import cepea, conab, ibge

    print("=" * 70)
    print("ANÁLISE DE SOJA - BRASIL")
    print("=" * 70)

    print("\n1. INDICADOR DE PREÇO (CEPEA/ESALQ - Paranaguá)")
    print("-" * 70)

    try:
        precos = await cepea.indicador("soja")

        if not precos.empty:
            ultimo_preco = precos.iloc[-1]
            print(f"Último valor: R$ {ultimo_preco['valor']:.2f}/sc em {ultimo_preco['data']}")

            print(f"Média (365 dias): R$ {precos['valor'].mean():.2f}/sc")
            print(f"Máxima: R$ {precos['valor'].max():.2f}/sc")
            print(f"Mínima: R$ {precos['valor'].min():.2f}/sc")

            var_total = ((precos.iloc[-1]["valor"] / precos.iloc[0]["valor"]) - 1) * 100
            print(f"Variação no período: {var_total:+.2f}%")
    except Exception as e:
        print(f"Erro ao coletar preços: {e}")

    print("\n2. SAFRA 2024/25 (CONAB)")
    print("-" * 70)

    try:
        safras = await conab.safras("soja", safra="2024/25")

        if not safras.empty:
            brasil = safras[safras["uf"].isna() | (safras["uf"] == "BRASIL")]
            if brasil.empty:
                total_area = safras["area_plantada"].sum()
                total_prod = safras["producao"].sum()
            else:
                total_area = brasil["area_plantada"].iloc[0]
                total_prod = brasil["producao"].iloc[0]

            print(f"Área plantada: {total_area:,.0f} mil ha")
            print(f"Produção estimada: {total_prod:,.0f} mil t")

            print("\nTop 5 estados produtores:")
            estados = safras[safras["uf"].notna() & (safras["uf"] != "BRASIL")]
            top5 = estados.nlargest(5, "producao")
            for _, row in top5.iterrows():
                uf = row.get("uf", "N/A")
                prod = row["producao"]
                print(f"  {uf}: {prod:,.0f} mil t")
    except Exception as e:
        print(f"Erro ao coletar safras: {e}")

    print("\n3. PRODUÇÃO HISTÓRICA PAM (IBGE)")
    print("-" * 70)

    try:
        pam = await ibge.pam("soja", ano=[2020, 2021, 2022, 2023], nivel="brasil")

        if not pam.empty:
            print("Evolução da produção (milhões de toneladas):")
            for _, row in pam.iterrows():
                ano = row.get("ano", "N/A")
                prod = row.get("producao", 0) / 1_000_000
                print(f"  {ano}: {prod:,.1f} M t")
    except Exception as e:
        print(f"Erro ao coletar PAM: {e}")

    print("\n4. COMPARATIVO REGIONAL (IBGE PAM 2023)")
    print("-" * 70)

    try:
        pam_uf = await ibge.pam("soja", ano=2023, nivel="uf")

        if not pam_uf.empty:
            print("Top 10 estados - Produção 2023:")
            if "producao" in pam_uf.columns:
                top10 = pam_uf.nlargest(10, "producao")
                for i, (_, row) in enumerate(top10.iterrows(), 1):
                    uf = row.get("localidade", row.get("uf", "N/A"))
                    prod = row.get("producao", 0) / 1_000_000
                    area = row.get("area_plantada", 0) / 1_000_000
                    print(f"  {i:2}. {uf}: {prod:,.1f} M t ({area:,.2f} M ha)")
    except Exception as e:
        print(f"Erro ao coletar PAM por UF: {e}")

    print("\n" + "=" * 70)
    print("Análise concluída!")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
