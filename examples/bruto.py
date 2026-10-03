#!/usr/bin/env python3
"""
Coleta bruta
============

Guarda o dado original de três fontes numa pasta, com o manifesto de cada aquisição:
o ZIP do SNCI público de Alagoas, a malha municipal de Alagoas e as unidades de
conservação num recorte de Maceió. Rodar de novo com ``retomar=True`` reaproveita o
que já está completo, depois de conferir os hashes.

Uso:
    python bruto.py
"""

from __future__ import annotations

import asyncio
from pathlib import Path

from agrobr import bruto

DESTINO = Path("dados_brutos")
MACEIO = (-35.8, -9.7, -35.65, -9.55)


async def main() -> None:
    coletas = [
        await bruto.coletar(
            "acervo_fundiario", "snci_publico", uf="AL", destino=DESTINO, retomar=True
        ),
        await bruto.coletar("ibge", "malha_municipal", uf="AL", destino=DESTINO, retomar=True),
        await bruto.coletar(
            "cnuc", "ucs", uf="AL", bbox=MACEIO, nome="maceio", destino=DESTINO, retomar=True
        ),
    ]
    for coleta in coletas:
        entrada = coleta.entrada
        origem = "reaproveitada" if coleta.reutilizado else "nova"
        print(f"{entrada.fonte}/{entrada.recurso}/{entrada.nome}: {entrada.status} ({origem})")
        if entrada.arquivo:
            print(f"  arquivo {entrada.arquivo}, {entrada.bytes} bytes, sha256 {entrada.sha256}")
        for pagina in entrada.paginas:
            print(
                f"  página {pagina.numero}: {pagina.feicoes_recebidas} feições em {pagina.arquivo}"
            )
    print(f"manifesto: {coletas[0].manifesto}")


if __name__ == "__main__":
    asyncio.run(main())
