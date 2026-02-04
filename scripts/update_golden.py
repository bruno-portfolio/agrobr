"""Atualiza golden data capturando dados atuais das fontes.

Uso:
    python scripts/update_golden.py --source cepea --produto soja
    python scripts/update_golden.py --all
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
from datetime import datetime
from pathlib import Path

GOLDEN_DIR = Path(__file__).parent.parent / "tests" / "golden_data"


async def capture_golden(source: str, produto: str) -> None:
    """Captura golden data de uma fonte."""
    from agrobr.cepea import client as cepea_client
    from agrobr.cepea.parsers.detector import get_parser_with_fallback

    print(f"Capturing {source}/{produto}...")

    html = await cepea_client.fetch_indicador_page(produto)
    parser, results = await get_parser_with_fallback(html, produto)

    periodo = datetime.now().strftime("%Y_%m")
    case_dir = GOLDEN_DIR / source / f"{produto}_{periodo}"
    case_dir.mkdir(parents=True, exist_ok=True)

    (case_dir / "response.html").write_text(html, encoding="utf-8")

    data_str = json.dumps(
        [r.model_dump(mode="json") for r in results],
        sort_keys=True,
        default=str,
    )
    checksum = f"sha256:{hashlib.sha256(data_str.encode()).hexdigest()[:16]}"

    expected = {
        "count": len(results),
        "first": {
            "data": str(results[0].data),
            "valor": str(results[0].valor),
            "unidade": results[0].unidade,
        },
        "last": {
            "data": str(results[-1].data),
            "valor": str(results[-1].valor),
            "unidade": results[-1].unidade,
        },
        "checksum": checksum,
    }
    (case_dir / "expected.json").write_text(
        json.dumps(expected, indent=2),
        encoding="utf-8",
    )

    metadata = {
        "source": source,
        "produto": produto,
        "periodo": periodo.replace("_", "-"),
        "captured_at": datetime.utcnow().isoformat() + "Z",
        "parser_version": parser.version,
        "url": f"captured from {source}",
        "notes": "Auto-generated golden data",
    }
    (case_dir / "metadata.json").write_text(
        json.dumps(metadata, indent=2),
        encoding="utf-8",
    )

    print(f"  Saved to {case_dir}")
    print(f"  Records: {len(results)}")
    print(f"  First: {results[0].data} = {results[0].valor}")
    print(f"  Last: {results[-1].data} = {results[-1].valor}")


async def main_async(args):
    if args.all:
        for source, produtos in [
            ("cepea", ["soja", "milho", "cafe", "boi"]),
        ]:
            for produto in produtos:
                try:
                    await capture_golden(source, produto)
                except Exception as e:
                    print(f"  ERROR: {e}")
    else:
        if not args.source or not args.produto:
            print("Error: --source and --produto are required unless using --all")
            return
        await capture_golden(args.source, args.produto)


def main():
    parser = argparse.ArgumentParser(description="Capture golden data for testing")
    parser.add_argument("--source", help="Source (cepea, conab, ibge)")
    parser.add_argument("--produto", help="Produto")
    parser.add_argument("--all", action="store_true", help="Capture all known combinations")
    args = parser.parse_args()

    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
