"""Coleta fingerprints atuais de todas as fontes.

Uso:
    python scripts/fetch_structures.py --output current_structures.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime
from pathlib import Path


async def fetch_all_structures(output_path: str) -> None:
    """Coleta fingerprints de todas as fontes."""
    from agrobr.cepea import client as cepea_client
    from agrobr.cepea.parsers.fingerprint import extract_fingerprint
    from agrobr.constants import Fonte

    structures = {
        "collected_at": datetime.utcnow().isoformat() + "Z",
        "sources": {},
    }

    print("Fetching CEPEA structure...")
    try:
        html = await cepea_client.fetch_indicador_page("soja")
        fp = extract_fingerprint(html, Fonte.CEPEA, "soja")
        structures["sources"]["cepea"] = fp.model_dump(mode="json")
        print(f"  CEPEA: OK (hash: {fp.structure_hash})")
    except Exception as e:
        structures["sources"]["cepea"] = {"error": str(e)}
        print(f"  CEPEA: ERROR - {e}")

    Path(output_path).write_text(json.dumps(structures, indent=2, default=str))
    print(f"\nStructures saved to {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Fetch current structure fingerprints")
    parser.add_argument(
        "--output",
        default="current_structures.json",
        help="Output file path",
    )
    args = parser.parse_args()

    asyncio.run(fetch_all_structures(args.output))


if __name__ == "__main__":
    main()
