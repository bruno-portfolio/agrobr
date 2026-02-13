"""Atualiza golden data capturando dados reais das fontes.

Uso:
    python scripts/update_golden.py --source bcb
    python scripts/update_golden.py --source ibge
    python scripts/update_golden.py --source comexstat
    python scripts/update_golden.py --source deral
    python scripts/update_golden.py --source abiove
    python scripts/update_golden.py --source cepea --produto soja
    python scripts/update_golden.py --all
    python scripts/update_golden.py --issue10   # apenas as 5 fontes da issue #10
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
from datetime import UTC, datetime
from io import StringIO
from pathlib import Path
from typing import Any

import pandas as pd

GOLDEN_DIR = Path(__file__).parent.parent / "tests" / "golden_data"


# ============================================================================
# Helpers
# ============================================================================


def _jsonable(val: Any) -> Any:
    if pd.isna(val):
        return None
    try:
        return val.item()
    except (AttributeError, ValueError):
        return val


def _build_expected_from_df(
    df: pd.DataFrame,
    *,
    non_null_columns: list[str] | None = None,
    first_row_keys: list[str] | None = None,
    last_row_keys: list[str] | None = None,
    use_count_min: bool = False,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    expected: dict[str, Any] = {}

    if use_count_min:
        expected["count_min"] = len(df)
    else:
        expected["count"] = len(df)

    expected["columns"] = df.columns.tolist()

    if non_null_columns:
        expected["non_null_columns"] = non_null_columns

    if len(df) > 0:
        first = df.iloc[0]
        last = df.iloc[-1]

        fkeys = first_row_keys or []
        lkeys = last_row_keys or []

        if fkeys:
            expected["first_row"] = {
                k: _jsonable(first[k]) for k in fkeys if k in df.columns and pd.notna(first[k])
            }
        if lkeys:
            expected["last_row"] = {
                k: _jsonable(last[k]) for k in lkeys if k in df.columns and pd.notna(last[k])
            }

    if extra:
        expected.update(extra)

    return expected


def _save_golden(
    case_dir: Path,
    expected: dict[str, Any],
    metadata: dict[str, Any],
) -> None:
    (case_dir / "expected.json").write_text(
        json.dumps(expected, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )
    (case_dir / "metadata.json").write_text(
        json.dumps(metadata, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )
    print(f"  Saved to {case_dir}")


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


# ============================================================================
# BCB
# ============================================================================


async def capture_bcb() -> None:
    import httpx

    print("Capturing BCB/custeio_sample...")

    url = "https://olinda.bcb.gov.br/olinda/servico/SICOR/versao/v2/odata/CusteioRegiaoUFProduto"
    params = {
        "$format": "json",
        "$top": "20",
        "$filter": "contains(nomeProduto,'SOJA')",
    }

    timeout = httpx.Timeout(connect=10.0, read=120.0, write=30.0, pool=30.0)
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        response = await client.get(url, params=params)
        response.raise_for_status()
        data = response.json()

    records = data.get("value", [])
    if not records:
        print("  ERROR: No records returned from BCB API")
        return

    print(f"  Fetched {len(records)} records")

    case_dir = GOLDEN_DIR / "bcb" / "custeio_sample"
    case_dir.mkdir(parents=True, exist_ok=True)

    (case_dir / "response.json").write_text(
        json.dumps(records, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    from agrobr.bcb.parser import parse_credito_rural

    df = parse_credito_rural(records)

    expected = _build_expected_from_df(
        df,
        non_null_columns=["uf", "produto", "valor"],
        first_row_keys=["uf", "produto", "finalidade"],
        last_row_keys=["uf", "produto"],
    )

    metadata = {
        "source": "bcb",
        "format": "json",
        "parser": "agrobr.bcb.parser::parse_credito_rural()",
        "parser_version": 1,
        "parser_kwargs": {},
        "needs_real_data": False,
        "captured_at": _now_iso(),
        "api_url": str(response.url),
        "notes": (
            f"Real data from BCB Olinda API — "
            f"CusteioRegiaoUFProduto SOJA top 20, {len(records)} records"
        ),
    }

    _save_golden(case_dir, expected, metadata)
    print(f"  Parsed {len(df)} rows | cols={df.columns.tolist()}")


# ============================================================================
# IBGE
# ============================================================================


async def capture_ibge() -> None:
    import sidrapy

    print("Capturing IBGE/pam_soja_sample...")

    df_raw = sidrapy.get_table(
        table_code="5457",
        territorial_level="3",
        ibge_territorial_code="all",
        variable="214",
        period="2023",
        classifications={"782": "40124"},
        header="n",
    )

    if len(df_raw) > 1:
        df_raw = df_raw.iloc[1:].reset_index(drop=True)

    if df_raw.empty:
        print("  ERROR: Empty response from SIDRA")
        return

    print(f"  Fetched {len(df_raw)} rows | cols={df_raw.columns.tolist()}")

    case_dir = GOLDEN_DIR / "ibge" / "pam_soja_sample"
    case_dir.mkdir(parents=True, exist_ok=True)

    df_raw.to_csv(case_dir / "response.csv", index=False, encoding="utf-8")

    from agrobr.ibge.client import parse_sidra_response

    df = parse_sidra_response(df_raw.copy())

    expected = _build_expected_from_df(
        df,
        non_null_columns=[
            "nivel_territorial_cod",
            "localidade",
            "ano",
        ],
        first_row_keys=["localidade", "produto"],
        last_row_keys=["localidade", "produto"],
    )

    metadata = {
        "source": "ibge",
        "format": "dataframe",
        "parser": "agrobr.ibge.client::parse_sidra_response()",
        "parser_version": 1,
        "parser_kwargs": {},
        "needs_real_data": False,
        "captured_at": _now_iso(),
        "query": {
            "table": "5457",
            "variable": "214",
            "territorial_level": "3",
            "period": "2023",
            "classification_81": "40124",
        },
        "notes": (
            f"Real data from IBGE SIDRA — PAM nova, produção soja por UF 2023, {len(df)} rows"
        ),
    }

    _save_golden(case_dir, expected, metadata)
    print(f"  Parsed {len(df)} rows | cols={df.columns.tolist()}")


# ============================================================================
# ComexStat
# ============================================================================


async def capture_comexstat() -> None:
    import httpx

    print("Capturing ComexStat/exportacao_soja_sample...")

    ano = 2024
    url = f"https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/EXP_{ano}.csv"

    timeout = httpx.Timeout(connect=10.0, read=180.0, write=30.0, pool=30.0)
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
    }

    print(f"  Downloading {url} (large file, may take a while)...")
    async with httpx.AsyncClient(
        timeout=timeout,
        follow_redirects=True,
        verify=False,
        headers=headers,
    ) as client:
        response = await client.get(url)
        response.raise_for_status()
        csv_full = response.text

    print(f"  Downloaded {len(csv_full):,} chars")

    sep = ";" if ";" in csv_full.split("\n")[0] else ","
    df_full = pd.read_csv(
        StringIO(csv_full),
        sep=sep,
        dtype=str,
        low_memory=False,
    )

    mask = (df_full["CO_NCM"] == "12019000") & (df_full["CO_MES"] == "01")
    df_sample = df_full[mask].head(20).copy()

    if df_sample.empty:
        mask_broad = df_full["CO_NCM"].str.startswith("1201") & (df_full["CO_MES"] == "01")
        df_sample = df_full[mask_broad].head(20).copy()

    if df_sample.empty:
        print("  ERROR: No soja records found")
        return

    print(f"  Filtered to {len(df_sample)} soja records")

    case_dir = GOLDEN_DIR / "comexstat" / "exportacao_soja_sample"
    case_dir.mkdir(parents=True, exist_ok=True)

    csv_text = df_sample.to_csv(index=False, sep=sep)
    (case_dir / "response.csv").write_text(csv_text, encoding="utf-8")

    from agrobr.comexstat.parser import parse_exportacao

    df = parse_exportacao(csv_text)

    expected = _build_expected_from_df(
        df,
        non_null_columns=["ano", "mes", "ncm", "uf", "kg_liquido", "valor_fob_usd"],
        first_row_keys=["ano", "mes", "ncm", "uf"],
        last_row_keys=["ano", "mes", "ncm", "uf"],
    )

    metadata = {
        "source": "comexstat",
        "format": "csv",
        "parser": "agrobr.comexstat.parser::parse_exportacao()",
        "parser_version": 1,
        "parser_kwargs": {},
        "needs_real_data": False,
        "captured_at": _now_iso(),
        "api_url": url,
        "notes": (
            f"Real data from ComexStat — "
            f"EXP_{ano}.csv, soja NCM 12019000 month 1, {len(df)} records"
        ),
    }

    _save_golden(case_dir, expected, metadata)
    print(f"  Parsed {len(df)} rows | cols={df.columns.tolist()}")


# ============================================================================
# DERAL
# ============================================================================


async def capture_deral() -> None:
    import httpx

    print("Capturing DERAL/pc_sample...")

    url = "https://www.agricultura.pr.gov.br/system/files/publico/Safras/PC.xls"
    headers = {
        "User-Agent": "agrobr/0.9.0 (https://github.com/your-org/agrobr)",
        "Accept": (
            "application/vnd.ms-excel, "
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet, "
            "*/*"
        ),
    }

    async with httpx.AsyncClient(
        timeout=30.0,
        follow_redirects=True,
        headers=headers,
    ) as client:
        response = await client.get(url)
        response.raise_for_status()
        xls_bytes = response.content

    print(f"  Downloaded {len(xls_bytes):,} bytes")

    case_dir = GOLDEN_DIR / "deral" / "pc_sample"
    case_dir.mkdir(parents=True, exist_ok=True)

    # Salvar bytes brutos — pd.ExcelFile detecta formato pelo magic number
    (case_dir / "response.xlsx").write_bytes(xls_bytes)

    from agrobr.deral.parser import parse_pc_xls

    df = parse_pc_xls(xls_bytes)

    condicoes: list[str] = []
    if "condicao" in df.columns:
        condicoes = sorted(df[df["condicao"] != ""]["condicao"].unique().tolist())

    expected = _build_expected_from_df(
        df,
        non_null_columns=["produto"],
        use_count_min=True,
        extra={
            "has_condicao": bool(condicoes),
            "condicoes_expected": condicoes if condicoes else ["boa", "media", "ruim"],
        },
    )
    expected.pop("first_row", None)
    expected.pop("last_row", None)

    metadata = {
        "source": "deral",
        "format": "xlsx",
        "parser": "agrobr.deral.parser::parse_pc_xls()",
        "parser_version": 1,
        "parser_kwargs": {},
        "needs_real_data": False,
        "captured_at": _now_iso(),
        "api_url": url,
        "notes": (
            f"Real data from DERAL PC.xls — "
            f"weekly crop conditions, {len(df)} records, "
            f"condicoes: {condicoes}"
        ),
    }

    _save_golden(case_dir, expected, metadata)
    produtos = df["produto"].unique().tolist() if "produto" in df.columns else []
    print(f"  Parsed {len(df)} rows | produtos={produtos}")


# ============================================================================
# ABIOVE
# ============================================================================


async def capture_abiove() -> None:
    import httpx

    print("Capturing ABIOVE/exportacao_sample...")

    base_url = "https://abiove.org.br/abiove_content/Abiove"
    headers = {"User-Agent": "agrobr/0.9.0 (https://github.com/your-org/agrobr)"}

    xlsx_bytes: bytes | None = None
    url_used = ""
    ano_found = 0

    async with httpx.AsyncClient(
        timeout=60.0,
        follow_redirects=True,
        headers=headers,
    ) as client:
        for ano in (2025, 2024):
            for m in range(12, 0, -1):
                url = f"{base_url}/exp_{ano:04d}{m:02d}.xlsx"
                try:
                    resp = await client.get(url)
                    if resp.status_code == 200 and len(resp.content) > 1000:
                        xlsx_bytes = resp.content
                        url_used = url
                        ano_found = ano
                        print(f"  Found: {url} ({len(xlsx_bytes):,} bytes)")
                        break
                except Exception:
                    continue
            if xlsx_bytes:
                break

    if not xlsx_bytes:
        print("  ERROR: No ABIOVE export file found")
        return

    case_dir = GOLDEN_DIR / "abiove" / "exportacao_sample"
    case_dir.mkdir(parents=True, exist_ok=True)

    (case_dir / "response.xlsx").write_bytes(xlsx_bytes)

    from agrobr.abiove.parser import parse_exportacao_excel

    df = parse_exportacao_excel(xlsx_bytes, ano=ano_found)

    produtos: list[str] = []
    if "produto" in df.columns:
        produtos = sorted(df["produto"].unique().tolist())

    expected = _build_expected_from_df(
        df,
        non_null_columns=["ano", "mes", "produto", "volume_ton"],
        use_count_min=True,
        extra={
            "has_multiple_products": len(produtos) > 1,
            "produtos_expected": produtos,
        },
    )

    metadata = {
        "source": "abiove",
        "format": "xlsx",
        "parser": "agrobr.abiove.parser::parse_exportacao_excel()",
        "parser_version": 1,
        "parser_kwargs": {"ano": ano_found},
        "needs_real_data": False,
        "captured_at": _now_iso(),
        "api_url": url_used,
        "notes": (
            f"Real data from ABIOVE — export {ano_found}, {len(df)} records, produtos: {produtos}"
        ),
    }

    _save_golden(case_dir, expected, metadata)
    print(f"  Parsed {len(df)} rows | produtos={produtos}")


# ============================================================================
# CEPEA (original)
# ============================================================================


async def capture_cepea(produto: str) -> None:
    from agrobr.cepea import client as cepea_client
    from agrobr.cepea.parsers.detector import get_parser_with_fallback

    print(f"Capturing cepea/{produto}...")

    html = await cepea_client.fetch_indicador_page(produto)
    parser, results = await get_parser_with_fallback(html, produto)

    periodo = datetime.now().strftime("%Y_%m")
    case_dir = GOLDEN_DIR / "cepea" / f"{produto}_{periodo}"
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

    metadata = {
        "source": "cepea",
        "produto": produto,
        "periodo": periodo.replace("_", "-"),
        "captured_at": _now_iso(),
        "parser_version": parser.version,
        "needs_real_data": False,
        "url": "captured from cepea",
        "notes": "Auto-generated golden data",
    }

    _save_golden(case_dir, expected, metadata)
    print(f"  Records: {len(results)}")
    print(f"  First: {results[0].data} = {results[0].valor}")
    print(f"  Last: {results[-1].data} = {results[-1].valor}")


# ============================================================================
# Main
# ============================================================================

CAPTURE_MAP: dict[str, Any] = {
    "bcb": capture_bcb,
    "ibge": capture_ibge,
    "comexstat": capture_comexstat,
    "deral": capture_deral,
    "abiove": capture_abiove,
}

ISSUE10_SOURCES = ["bcb", "ibge", "comexstat", "deral", "abiove"]


async def main_async(args: argparse.Namespace) -> None:
    if args.issue10:
        print("=== Issue #10: Capturando dados reais para 5 fontes ===\n")
        for source in ISSUE10_SOURCES:
            try:
                await CAPTURE_MAP[source]()
            except Exception as e:
                print(f"  ERROR ({source}): {e}")
            print()
        print("=== Done ===")
        return

    if args.all:
        for source, fn in CAPTURE_MAP.items():
            try:
                await fn()
            except Exception as e:
                print(f"  ERROR ({source}): {e}")
            print()

        for produto in ["soja", "milho", "cafe", "boi"]:
            try:
                await capture_cepea(produto)
            except Exception as e:
                print(f"  ERROR (cepea/{produto}): {e}")
            print()
        return

    if args.source:
        source = args.source.lower()
        if source == "cepea":
            produto = args.produto or "soja"
            await capture_cepea(produto)
        elif source in CAPTURE_MAP:
            await CAPTURE_MAP[source]()
        else:
            sources = ", ".join(["cepea"] + list(CAPTURE_MAP.keys()))
            print(f"Unknown source: {source}. Options: {sources}")
    else:
        print("Error: --source, --all, or --issue10 required")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Capture golden data for testing",
    )
    parser.add_argument(
        "--source",
        help="Source (cepea, bcb, ibge, comexstat, deral, abiove)",
    )
    parser.add_argument("--produto", help="Produto (only for cepea)")
    parser.add_argument(
        "--all",
        action="store_true",
        help="Capture all sources",
    )
    parser.add_argument(
        "--issue10",
        action="store_true",
        help="Capture only the 5 sources for issue #10",
    )
    args = parser.parse_args()

    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
