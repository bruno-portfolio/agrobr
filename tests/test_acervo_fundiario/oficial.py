from __future__ import annotations

import struct
import zipfile
from datetime import date
from decimal import Decimal
from io import BytesIO
from typing import Any

import pandas as pd

IBGE_UF = {
    11: "RO", 12: "AC", 13: "AM", 14: "RR", 15: "PA", 16: "AP", 17: "TO",
    21: "MA", 22: "PI", 23: "CE", 24: "RN", 25: "PB", 26: "PE", 27: "AL", 28: "SE", 29: "BA",
    31: "MG", 32: "ES", 33: "RJ", 35: "SP", 41: "PR", 42: "SC", 43: "RS",
    50: "MS", 51: "MT", 52: "GO", 53: "DF",
}  # fmt: skip
SIRGAS_2000 = (
    'GEOGCS["SIRGAS 2000",DATUM["D_SIRGAS_2000",SPHEROID["GRS_1980",6378137,298.257222101]],'
    'PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295]]'
)
Rings = list[list[tuple[float, ...]]]


def member(archive: bytes, suffix: str) -> bytes:
    with zipfile.ZipFile(BytesIO(archive)) as handle:
        (name,) = [name for name in handle.namelist() if name.lower().endswith(suffix)]
        return handle.read(name)


def dbf(archive: bytes) -> list[dict[str, str]]:
    raw = member(archive, ".dbf")
    count, header, width = struct.unpack_from("<IHH", raw, 4)
    fields = []
    offset = 32
    while raw[offset] != 0x0D:
        fields.append((raw[offset : offset + 11].split(b"\0")[0].decode("ascii"), raw[offset + 16]))
        offset += 32
    records = []
    for index in range(count):
        position = header + index * width + 1
        record = {}
        for name, length in fields:
            record[name] = raw[position : position + length].decode("latin-1")
            position += length
        records.append(record)
    return records


def shapes(archive: bytes) -> list[Rings | None]:
    raw = member(archive, ".shp")
    result: list[Rings | None] = []
    offset = 100
    while offset < len(raw):
        length = struct.unpack_from(">i", raw, offset + 4)[0]
        content = offset + 8
        shape_type = struct.unpack_from("<i", raw, content)[0]
        if shape_type == 0:
            result.append(None)
        else:
            parts_count, points_count = struct.unpack_from("<ii", raw, content + 36)
            parts = list(struct.unpack_from(f"<{parts_count}i", raw, content + 44))
            start = content + 44 + 4 * parts_count
            coords = struct.unpack_from(f"<{2 * points_count}d", raw, start)
            columns = [coords[0::2], coords[1::2]]
            if shape_type == 15:
                z_start = start + 16 * points_count + 16
                columns.append(struct.unpack_from(f"<{points_count}d", raw, z_start))
            points = list(zip(*columns, strict=True))
            bounds = zip(parts, [*parts[1:], points_count], strict=True)
            result.append([points[first:last] for first, last in bounds])
        offset = content + 2 * length
    return result


def text(value: str) -> str | None:
    return value.strip(" ") or None


def dbf_date(value: str) -> date | None:
    value = value.strip()
    if value in ("", "00000000"):
        return None
    return date(int(value[:4]), int(value[4:6]), int(value[6:]))


def day_month_year(value: str) -> date | None:
    value = value.strip()
    if not value:
        return None
    day, month, year = (int(piece) for piece in value.split("/"))
    return date(year, month, day)


def integer(value: str) -> int | None:
    value = value.strip()
    return int(value) if value else None


def decimal(value: str) -> float | None:
    value = value.strip()
    return float(Decimal(value)) if value and set(value) != {"*"} else None


def replace_cell(archive: bytes, index: int, field: str, value: str) -> bytes:
    source = zipfile.ZipFile(BytesIO(archive))
    output = BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as target:
        for info in source.infolist():
            data = source.read(info.filename)
            if info.filename.lower().endswith(".dbf"):
                header, width = struct.unpack_from("<HH", data, 8)
                offset, position = 32, 1
                while data[offset : offset + 11].split(b"\0")[0].decode("ascii") != field:
                    position += data[offset + 16]
                    offset += 32
                length = data[offset + 16]
                start = header + index * width + position
                cell = value.encode("latin-1").rjust(length)
                data = data[:start] + cell + data[start + length :]
            target.writestr(info, data)
    return output.getvalue()


def sigef(record: dict[str, str]) -> dict[str, Any]:
    return {
        "codigo_parcela": text(record["parcela_co"]),
        "rt": text(record["rt"]),
        "art": text(record["art"]),
        "situacao": text(record["situacao_i"]),
        "codigo_imovel": text(record["codigo_imo"]),
        "data_submissao": dbf_date(record["data_submi"]),
        "data_aprovacao": dbf_date(record["data_aprov"]),
        "status": text(record["status"]),
        "nome_area": text(record["nome_area"]),
        "registro_matricula": text(record["registro_m"]),
        "registro_data": dbf_date(record["registro_d"]),
        "cod_municipio": integer(record["municipio_"]),
        "uf": IBGE_UF.get(int(record["uf_id"])),
    }


def snci(record: dict[str, str]) -> dict[str, Any]:
    return {
        "num_processo": text(record["num_proces"]),
        "sr": text(record["sr"]),
        "num_certificacao": text(record["num_certif"]),
        "data_certificacao": dbf_date(record["data_certi"]),
        "area_peca_tecnica": decimal(record["qtd_area_p"]),
        "cod_profissional": text(record["cod_profis"]),
        "cod_imovel_rural": text(record["cod_imovel"]),
        "nome_imovel": text(record["nome_imove"]),
        "uf": text(record["uf_municip"]),
    }


def assentamento(record: dict[str, str]) -> dict[str, Any]:
    return {
        "codigo_sipra": text(record["cd_sipra"]),
        "nome_projeto": text(record["nome_proje"]),
        "municipio": text(record["municipio"]),
        "uf": text(record["uf"]),
        "area_ha": decimal(record["area_hecta"]),
        "capacidade": integer(record["capacidade"]),
        "num_familias": integer(record["num_famili"]),
        "fase": integer(record["fase"]),
        "data_criacao": day_month_year(record["data_de_cr"]),
        "forma_obtencao": text(record["forma_obte"]),
        "data_obtencao": day_month_year(record["data_obten"]),
        "area_calc_ha": decimal(record["area_calc_"]),
        "sr": text(record["sr"]),
        "descricao_fase": text(record["descricao_"]),
    }


def published(frame: Any) -> list[dict[str, Any]]:
    rows = []
    for row in frame.drop(columns="geometry", errors="ignore").to_dict("records"):
        cells = {}
        for column, value in row.items():
            if pd.isna(value):
                cells[column] = None
            elif hasattr(value, "to_pydatetime"):
                moment = value.to_pydatetime()
                cells[column] = moment.date() if moment.time() == moment.min.time() else moment
            else:
                cells[column] = value.item() if hasattr(value, "item") else value
        rows.append(cells)
    return rows


def rings(geometry: Any) -> Rings | None:
    if geometry is None:
        return None
    polygons = list(geometry.geoms) if geometry.geom_type == "MultiPolygon" else [geometry]
    found: Rings = []
    for polygon in polygons:
        found.append([tuple(point) for point in polygon.exterior.coords])
        found.extend([tuple(point) for point in ring.coords] for ring in polygon.interiors)
    return sorted(found)


def source_rings(archive: bytes) -> list[Rings | None]:
    return [None if item is None else sorted(item) for item in shapes(archive)]
