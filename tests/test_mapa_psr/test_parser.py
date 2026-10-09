"""Testes para agrobr.alt.mapa_psr.parser."""

from __future__ import annotations

from agrobr.alt.mapa_psr.parser import (
    parse_apolices,
)


def _make_csv(
    rows: list[dict[str, str]] | None = None,
    sep: str = ";",
    encoding: str = "utf-8",
    include_pii: bool = False,
    include_geo: bool = False,
) -> bytes:
    if rows is None:
        rows = [
            {
                "ANO_APOLICE": "2023",
                "NR_APOLICE": "AP001",
                "SG_UF_PROPRIEDADE": "MT",
                "NM_MUNICIPIO_PROPRIEDADE": "SORRISO",
                "CD_GEOCMU": "5107925",
                "NM_CULTURA_GLOBAL": "SOJA",
                "NM_CLASSIF_PRODUTO": "AGRICOLA",
                "NR_AREA_TOTAL": "500.5",
                "VL_PREMIO_LIQUIDO": "15000.00",
                "VL_SUBVENCAO_FEDERAL": "6000.00",
                "VL_LIMITE_GARANTIA": "250000.00",
                "VALOR_INDENIZACAO": "120000.00",
                "EVENTO_PREPONDERANTE": "SECA",
                "NR_PRODUTIVIDADE_ESTIMADA": "60.0",
                "NR_PRODUTIVIDADE_SEGURADA": "48.0",
                "NivelDeCobertura": "80",
                "PE_TAXA": "7.5",
                "NM_RAZAO_SOCIAL": "Seguradora ABC",
            },
            {
                "ANO_APOLICE": "2023",
                "NR_APOLICE": "AP002",
                "SG_UF_PROPRIEDADE": "PR",
                "NM_MUNICIPIO_PROPRIEDADE": "LONDRINA",
                "CD_GEOCMU": "4113700",
                "NM_CULTURA_GLOBAL": "MILHO",
                "NM_CLASSIF_PRODUTO": "AGRICOLA",
                "NR_AREA_TOTAL": "200.0",
                "VL_PREMIO_LIQUIDO": "8000.00",
                "VL_SUBVENCAO_FEDERAL": "3200.00",
                "VL_LIMITE_GARANTIA": "100000.00",
                "VALOR_INDENIZACAO": "0",
                "EVENTO_PREPONDERANTE": "",
                "NR_PRODUTIVIDADE_ESTIMADA": "120.0",
                "NR_PRODUTIVIDADE_SEGURADA": "96.0",
                "NivelDeCobertura": "80",
                "PE_TAXA": "6.0",
                "NM_RAZAO_SOCIAL": "Seguradora XYZ",
            },
            {
                "ANO_APOLICE": "2022",
                "NR_APOLICE": "AP003",
                "SG_UF_PROPRIEDADE": "GO",
                "NM_MUNICIPIO_PROPRIEDADE": "RIO VERDE",
                "CD_GEOCMU": "5218805",
                "NM_CULTURA_GLOBAL": "SOJA",
                "NM_CLASSIF_PRODUTO": "AGRICOLA",
                "NR_AREA_TOTAL": "1000.0",
                "VL_PREMIO_LIQUIDO": "30000.00",
                "VL_SUBVENCAO_FEDERAL": "12000.00",
                "VL_LIMITE_GARANTIA": "500000.00",
                "VALOR_INDENIZACAO": "350000.00",
                "EVENTO_PREPONDERANTE": "GEADA",
                "NR_PRODUTIVIDADE_ESTIMADA": "55.0",
                "NR_PRODUTIVIDADE_SEGURADA": "44.0",
                "NivelDeCobertura": "80",
                "PE_TAXA": "8.0",
                "NM_RAZAO_SOCIAL": "Seguradora ABC",
            },
        ]

    headers = list(rows[0].keys())
    if include_pii:
        headers.extend(["NM_SEGURADO", "NR_DOCUMENTO_SEGURADO"])
    if include_geo:
        headers.extend(["LATITUDE", "LONGITUDE", "NR_GRAU_LAT"])

    lines = [sep.join(headers)]
    for row in rows:
        vals = [row.get(h, "") for h in list(rows[0].keys())]
        if include_pii:
            vals.extend(["Joao Silva", "12345678900"])
        if include_geo:
            vals.extend(["-12.5", "-55.7", "12"])
        lines.append(sep.join(vals))

    return "\n".join(lines).encode(encoding)


class TestParseApolices:
    def test_filtro_cultura_com_acento(self):
        rows = [
            {
                "ANO_APOLICE": "2023",
                "NR_APOLICE": "AP001",
                "SG_UF_PROPRIEDADE": "MG",
                "NM_MUNICIPIO_PROPRIEDADE": "PATROCINIO",
                "CD_GEOCMU": "3148103",
                "NM_CULTURA_GLOBAL": "CAF\u00c9 AR\u00c1BICA",
                "NM_CLASSIF_PRODUTO": "AGRICOLA",
                "NR_AREA_TOTAL": "100.0",
                "VL_PREMIO_LIQUIDO": "5000.00",
                "VL_SUBVENCAO_FEDERAL": "2000.00",
                "VL_LIMITE_GARANTIA": "50000.00",
                "VALOR_INDENIZACAO": "0",
                "EVENTO_PREPONDERANTE": "",
                "NR_PRODUTIVIDADE_ESTIMADA": "30.0",
                "NR_PRODUTIVIDADE_SEGURADA": "24.0",
                "NivelDeCobertura": "80",
                "PE_TAXA": "5.0",
                "NM_RAZAO_SOCIAL": "Seguradora ABC",
            },
            {
                "ANO_APOLICE": "2023",
                "NR_APOLICE": "AP002",
                "SG_UF_PROPRIEDADE": "MT",
                "NM_MUNICIPIO_PROPRIEDADE": "SORRISO",
                "CD_GEOCMU": "5107925",
                "NM_CULTURA_GLOBAL": "ALGOD\u00c3O",
                "NM_CLASSIF_PRODUTO": "AGRICOLA",
                "NR_AREA_TOTAL": "500.0",
                "VL_PREMIO_LIQUIDO": "15000.00",
                "VL_SUBVENCAO_FEDERAL": "6000.00",
                "VL_LIMITE_GARANTIA": "250000.00",
                "VALOR_INDENIZACAO": "0",
                "EVENTO_PREPONDERANTE": "",
                "NR_PRODUTIVIDADE_ESTIMADA": "60.0",
                "NR_PRODUTIVIDADE_SEGURADA": "48.0",
                "NivelDeCobertura": "80",
                "PE_TAXA": "7.5",
                "NM_RAZAO_SOCIAL": "Seguradora XYZ",
            },
        ]
        csv_bytes = _make_csv(rows=rows)
        df_cafe = parse_apolices(csv_bytes, cultura="cafe")
        assert len(df_cafe) == 1
        assert "CAF" in df_cafe.iloc[0]["cultura"]

        df_algodao = parse_apolices(csv_bytes, cultura="algodao")
        assert len(df_algodao) == 1
        assert "ALGOD" in df_algodao.iloc[0]["cultura"]

    def test_filtro_municipio_pelo_codigo_e_nao_pelo_rotulo(self):
        csv_bytes = _make_csv().replace(b"SORRISO", b"SORRISO (NORTE)")
        sorriso = {"codigo_ibge": 5107925, "nome": "Sorriso", "uf": "MT"}
        df = parse_apolices(csv_bytes, municipio=sorriso)
        assert df["municipio"].tolist() == ["SORRISO (NORTE)"]
        assert df["cd_ibge"].tolist() == ["5107925"]
