from agrobr.constants import URLS, Fonte

GRAPHQL_URL: str = URLS[Fonte.MAPBIOMAS_ALERTA]["graphql"]

PAGE_SIZE = 500
MAX_REGISTROS_PADRAO = 5000
TIPOS_DATA: dict[str, str] = {"deteccao": "DetectedAt", "publicacao": "PublishedAt"}
JANELA_PUBLICACAO_DIAS = 293
FONTES: frozenset[str] = frozenset(
    {
        "All",
        "DeterbAmazonia",
        "DeterCerrado",
        "DeterPantanal",
        "Glad",
        "IefMg",
        "InemaBa",
        "ProdesAmazonia",
        "ProdesCerrado",
        "ProdesMataAtlantica",
        "ProdesPampa",
        "ProdesPantanal",
        "ProdesCaatinga",
        "Sad",
        "SadCaatinga",
        "SadCerrado",
        "SadMataAtlantica",
        "SadPampa",
        "SadPantanal",
        "SipamSar",
        "SiradX",
        "SosAtlas",
        "SosInpe",
    }
)

ALERTS_QUERY = """
query alerts(
  $page: Int, $limit: Int,
  $startDate: BaseDate, $endDate: BaseDate, $dateType: DateTypes,
  $sources: [SourceTypes!],
  $boundingBox: [Float!],
  $territoryIds: [Int!],
  $sortField: AlertSortField, $sortDirection: SortDirection
) {
  alerts(
    page: $page, limit: $limit,
    startDate: $startDate, endDate: $endDate, dateType: $dateType,
    sources: $sources,
    boundingBox: $boundingBox,
    territoryIds: $territoryIds,
    sortField: $sortField, sortDirection: $sortDirection
  ) {
    collection {
      alertCode
      areaHa
      detectedAt
      publishedAt
      statusName
      sources
      coordenates { latitude longitude }
      geometryWkt
    }
    metadata {
      currentPage
      totalCount
      totalPages
    }
  }
}
"""

ALERT_DATE_RANGE_QUERY = """
{
  alertDateRange {
    minDetectedAt
    maxDetectedAt
    minPublishedAt
    maxPublishedAt
  }
}
"""

LAST_PUBLICATION_QUERY = """
{
  lastAlertPublication {
    publishedAt
    total
  }
}
"""

RENAME_MAP: dict[str, str] = {
    "alertCode": "alert_code",
    "areaHa": "area_ha",
    "detectedAt": "data_deteccao",
    "publishedAt": "data_publicacao",
    "statusName": "status",
}

COLUNAS_SAIDA: list[str] = [
    "alert_code",
    "area_ha",
    "data_deteccao",
    "data_publicacao",
    "status",
    "fonte",
    "lat",
    "lon",
]

COLUNAS_SAIDA_GEO: list[str] = COLUNAS_SAIDA + ["geometry"]
