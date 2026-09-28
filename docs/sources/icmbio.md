# ICMBio — Unidades de Conservacao Federais

## Visao Geral

| Item | Detalhe |
|------|---------|
| Provedor | ICMBio (Instituto Chico Mendes de Conservacao da Biodiversidade) |
| Dados | Limites de UCs federais |
| Acesso | WFS OGC (INDE GeoServer) |
| Formato | CSV (tabular) / GeoJSON (geo) |
| Autenticacao | Nenhuma |
| Licenca | Dados publicos governo federal |
| Features | 347 UCs na captura de 18/09/2026 (contagem variável) |

## Acesso via WFS

| Parametro | Valor |
|-----------|-------|
| Endpoint | `geoservicos.inde.gov.br/geoserver/ICMBio/ows` |
| WFS Version | 1.1.0 |
| Layer | `ICMBio:limiteucsfederais_a` |
| CRS | EPSG:4674 na camada; `ucs_geo` pede `srsName=EPSG:4326` |

## Exemplo de Uso

```python
import asyncio
from agrobr import icmbio

async def main():
    # Todas as UCs federais
    df = await icmbio.ucs()

    # Filtrar por grupo (PI = protecao integral, US = uso sustentavel)
    df = await icmbio.ucs(grupo="PI")

    # Filtrar por UF (usa LIKE, funciona com UCs multi-UF)
    df = await icmbio.ucs(uf="MT")

    # Com geometria (requer geopandas)
    gdf = await icmbio.ucs_geo(bbox=(-56, -16, -54, -14))

    # Com metadados
    df, meta = await icmbio.ucs(return_meta=True)

asyncio.run(main())
```

## Colunas

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| codigo | str | CNUC publicado; repetições são preservadas |
| nome | str | Nome da UC |
| categoria | str | Sigla da categoria (PARNA, ESEC, FLONA, etc) |
| grupo | str | PI (protecao integral) ou US (uso sustentavel) |
| uf | str | UF(s) abrangidas (separadas por /) |
| bioma | str | Bioma IBGE |
| area_ha | float | Area em hectares |
| ano_criacao | Int64 | Ano de criacao |
| ato_criacao | str | Ato legal de criacao |

## Limitacoes

- Apenas UCs federais; a contagem corrente varia. Estaduais e municipais nao estao neste WFS.
- Campo `uf` pode conter multiplas UFs (ex: "MT/PA")
- `area_ha` é a área da UC inteira, não a parte dentro da UF: `uf="SP"` devolve 22 UCs, 7 delas com outra UF e a área toda (a APA das Ilhas e Várzeas do Rio Paraná, SP/PR/MS, sai com 1.005.181 ha). Somar `area_ha` por UF conta essas UCs mais de uma vez.
- Dados refletem o estado atual do GeoServer INDE/ICMBio

## Reconciliação da camada corrente

A captura de 18/09/2026 contém 347 UCs na consulta sem filtros, 50 com
`bioma="Cerrado"` e 22 com `uf="SP"`. São seleções sobrepostas da mesma camada:
419 ocorrências conferidas, com 347 códigos CNUC distintos nesta captura.
Não constituem fontes independentes nem comprovam outra data histórica.
O contrato não impõe chave primária nem remove eventuais repetições futuras.

As nove colunas de saída foram comparadas célula a célula com CSVs integrais,
inclusive primeiro e último registro, tanto na fonte quanto no dataset.
`areahaalb` é preservado como `area_ha`, sem soma ou conversão de escala;
`criacaoano` é atributo da UC, não edição da camada. Os textos de UF/bioma
compostos permanecem integrais: 43 UCs do corpo sem filtros têm múltiplas UFs.
Não houve campos vazios nestes CSVs; isso não elimina a nulabilidade de área/ano.

O WFS devolveu 11 campos, incluindo `FID` e `ogc_fid`, mantidos nos localizadores
do oráculo e ausentes da saída pública. O inventário XSD cobre as 22 propriedades,
com decisão nominal para as 13 fora do contrato tabular, inclusive geometria.
O verificador N1 compara esse inventário e a estrutura dos CSVs; não altera dados
nem substitui a aquisição real. A concordância com `numberOfFeatures` consultado
antes do CSV é registrada como `count_reconciled`, sem snapshot transacional.

Evidência portátil: `tests/golden_data/reconciliacao_r11_20260918/icmbio/`.
O replay usa os corpos oficiais completos e os parâmetros HTTP reais. O parser 3
e o contrato 1.0 permanecem inalterados. Reconciliação de geometria fica fora desta
variante tabular.

`ucs_geo` pede `srsName=EPSG:4326` e confere o CRS que o corpo declara: se vier
outro (o nativo da camada é EPSG:4674), levanta `ParseError` em vez de publicar
as coordenadas com o rótulo 4326. Evidência: `tests/golden_data/icmbio/crs_20260923/`.
