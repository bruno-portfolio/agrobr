# CNUC — Cadastro Nacional de Unidades de Conservação

## Visão geral

| Item | Detalhe |
|------|---------|
| Provedor | MMA (Ministério do Meio Ambiente e Mudança do Clima), Departamento de Áreas Protegidas |
| Dados | Limites e atributos das UCs federais, estaduais e municipais, com RPPNs |
| Acesso | WFS 2.0 (MapServer do portal CNUC) |
| Formato | GML 3.2 (tabular e geo) |
| Autenticação | Nenhuma |
| Licença | CC-BY (dataset "Unidades de Conservação" no portal de dados abertos do MMA) |
| Feições | 3.450 UCs com limite na captura de 01/10/2026 (contagem variável) |

## Acesso via WFS

| Parâmetro | Valor |
|-----------|-------|
| Portal | `cnuc.mma.gov.br` |
| Endpoint | `cnuc-mapserv.mma.gov.br/cgi-bin/mapserv?MAP=/var/www/storage/app/mapfiles/ucs.map` |
| Versão WFS | 2.0.0 |
| Camada | `ms:ucs_selected` |
| CRS | EPSG:4674 na camada; `ucs_geo` pede `urn:ogc:def:crs:EPSG::4326` |
| Dados abertos | `dados.mma.gov.br/dataset/unidadesdeconservacao` (CSV semestral e licença) |

O endereço do MapServer é o que o próprio portal CNUC usa no mapa. O parâmetro `MAP` aponta um caminho interno do servidor: se o MMA reorganizar o servidor, a URL muda sem aviso, e a consulta levanta `SourceUnavailableError`.

## Exemplo de uso

```python
import asyncio
from agrobr import cnuc

async def main():
    # UCs das 3 esferas em Sergipe, com RPPNs
    df = await cnuc.ucs(uf="SE")

    # Só as RPPNs federais
    df = await cnuc.ucs(
        esfera="federal", categoria="Reserva Particular do Patrimônio Natural"
    )

    # UCs que abrangem um município (nome inteiro ou código IBGE)
    df = await cnuc.ucs(municipio="Aracaju")

    # Com geometria (requer geopandas)
    gdf = await cnuc.ucs_geo(uf="SE", esfera="municipal")

    # Com metadados
    df, meta = await cnuc.ucs(bbox=(-38.5, -11.6, -36.3, -9.3), return_meta=True)

asyncio.run(main())
```

## Funções e parâmetros

`cnuc.ucs(*, uf=None, municipio=None, esfera=None, categoria=None, grupo=None, bioma=None, bbox=None, max_registros=None, as_polars=False, return_meta=False)` devolve o tabular. `cnuc.ucs_geo(...)` aceita os mesmos filtros, sem `as_polars`, e devolve um `GeoDataFrame` em EPSG:4326.

| Parâmetro | Valores | Onde filtra |
|-----------|---------|-------------|
| `uf` | sigla da UF | no servidor pelo nome da UF (em `MT`, sem as UCs só de Mato Grosso do Sul); o resultado casa a sigla exata dentro de `uf` |
| `municipio` | nome inteiro ou código IBGE de 7 dígitos | pela UF do município no servidor; cada município publicado é comparado ao cadastro do IBGE |
| `esfera` | `federal`, `estadual`, `municipal` | no servidor |
| `categoria` | as 12 categorias de manejo publicadas, sem caixa e acento | no servidor |
| `grupo` | `PI`, `US` | no servidor |
| `bioma` | Amazônia, Caatinga, Cerrado, Mata Atlântica, Pampa, Pantanal | no resultado, pela coluna `bioma` |
| `bbox` | (lon mín, lat mín, lon máx, lat máx) em EPSG:4326 | no servidor |
| `max_registros` | inteiro positivo | as primeiras UCs em ordem de `codigo` |

Valor fora do domínio levanta `InvalidParameterError` antes da rede, com a lista dos válidos. As 12 categorias: Área de Proteção Ambiental, Área de Relevante Interesse Ecológico, Estação Ecológica, Floresta, Monumento Natural, Parque, Refúgio de Vida Silvestre, Reserva Biológica, Reserva de Desenvolvimento Sustentável, Reserva de Fauna, Reserva Extrativista e Reserva Particular do Patrimônio Natural.

A consulta conta as UCs no servidor antes de baixar. Acima de 10.000 UCs no tabular, ou de 600 no `ucs_geo`, levanta `ResourceLimitError` sem baixar nada; a maior UF (RJ) tem 568. O `bioma` sozinho não reduz o download, porque filtra no resultado. Quando não há filtro aplicado no resultado (`uf`, `municipio` e `bioma`), `max_registros` também reduz o download no servidor. A mensagem do erro sugere só o que reduz o download: `uf`, `esfera`, `categoria`, `grupo` e `bbox`, e `max_registros` quando não há filtro no resultado.

## Colunas

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| codigo | str | Código CNUC |
| nome | str | Nome da UC |
| esfera | str | `federal`, `estadual` ou `municipal` |
| categoria | str | Categoria de manejo, como publicada |
| grupo | str | `PI` (proteção integral) ou `US` (uso sustentável) |
| categoria_iucn | str | Categoria IUCN publicada |
| uf | str | Siglas em ordem alfabética, separadas por `/` |
| municipios | str | Lista publicada, `NOME (UF), …` |
| bioma | str | Biomas com área publicada na UC, separados por `/` |
| area_ha | float | Área em hectares |
| data_criacao | datetime64[ns] | Data de criação |
| ato_criacao | str | Ato legal de criação |
| orgao_gestor | str | Órgão gestor |
| qualidade_poligono | str | Polígono pelo memorial descritivo, estimativa ou representação esquemática |
| wdpa_id | str | Identificador na base mundial de áreas protegidas |

O `ucs_geo` acrescenta `geometry` (Polygon ou MultiPolygon).

## Limitações

- Só UCs com limite cadastrado no CNUC. Das 3.576 UCs do cadastro CSV de julho de 2026, 170 não estavam na camada em 01/10/2026: 167 têm a área tirada do ato legal, ou seja, sem polígono no CNUC, e 3 têm área do polígono, como a FLONA de Cristópolis (BA), que está no CSV e no ICMBio.
- As zonas de amortecimento da camada (61 em 01/10/2026, `limite='za'`) ficam de fora.
- `municipios` vem cortado pela fonte em 200 caracteres, com `...`, em 20 UCs. Com `municipio=`, as UCs da UF com a lista cortada que não citam o município antes do corte ficam fora e aparecem no aviso.
- Três grafias da camada não casam com o cadastro do IBGE (`GRÃO PARÁ (SC)`, `SANTO ANTÔNIO DO LEVERGER (MT)` e `SÃO THOMÉ DAS LETRAS (MG)`); essas UCs também ficam fora do filtro `municipio=` e aparecem no aviso. Os avisos chegam a `MetaInfo.validation_warnings` e saem como `UserWarning`.
- `area_ha` é a área da UC inteira, não a parte dentro da UF.
- `bioma` é derivado das áreas por bioma publicadas: a área marinha fica fora, e 65 UCs saem com `bioma` nulo.
- A camada é corrente, sem edição histórica: `deterministic` não se aplica.

## Conteúdo da camada

Em 01/10/2026, a camada tinha 3.511 feições: 3.450 UCs e 61 zonas de amortecimento. Por esfera: 1.086 federais (736 RPPNs), 1.458 estaduais (669 RPPNs) e 906 municipais (21 RPPNs). O código CNUC não se repetiu. Das 347 UCs federais da camada do ICMBio (`icmbio.ucs`), 346 estão aqui; o CNUC já tinha 4 RDS federais de 08/09/2026 que o ICMBio ainda não publicava.

A contagem do servidor (`resultType=hits`) vem antes do download e é conciliada com o número de feições recebidas (`count_reconciled` no `MetaInfo`), sem snapshot transacional. O CNUC é atualizado pelos órgãos gestores, UC a UC.

## Relação com o ICMBio

`icmbio.ucs` e o dataset `unidades_conservacao_federais` continuam com a camada do ICMBio na INDE: só UCs federais, sem RPPN. Para as três esferas e as RPPNs, use `cnuc.ucs` ou o dataset `unidades_conservacao`.
