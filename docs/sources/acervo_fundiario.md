# Acervo Fundiário — SIGEF, SNCI e Assentamentos (INCRA)

!!! warning "Licença `nc` — vedado uso comercial"
    Os dados do Acervo Fundiário do INCRA são de uso público com restrição de uso comercial.
    A primeira chamada emite `UserWarning` lembrando dessa restrição.

!!! note "Nao acessivel de fora do Brasil nos ambientes testados"
    O host `certificacao.incra.gov.br` respondeu normalmente do Brasil, mas nao respondeu
    (connection timeout) a partir dos runners do GitHub Actions nem de nenhum dos 8 nos
    internacionais testados em 31/08/2026 (Austria, Chipre, Finlandia, Ira, Servia, Ucrania).
    Se voce roda o agrobr fora do Brasil e recebe `httpx.ConnectTimeout` nesta fonte,
    a causa provavel e essa restricao de rede — nao um bug da biblioteca.
    Por isso os testes live desta fonte usam o marker `integration_br` e ficam fora do CI.

!!! info "Dependência geoespacial"
    Instale `pip install agrobr[geo]` antes de qualquer consulta. O extra inclui
    `geopandas` e `pyogrio`; a disponibilidade é conferida antes do download dos
    ZIPs, que podem ter centenas de megabytes.

## Visão Geral

| Item | Detalhe |
|------|---------|
| Provedor | INCRA (Instituto Nacional de Colonização e Reforma Agrária) |
| Dados | Parcelas certificadas (SIGEF/SNCI) + assentamentos |
| Acesso | Download de shapefile ZIP estático |
| Endpoint | `https://certificacao.incra.gov.br/csv_shp/zip/` |
| CRS | EPSG:4674 (SIRGAS 2000) |
| Encoding | DBF latin1 (cp1252) |
| Atualização | Contínua (varia por UF, exposta via `Last-Modified`) |
| Autenticação | Nenhuma |
| Licença | Vedado uso comercial — `nc` |

## Cobertura por dataset

| Dataset | UFs disponíveis | Tamanho típico | Granularidade |
|---|---|---|---|
| **SIGEF** | 27/27 | 2-766 MB por UF | Por UF |
| **SNCI** | 27/27 | 0,01-23 MB por UF | Por UF |
| **Assentamentos** | Brasil único | 50 MB | Brasil completo, filtro UF client-side |

O INCRA publica SIGEF e SNCI para as 27 UFs (22/09/2026). UF sem arquivo no servidor levanta `SourceUnavailableError` (HTTP 404).

## Funções públicas

```python
import asyncio
from agrobr import acervo_fundiario

async def main():
    # SIGEF — parcelas certificadas pós-2013
    df = await acervo_fundiario.sigef("GO")
    df, meta = await acervo_fundiario.sigef("MG", return_meta=True)
    df_pl = await acervo_fundiario.sigef("SP", as_polars=True)
    gdf = await acervo_fundiario.sigef_geo("GO", bbox=(-50, -16, -49, -15))

    # SNCI — certificações do sistema anterior ao SIGEF (sem corte por data; há registros até 2016)
    df = await acervo_fundiario.snci("GO")
    gdf = await acervo_fundiario.snci_geo("MT")

    # Assentamentos — Brasil único, uf opcional
    df = await acervo_fundiario.assentamentos()             # todas as UFs
    df = await acervo_fundiario.assentamentos(uf="GO")      # filtro client-side
    gdf = await acervo_fundiario.assentamentos_geo(uf="MG")

asyncio.run(main())
```

## Cache filesystem

Arquivos baixados ficam em `~/.agrobr/cache/acervo_fundiario/{tema}/{UF}.zip` com `{UF}.json` ao lado contendo `last_modified`, `etag`, `sha256`, `size_bytes`, `fetched_at`, `source_url`. Onde fica e como limpar: [O que o agrobr grava no disco](../advanced/disco.md).

Com `return_meta=True`, o `MetaInfo` diz de onde veio o arquivo. Quando o HEAD confirma o cache: `from_cache=True`, `fetched_at` = coleta original do ZIP (o `fetched_at` do `{UF}.json`) e `source_details` com `revalidado_em`, `etag` e `last_modified`. Num download novo: `from_cache=False`, `fetched_at` = o download e `source_details` só com `etag` e `last_modified`.

A revalidação faz HEAD e exige ao menos um validador presente (`ETag` ou
`Last-Modified`) coincidente. Divergências nesses headers ou no tamanho
invalidam o cache; ausência de ambos exige novo download. Locks são locais
ao event loop, e cada escrita limpa somente seu próprio temporário.

**Tamanho potencial do cache:**

- SIGEF Brasil completo (27 UFs) ≈ 3,1 GB (maior: MG=766 MB, SP=356 MB, PR=312 MB)
- SNCI Brasil completo (27 UFs) ≈ 105 MB
- Assentamentos Brasil = 50 MB

Por demanda. Caso casual de 1-3 UFs costuma ficar abaixo de 1 GB.

**Opt-out:**

```python
df = await acervo_fundiario.sigef("GO", use_cache=False)
```

```bash
export AGROBR_ACERVO_FUNDIARIO_CACHE_DISABLED=1
```

Com o cache desligado, o ZIP vai para uma pasta temporária do sistema e é apagado ao fim da consulta, com
sucesso, erro ou cancelamento; nada é gravado em `~/.agrobr/cache/acervo_fundiario/`. Os booleanos da variável
aceitam `1`/`true`/`yes`.

## Schemas

### SIGEF

| Coluna | Tipo | Descrição |
|---|---|---|
| codigo_parcela | str | UUID da parcela |
| rt | str | Responsável técnico |
| art | str | Anotação de responsabilidade técnica |
| situacao | str | Situação informada |
| codigo_imovel | str | Código do imóvel rural |
| data_submissao | datetime | Data de submissão |
| data_aprovacao | datetime | Data de aprovação |
| status | str | Status da certificação |
| nome_area | str | Nome da área/fazenda |
| registro_matricula | str | Matrícula do registro |
| registro_data | datetime | Data do registro (nullable) |
| cod_municipio | int | Código IBGE do município |
| uf | str | Sigla UF (mapeada de `uf_id` IBGE) |
| geometry | Polygon Z | Geometria com altitude nos vértices (apenas em `_geo`) |

### SNCI

| Coluna | Tipo | Descrição |
|---|---|---|
| num_processo | str | Número do processo |
| sr | str | Superintendência regional |
| num_certificacao | str | Número da certificação |
| data_certificacao | datetime | Data da certificação |
| area_peca_tecnica | float | Área em hectares (peça técnica) |
| cod_profissional | str | Código do profissional credenciado |
| cod_imovel_rural | str | Código do imóvel rural |
| nome_imovel | str | Nome do imóvel |
| uf | str | Sigla UF (de `uf_municip`) |
| geometry | Polygon | Apenas em `_geo` |

### Assentamentos

| Coluna | Tipo | Descrição |
|---|---|---|
| codigo_sipra | str | Código SIPRA do projeto |
| nome_projeto | str | Nome do projeto |
| municipio | str | Município |
| uf | str | Sigla UF |
| area_ha | float | Área declarada em hectares |
| capacidade | int | Capacidade de famílias |
| num_familias | int | Número de famílias assentadas |
| fase | int | Fase do projeto |
| data_criacao | datetime | Data de criação |
| forma_obtencao | str | Forma de obtenção |
| data_obtencao | datetime | Data de obtenção |
| area_calc_ha | float | Área calculada em hectares |
| sr | str | Superintendência regional (nullable) |
| descricao_fase | str | Descrição da fase (nullable) |
| geometry | Polygon | Apenas em `_geo`; nula quando a fonte publica o registro sem geometria (1 de 8.216 em 22/09/2026) |

## Filtros

### `bbox`

Aplicado pelo `pyogrio` durante a leitura do shapefile (filtro espacial pré-leitura, 4-9x menos RAM/tempo que filtrar `gdf.cx[]` após carregar tudo).

```python
gdf = await acervo_fundiario.sigef_geo("MG", bbox=(-44, -18, -43, -17))
```

### `uf` em assentamentos

O dataset de assentamentos é Brasil único — o filtro `uf` é client-side, normalizando a coluna `uf` (`.str.upper().str.strip()`) e comparando.

Se a fonte trouxer UF fora das 27 siglas, o parser não descarta a linha: o log `acervo_fundiario_dirty_uf_data` reporta as contagens. Filtro `uf="MG"` retorna apenas linhas com `MG`; as demais continuam no DataFrame quando `uf=None`. Na captura de 22/09/2026, as 8.216 linhas tinham UF válida.

## Limitações

- **TLS verificado** — downloads e health check validam certificado e hostname.
  Falhas de certificado interrompem a conexão, sem desabilitar a verificação.
- **Sem distinção particular/público** — o shapefile não tem campo de tipo (era distinção do WFS legacy)
- **Tamanho de cache pode acumular GB** — ver seção "Cache filesystem"
