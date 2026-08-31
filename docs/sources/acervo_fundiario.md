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
| **SIGEF** | 15/27 (AC, AL, AM, BA, ES, GO, MA, MG, MS, MT, PA, PR, SC, SP, TO) | 8-687 MB por UF | Por UF |
| **SNCI** | 10/27 (BA, GO, MG, MS, MT, PA, PI, SC, SP, TO) | 0.6-22 MB por UF | Por UF |
| **Assentamentos** | Brasil único | 48 MB | Brasil completo, filtro UF client-side |

UFs não listadas levantam `SourceUnavailableError` com a lista de disponíveis. Ausência reflete dados upstream do INCRA, não bug do agrobr.

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

    # SNCI — parcelas certificadas pré-2013
    df = await acervo_fundiario.snci("GO")
    gdf = await acervo_fundiario.snci_geo("MT")

    # Assentamentos — Brasil único, uf opcional
    df = await acervo_fundiario.assentamentos()             # todas as UFs
    df = await acervo_fundiario.assentamentos(uf="GO")      # filtro client-side
    gdf = await acervo_fundiario.assentamentos_geo(uf="MG")

asyncio.run(main())
```

## Cache filesystem

Arquivos baixados ficam em `~/.agrobr/cache/acervo_fundiario/{tema}/{UF}.zip` com `meta.json` ao lado contendo `last_modified`, `etag`, `sha256`, `size_bytes`, `fetched_at`, `source_url`.

A revalidação usa o header `Last-Modified` do servidor: a 2ª chamada faz HEAD (~50ms) e reusa cache se o arquivo não mudou.

**Tamanho potencial do cache:**

- SIGEF Brasil completo (15 UFs) ≈ 2.4 GB (maior: MG=687 MB, SP=322 MB, PR=287 MB)
- SNCI Brasil completo (10 UFs) ≈ 84 MB
- Assentamentos Brasil = 48 MB

Por demanda. Caso casual de 1-3 UFs costuma ficar abaixo de 1 GB.

**Opt-out:**

```python
df = await acervo_fundiario.sigef("GO", use_cache=False)
```

```bash
export AGROBR_ACERVO_FUNDIARIO_CACHE_DISABLED=1
```

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
| geometry | Polygon | Geometria (apenas em `_geo`) |

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
| geometry | Polygon | Apenas em `_geo` |

## Filtros

### `bbox`

Aplicado pelo `pyogrio` durante a leitura do shapefile (filtro espacial pré-leitura, 4-9x menos RAM/tempo que filtrar `gdf.cx[]` após carregar tudo).

```python
gdf = await acervo_fundiario.sigef_geo("MG", bbox=(-44, -18, -43, -17))
```

### `uf` em assentamentos

O dataset de assentamentos é Brasil único — o filtro `uf` é client-side, normalizando a coluna `uf` (`.str.upper().str.strip()`) e comparando.

Dados upstream do INCRA têm UFs inválidas conhecidas (`MB`=501 rows, `SM`=200 rows, `12`, `'ma'`). O parser não filtra essas rows silenciosamente — um log warning `acervo_fundiario_dirty_uf_data` reporta os counts. Filtro `uf="MG"` retorna apenas rows com `MG` válido; as inválidas continuam no DataFrame quando `uf=None`.

## Limitações

- **12 UFs faltam SIGEF** (AP, CE, DF, PB, PE, PI, RJ, RN, RO, RR, RS, SE) — não há registro upstream
- **17 UFs faltam SNCI** — só Centro-Oeste/Sudeste/Sul/parte do Norte cobertos
- **Cert SSL inválido** no servidor — agrobr usa TLS relaxado (`check_hostname=False`, `verify_mode=CERT_NONE`). Servidor é gov.br público.
- **Sem distinção particular/público** — o shapefile não tem campo de tipo (era distinção do WFS legacy)
- **Tamanho de cache pode acumular GB** — ver seção "Cache filesystem"
