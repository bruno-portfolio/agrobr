# IMEA — Cotações e Indicadores MT

> As séries públicas do IMEA são classificadas como `zona_cinza`: não foi comprovada licença de reutilização nem que a cláusula de arquivos não públicos alcance esse recorte. Os termos condicionam o compartilhamento de arquivos não públicos à autorização prévia por escrito; essa restrição permanece para tais arquivos. As reservas sobre bases de dados e outros ativos não são uma licença aberta. O módulo avisa na primeira chamada. [Termo de Uso do IMEA](https://imea.com.br/imea-site/termo-de-uso.html).

!!! warning "Licença do recorte público"
    O primeiro uso emite `UserWarning` sobre a classificação `zona_cinza`. A restrição expressa de compartilhamento permanece para arquivos não públicos.

Instituto Mato-Grossense de Economia Agropecuária.
Cotações diárias, indicadores de preço e dados de safra para Mato Grosso.

## API

```python
from agrobr import imea

# Cotações de soja em MT
df = await imea.cotacoes("soja")

# Filtrar por safra
df = await imea.cotacoes("soja", safra="24/25")

# Filtrar por unidade (veja "Unidade e indicador" abaixo)
df = await imea.cotacoes("soja", unidade="R$/sc")

# Outras cadeias
df = await imea.cotacoes("milho")
df = await imea.cotacoes("algodao")
df = await imea.cotacoes("bovinocultura")
df = await imea.cotacoes("custo_producao")
```

Argumento desconhecido (ex.: `municipio=`) levanta `TypeError` antes de qualquer requisição.

## Colunas — `cotacoes`

| Coluna | Tipo | Descrição |
|---|---|---|
| `cadeia` | str | Cadeia pedida (ver "Frete de grãos" abaixo) |
| `indicador_id` | str | Identificador do indicador na fonte (`IndicadorFinalId`, texto como publicado; há ids de 18 dígitos) |
| `indicador` | str | Nome oficial do indicador (ex.: "Preço soja disponível compra"); nulo se o id não estiver no catálogo da cadeia |
| `localidade` | str | Praça, macrorregião ou categoria, conforme o indicador (na conjuntura, "Algodão", "Total MT"; na semente de soja, "Convencional"/"Transgênica") |
| `valor` | float | Valor publicado (nulo quando a fonte não publica) |
| `variacao` | float | Variação (%) |
| `safra` | str | Safra (ex: "24/25"; nula nos indicadores sem safra) |
| `unidade` | str | Unidade (R$/sc, R$/t, R$/ha, %...) |
| `unidade_descricao` | str | Descrição da unidade |
| `data_publicacao` | datetime64[ns] | Data e hora de publicação (nula em parte dos registros) |

Uma linha é identificada por `indicador_id` + `localidade` + `data_publicacao` + `safra` + `unidade`. Sem o indicador,
milhares de linhas da mesma cadeia repetem as outras quatro colunas (ex.: soja, 4.140 de 4.568 em 23/09/2026).

**Registro publicado em duplicata.** A fonte às vezes publica o mesmo registro mais de uma vez: em 25/09/2026, 23
registros do indicador `708192508838936580` (R$/sc, Mato Grosso e 22 municípios) saíram quatro vezes cada.

- O registro igual em todas as colunas sai uma vez só, com aviso, e a contagem vai em
  `source_details["duplicatas_colapsadas"]` (`linhas` e `indicadores`).
- A chave repetida com valores diferentes não se colapsa: as linhas saem todas, com aviso e a contagem em
  `source_details["chaves_repetidas"]`. Um erro derrubaria a cadeia inteira por um indicador.
- Os 2 avisos vão para `meta.validation_warnings` em toda chamada; o `UserWarning` sai só na 1ª chamada de cada cadeia no processo.

### Unidade e indicador

`unidade` sozinha não identifica o produto. Na soja, `unidade="R$/sc"` traz o grão (saca de 60 kg, ex.: "Preço soja
disponível compra" em Sorriso) **e a semente** ("SEMENTE SOJA BRANCA (R$/sc 40kg)", localidade "Convencional" ou
"Transgênica"). Separe pelo `indicador`:

```python
df = await imea.cotacoes("soja", unidade="R$/sc")
grao = df[~df["indicador"].str.contains("semente", case=False, na=False)]
```

### Frete de grãos

O frete ("Preço disponível do Frete de Grãos", R$/t) é publicado nas cadeias soja e milho; em cada consulta ele sai com a
cadeia pedida. Ao juntar soja e milho, deduplique por `indicador_id` + `data_publicacao` + `localidade`.

## Cadeias Produtivas

| Nome agrobr | Cadeia IMEA (id) |
|---|---|
| `soja` / `soybeans` | Soja (4) |
| `milho` / `corn` | Milho (3) |
| `algodao` / `cotton` | Algodão (1) |
| `bovinocultura` / `boi` / `boi_gordo` / `bovinos` / `cattle` | Bovinocultura de Corte (2) |
| `suinocultura` / `pork` | Suinocultura (7) |
| `leite` / `dairy` | Leite (8) |
| `conjuntura` | Conjuntura Econômica (5): VBP, cesta básica e índices de preços no varejo (Cuiabá) |
| `custo_producao` | Custo de Produção (10): preços de insumos (semente, tratamento de semente) |

O número da cadeia também é aceito (`"5"`, `"10"`). As cadeias inativas na fonte (6 Madeira Nativa, 9 Aves,
11 Geoprocessamento), nome desconhecido ou valor que não é texto levantam `InvalidParameterError`, com as
opções, antes da rede.

## MetaInfo

```python
df, meta = await imea.cotacoes("soja", return_meta=True)
print(meta.source)  # "imea"
print(meta.source_url)  # .../v2/mobile/cadeias/4/cotacoes
print(meta.source_details["indicadores_url"])  # .../v2/mobile/cadeias/4/indicadores
print(meta.source_details["indicadores_sha256"])  # SHA-256 do catálogo de indicadores
print(meta.source_details["indicadores_bytes"])  # tamanho do catálogo em bytes
print(meta.source_details["duplicatas_colapsadas"])  # {"linhas": 0, "indicadores": []}
print(meta.source_details["chaves_repetidas"])  # {"linhas": 0, "indicadores": []}
```

## Fonte

- API: `https://api1.imea.com.br/api/v2/mobile/cadeias/{id}/cotacoes` e, para o nome dos indicadores,
  `.../cadeias/{id}/indicadores` (duas requisições por consulta)
- Formato: JSON (REST API); chave publicada ausente levanta `ParseError`
- Atualização: diária
- Cobertura: Mato Grosso
- Autenticação: nenhuma (API pública)
- Licença: `zona_cinza` para séries públicas; arquivos não públicos exigem autorização escrita.
