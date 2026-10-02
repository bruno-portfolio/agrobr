# Contratos de Dados

O agrobr garante estabilidade de schema. Seu pipeline não vai quebrar.

Cada contrato é definido em Python (`agrobr/contracts/`) e exportado como JSON (`agrobr/schemas/`).
Validação é automática: todo `fetch()` de dataset valida o DataFrame contra o contrato registrado.

## Garantias Globais

| Garantia | Descrição |
|----------|-----------|
| **Nomes estáveis** | Colunas nunca mudam de nome (só adicionam) |
| **Colunas estáveis presentes** | Toda coluna `stable` existe no DataFrame; `nullable` permite valores nulos, não ausência da coluna |
| **Tipos só alargam** | int→float ok, float→int nunca |
| **Datas ISO-8601** | Sempre YYYY-MM-DD |
| **Unidades explícitas** | Coluna dedicada |
| **Breaking = Major** | Quebras só em versão major |
| **Primary keys** | Quando há chave definida, suas colunas existem e a combinação é única |
| **Min/max constraints** | Valores numéricos validados contra limites |
| **Argumento desconhecido** | Todo dataset recusa argumento fora da assinatura com `TypeError`, antes da rede |
| **Tipos no polars** | Com `as_polars=True`, cada coluna sai com o tipo do contrato (`int` → `Int64`, `float` → `Float64`, `str` → `String`, `bool` → `Boolean`), mesmo toda nula; a coluna de data toda nula sai `Datetime("ns")` |

## Datasets

> A tabela lista os contratos **documentados** — não é idêntica a `datasets.list_datasets()`. `bcb_focus`, `bcb_ptax` e `bcb_ptax_moedas` são contratos de fonte reutilizados por `expectativas_mercado`, `cotacoes_cambio` e `moedas_cambio`; os nomes dos datasets não acrescentam aliases de contrato. A página PTAX reúne os contratos de cotações e catálogo.

Os quatro nomes de datasets Agrofit reutilizam os contratos `agrofit_*` existentes via `_contract_name`; não registram aliases de contrato. São 54 datasets e 89 contratos registrados. `bcb_credito_rural_total` é o contrato de fonte da função `bcb.credito_rural_total`, sem dataset, e `bcb_credito_rural_registro`, o do `agregacao="registro"` do `bcb.credito_rural` e do dataset `credito_rural`. `autorizacoes_defensivos` preserva linhas repetidas publicadas e não possui chave primária artificial.

`series_economicas` reutiliza `bcb_sgs` 3.0 sem alias de contrato. A seleção é por código ou alias SGS, com unidade e frequência dependentes da série. O dataset conserva a proveniência da consulta e não reconstitui revisões históricas.

Os dois [datasets de cultivares](../api/cultivares.md) reutilizam os novos contratos de fonte `rnc_registradas` e `rnc_protegidas` 1.0. As chaves são o número do registro RNC e o processo SNPC; certificados compartilhados por processos distintos são preservados. A consulta usa o cadastro corrente, com cache de aquisição de 24 horas e sem reconstrução histórica.

[`uso_do_solo`](uso_do_solo.md) valida a cobertura municipal das coleções 10 e 11 com `mapbiomas_cobertura_municipal` 1.1. As onze colunas incluem `geocodigo` e `id_registro` da publicação e o `cod_municipio` tirado do `geocodigo`; registros distintos com a mesma classificação territorial são preservados. A cobertura e as transições estaduais ficam nos seus contratos 2.0.

`empregadores_lista_suja` também reutiliza contrato de fonte existente, `lista_suja_empregadores` 2.0, sem alias. Sua chave de ID vale dentro do hash de uma publicação; documentos repetidos são preservados.

| Dataset | Descrição | Fontes |
|---------|-----------|--------|
| [unidades_conservacao](./unidades_conservacao.md) | Unidades de conservação federais, estaduais e municipais, com RPPNs, do CNUC | CNUC/MMA |
| [unidades_conservacao_federais](./unidades_conservacao_federais.md) | Cadastro corrente de unidades de conservação federais da camada ICMBio/INDE | ICMBio |
| [precos_diesel](./precos_diesel.md) | Preços semanais de diesel da ANP e médias mensais derivadas | ANP |
| [moedas_cambio](./moedas_cambio.md) | Catálogo corrente de moedas do serviço PTAX/BCB | BCB |
| [expectativas_mercado](./expectativas_mercado.md) | Expectativas anuais e mensais de mercado do Focus/BCB | BCB |
| [cotacoes_cambio](./cotacoes_cambio.md) | Cotações e paridades cambiais dos boletins PTAX/BCB | BCB |
| [autorizacoes_defensivos](./autorizacoes_defensivos.md) | Autorizações de uso com multiplicidade publicada — `agrofit_autorizacoes` v1.1 | Agrofit/MAPA |
| [composicao_defensivos](./composicao_defensivos.md) | Componentes e concentrações por família e registro — `agrofit_composicao` v1.0 | Agrofit/MAPA |
| [defensivos_formulados](./defensivos_formulados.md) | Produtos formulados por registro — `agrofit_formulados` v1.1 | Agrofit/MAPA |
| [defensivos_tecnicos](./defensivos_tecnicos.md) | Produtos técnicos por registro — `agrofit_tecnicos` v1.1 | Agrofit/MAPA |
| [preco_diario](./preco_diario.md) | Preços diários spot | CEPEA → cache |
| [producao_anual](./producao_anual.md) | Produção anual consolidada | IBGE PAM → CONAB |
| [empregadores_lista_suja](./empregadores_lista_suja.md) | Cadastro corrente de empregadores do MTE — `lista_suja_empregadores` v2.0 | MTE / Lista Suja |
| [estimativa_safra](./estimativa_safra.md) | Estimativas v3.1 por levantamento CONAB ou mês LSPA | CONAB → IBGE LSPA; seleção explícita |
| [balanco](./balanco.md) | Oferta/demanda | CONAB |
| [credito_rural](./credito_rural.md) | Crédito rural por cultura | BCB/SICOR → BigQuery |
| [cultivares_registradas](./cultivares_registradas.md) | Cadastro RNC com registro textual — `rnc_registradas` v1.0 | CultivarWeb/MAPA |
| [cultivares_protegidas](./cultivares_protegidas.md) | Cadastro SNPC por processo, preservando término textual — `rnc_protegidas` v1.0 | CultivarWeb/MAPA |
| [bcb_sgs](./bcb_sgs.md) | Séries temporais por código, referências e histórico por blocos | BCB SGS |
| [bcb_focus](./bcb_focus.md) | Expectativas anuais/mensais, detalhe, base e cobertura | BCB Focus |
| [bcb_ptax / bcb_ptax_moedas](./bcb_ptax.md) | Cotações por moeda/boletim e catálogo OData corrente | BCB PTAX |
| [bcb_credito_rural_total](./bcb_credito_rural_total.md) | Crédito rural por UF e finalidade, sem produto, com a industrialização | BCB/SICOR (`RegiaoUF`) |
| [bcb_credito_rural_registro](./bcb_credito_rural_registro.md) | Crédito rural registro a registro (`agregacao="registro"`), com mês, fonte de recursos, modalidade e atividade | BCB/SICOR (`*RegiaoUFProduto`) |
| [exportacao](./exportacao.md) | Exportações agrícolas | ComexStat → ABIOVE |
| [fertilizante](./fertilizante.md) | Entregas de fertilizantes | ANDA |
| [importacao](./importacao.md) | Importações agrícolas | ComexStat |
| [custo_producao](./custo_producao.md) | Custos de produção | CONAB |
| [custo_sociobiodiversidade](./custo_sociobiodiversidade.md) | Custos da sociobiodiversidade nas unidades publicadas | CONAB |
| [pecuaria_municipal](./pecuaria_municipal.md) | Rebanhos e produção animal | IBGE PPM |
| [abate_trimestral](./abate_trimestral.md) | Abate de bovinos, suínos e frangos | IBGE Abate |
| [censo_agropecuario](./censo_agropecuario.md) | Censo Agropecuário 1995/2006/2017 (11 temas) | IBGE Censo Agro |
| [censo_agropecuario_legado](./censo_agropecuario_legado.md) | Censo Agropecuário 1995/96 — 6 temas legados (FTP) | IBGE FTP |
| [censo_agropecuario_historico](./censo_agropecuario_historico.md) | Série histórica Censo Agropecuário 1920-2006 (9 temas, até UF) | IBGE SIDRA |
| [censo_agropecuario_municipal_1985](./censo_agropecuario_municipal_1985.md) | Censo 1985 — 53 tabelas municipais, casa a casa, com o status de cada casa | IBGE PDFs |
| [cadastro_rural](./cadastro_rural.md) | Cadastro Ambiental Rural | SICAR |
| [clima](./clima.md) | Clima mensal por UF; diário/horário por estação | INMET API → INMET ZIP → NASA POWER (UF) |
| [comercio_internacional](./comercio_internacional.md) | Comércio internacional bilateral (HS codes) | UN Comtrade |
| [condicao_lavouras](./condicao_lavouras.md) | Condição das lavouras paranaenses | SEAB/DERAL |
| [desmatamento](./desmatamento.md) | Desmatamento PRODES e alertas DETER por bioma | INPE |
| [embarques_anec](./embarques_anec.md) | Embarques semanais por porto e produto | ANEC |
| [embarques_mensais_anec](./embarques_mensais_anec.md) | Volumes mensais, estimativas e faixas por edição | ANEC |
| [comparacao_anual_anec](./comparacao_anual_anec.md) | Comparação mensal entre anos por edição | ANEC |
| [destinos_anec](./destinos_anec.md) | Participação dos destinos no período acumulado | ANEC |
| [silvicultura](./silvicultura.md) | Producao silvicultural (IBGE PEVS) | IBGE PEVS |
| [extrativismo_vegetal](./extrativismo_vegetal.md) | Producao extrativista vegetal (IBGE PEVS) | IBGE PEVS |
| [leite_industrial](./leite_industrial.md) | Leite trimestral (aquisicao/industrializacao) | IBGE Leite |
| [lspa](./lspa.md) | Estimativas mensais de produção agrícola | IBGE LSPA |
| [oferta_demanda_global](./oferta_demanda_global.md) | Oferta/demanda global (USDA PSD) | USDA |
| [pib_agro](./pib_agro.md) | PIB agropecuário por setor e trimestre | IBGE SIDRA |
| [preco_atacado](./preco_atacado.md) | Preços de atacado em CEASAs | CONAB CEASA/PROHORT |
| [progresso_safra](./progresso_safra.md) | Progresso semanal semeadura/colheita | CONAB |
| [queimadas](./queimadas.md) | Focos de calor por satélite | INPE |
| [futuros_agricolas](./futuros_agricolas.md) | Futuros agrícolas B3 (ajustes, histórico, posições) | B3 |
| [posicionamento_fundos](./posicionamento_fundos.md) | Posicionamento de fundos por categoria de trader (COT) | CFTC |
| [movimentacao_portuaria](./movimentacao_portuaria.md) | Movimentação portuária de cargas ⚠️ (fonte fora do ar) | ANTAQ |
| [seguro_rural](./seguro_rural.md) | Seguro rural — apólices e sinistros | MAPA PSR |
| [serie_historica_safra](./serie_historica_safra.md) | Série histórica de safras (45 produtos) | CONAB |
| [series_economicas](./series_economicas.md) | Séries por código ou alias SGS, intervalo e últimas observações | BCB SGS |
| [uso_do_solo](./uso_do_solo.md) | Cobertura e uso da terra (MapBiomas) | MapBiomas |
| [zoneamento_agricola](./zoneamento_agricola.md) | Zoneamento agrícola de risco climático (ZARC) | MAPA/Embrapa |

## Schemas JSON

Cada contrato gera automaticamente um arquivo JSON em `agrobr/schemas/`:

```python
from agrobr.contracts import get_contract, list_contracts, generate_json_schemas

# Listar contratos registrados
list_contracts()

# Acessar contrato
contract = get_contract("preco_diario")
print(contract.primary_key)   # ['data', 'produto']
print(contract.to_json())     # Schema JSON completo

# Validação (automática em todo fetch, ou manual)
from agrobr.contracts import validate_dataset
validate_dataset(df, "preco_diario")  # raises ContractViolationError

# Gerar todos os JSONs
generate_json_schemas("agrobr/schemas/")
```

## Uso

```python
from agrobr import datasets

# Listar datasets
print(datasets.list_datasets())
# 54 datasets

# Listar produtos de um dataset
datasets.list_products("preco_diario")
# ['soja', 'milho', 'boi', 'bezerro', 'cafe', 'cafe_robusta', 'trigo', 'algodao']

# Info de um dataset
datasets.info("preco_diario")
# {'name': 'preco_diario', 'sources': ['cepea', 'cache'], ...}

# Ficha em texto de um dataset e de todos
print(datasets.describe("preco_diario"))
print(datasets.describe_all())

# O objeto do dataset (uma cópia): info e fetch com os argumentos de datasets.preco_diario
ds = datasets.get_dataset("preco_diario")
df = await ds.fetch("soja", inicio="2024-01-01")
```

Nome desconhecido em `get_dataset`, `info`, `list_products` ou `describe` levanta `UnknownNameError`, que herda de
`InvalidParameterError` e de `KeyError`, com os nomes válidos na mensagem.

## Fallback Automático

O fallback automático aplica-se aos datasets com fontes alternativas configuradas, respeitando a seleção e a cobertura da consulta. Datasets com fonte única, como os quatro Agrofit, não possuem fallback para outra instituição:

```
preco_diario: CEPEA → cache local
producao_anual: IBGE PAM → CONAB
estimativa_safra: CONAB → IBGE LSPA
balanco: CONAB
credito_rural: BCB/SICOR → BigQuery (basedosdados)
exportacao: ComexStat → ABIOVE
fertilizante: ANDA
custo_producao: CONAB
custo_sociobiodiversidade: CONAB
clima: INMET API → INMET ZIP → NASA POWER (somente UF)
futuros_agricolas: B3
```

## MetaInfo

Toda chamada com `return_meta=True` retorna metadados de proveniência:

```python
df, meta = await datasets.preco_diario("soja", return_meta=True)

print(meta.source)            # Fonte usada
print(meta.dataset)           # Nome do dataset
print(meta.contract_version)  # Versão do contrato
print(meta.records_count)     # Registros retornados
print(meta.from_cache)        # Se veio do cache
print(meta.attempted_sources) # Fontes tentadas, incluindo cascatas internas
print(meta.selected_source)   # Fonte real que forneceu os dados
print(meta.snapshot)          # Data de corte (modo determinístico)
print(meta.license)           # Classificação de licença do dado (docs/licenses.md)
```

Se um adaptador aciona uma cascata interna, `attempted_sources`,
`selected_source` e `from_cache` preservam essa proveniência. Uma fonte simples
mantém o nome do adaptador do dataset.

`license` é a classificação de licença do dado (`livre`, `nc`, `zona_cinza` ou `restrito`, da
[tabela de licenças](../licenses.md)): a mais restritiva entre as fontes de `data_sources` (o CEPEA com
o fallback da Notícias Agrícolas sai `restrito`) ou, sem elas, a da fonte selecionada. Vem da tabela
`agrobr.constants.LICENCAS`, a mesma da documentação; fonte fora da tabela dá `None`.

**Proveniência física.**

- `source_url` é o recurso que deu o dado, como a consulta com os filtros ou o
  arquivo baixado. Token e credencial saem como `[REDACTED]`. A página
  institucional, quando ajuda, fica em `source_details`.
- Com um corpo só, `raw_content_hash` é o SHA-256 completo dele e
  `raw_content_size` é o tamanho em bytes.
- `fetch_timestamp` é o horário UTC da aquisição do corpo que o topo descreve,
  e o dataset repassa o da fonte:
  - corpo recebido agora: a hora dessa aquisição;
  - corpo lido do cache (por exemplo, o ZIP do INMET, o Acervo Fundiário, a
    ANEC, o RNC, o ZARC e o Agrofit): a hora da aquisição original, igual ao
    `fetched_at`;
  - vários corpos: a aquisição mais recente entre eles, igual ao `fetched_at`;
  - registros do cache DuckDB do CEPEA: nulo (ver abaixo).
- Com vários corpos (páginas, anos, períodos ou consultas), `raw_content_hash`
  fica nulo e `raw_content_size` fica zero. O INMET (`resources`), o PSR
  (`corpos`), o IBGE (`consultas`, na SIDRA) e as séries da CONAB
  (`publicacao.series`) listam cada corpo em `source_details`, com URL e SHA-256.
- Um corpo auxiliar que entra no dado fica em `source_details`, com URL, SHA-256
  e bytes, e o topo descreve o corpo dos registros. No IMEA, o catálogo que dá o
  nome dos indicadores sai em `indicadores_url`, `indicadores_sha256` e
  `indicadores_bytes`.
- O dado que o CEPEA lê do cache DuckDB (`source` igual a `cache` ou
  `cache_fallback`) sai com `raw_content_hash` nulo, `raw_content_size` zero e
  `fetch_timestamp` nulo, porque o cache guarda registros, não o corpo. O
  `fetched_at` é o da coleta original.
- Quando `source_details` traz `hash_kind` ou `raw_content_hash_kind` igual a
  `resource_manifest_sha256` (BCB, Comtrade, Embrapa Solos, FUNAI, PRODES/DETER,
  INCRA e SICAR), `raw_content_hash` é o SHA-256 do manifesto das consultas, não
  de um corpo HTTP, e `raw_content_size` é o tamanho desse manifesto. O manifesto
  leva a hora de cada recurso (`fetched_at`) nessas fontes, menos no SICAR, e
  também na ANP com mais de um arquivo (`raw_hash_kind`): o hash identifica a
  aquisição e muda a cada chamada, mesmo com o conteúdo igual. Para saber se o conteúdo
  mudou, compare o `sha256` de cada item de `source_details["resources"]`.
- Na ANTT (`fluxo_pedagio`) e nos custos da CONAB (`custo_producao`), o `hash_kind` é
  próprio (`sha256_canonical_utf8_query_and_acquisition_manifest` e
  `sha256_canonical_utf8_query_acquisition_selection_manifest`), com a mesma regra: hash e
  tamanho são os do manifesto. O total recebido, com tentativas e falhas, fica em
  `source_details["received_bytes"]`, e o dos arquivos que entraram no dado (CSVs na ANTT e
  planilha na CONAB), em `source_details["data_file_bytes"]`.
- Na NASA POWER, o hash é o da lista de recibos em
  `source_details["http_receipts"]`, e o tamanho soma os corpos HTTP.
