# Contratos de Dados

O agrobr garante estabilidade de schema. Seu pipeline não vai quebrar.

Cada contrato é definido em Python (`agrobr/contracts/`) e exportado como JSON (`agrobr/schemas/`).
Validação é automática: todo `fetch()` de dataset valida o DataFrame contra o contrato registrado.

## Garantias Globais

| Garantia | Descrição |
|----------|-----------|
| **Nomes estáveis** | Colunas nunca mudam de nome (só adicionam) |
| **Tipos só alargam** | int→float ok, float→int nunca |
| **Datas ISO-8601** | Sempre YYYY-MM-DD |
| **Unidades explícitas** | Coluna dedicada |
| **Breaking = Major** | Quebras só em versão major |
| **Primary keys** | Cada dataset tem chave primária definida (sem duplicatas) |
| **Min/max constraints** | Valores numéricos validados contra limites |

## Datasets

| Dataset | Descrição | Fontes |
|---------|-----------|--------|
| [preco_diario](./preco_diario.md) | Preços diários spot | CEPEA → cache |
| [producao_anual](./producao_anual.md) | Produção anual consolidada | IBGE PAM → CONAB |
| [estimativa_safra](./estimativa_safra.md) | Estimativas safra corrente | CONAB → IBGE LSPA |
| [balanco](./balanco.md) | Oferta/demanda | CONAB |
| [credito_rural](./credito_rural.md) | Crédito rural por cultura | BCB/SICOR → BigQuery |
| [exportacao](./exportacao.md) | Exportações agrícolas | ComexStat → ABIOVE |
| [fertilizante](./fertilizante.md) | Entregas de fertilizantes | ANDA |
| [importacao](./importacao.md) | Importações agrícolas | ComexStat |
| [custo_producao](./custo_producao.md) | Custos de produção | CONAB |
| [pecuaria_municipal](./pecuaria_municipal.md) | Rebanhos e produção animal | IBGE PPM |
| [abate_trimestral](./abate_trimestral.md) | Abate de bovinos, suínos e frangos | IBGE Abate |
| [censo_agropecuario](./censo_agropecuario.md) | Censo Agropecuário 1995/2006/2017 (10 temas) | IBGE Censo Agro |
| [censo_agropecuario_legado](./censo_agropecuario_legado.md) | Censo Agropecuário 1995/96 — 6 temas legados (FTP) | IBGE FTP |
| [censo_agropecuario_historico](./censo_agropecuario_historico.md) | Série histórica Censo Agropecuário 1920-2006 (9 temas, até UF) | IBGE SIDRA |
| [censo_agropecuario_municipal_1985](./censo_agropecuario_municipal_1985.md) | Censo 1985 municipal — 53 temas via OCR de PDFs (22 UFs) | IBGE PDFs |
| [cadastro_rural](./cadastro_rural.md) | Cadastro Ambiental Rural | SICAR |
| [clima](./clima.md) | Dados climáticos mensais por UF ou por estação | INMET → NASA POWER |
| [comercio_internacional](./comercio_internacional.md) | Comércio internacional bilateral (HS codes) | UN Comtrade |
| [condicao_lavouras](./condicao_lavouras.md) | Condição das lavouras paranaenses | SEAB/DERAL |
| [desmatamento](./desmatamento.md) | Desmatamento PRODES e alertas DETER por bioma | INPE |
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
| [movimentacao_portuaria](./movimentacao_portuaria.md) | Movimentação portuária de cargas | ANTAQ |
| [seguro_rural](./seguro_rural.md) | Seguro rural — apólices e sinistros | MAPA PSR |
| [serie_historica_safra](./serie_historica_safra.md) | Série histórica de safras (32 culturas) | CONAB |
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
datasets.list_datasets()
# ['abate_trimestral', 'balanco', 'cadastro_rural', 'censo_agropecuario',
#  'censo_agropecuario_historico', 'censo_agropecuario_municipal_1985',
#  'credito_rural', 'custo_producao', 'estimativa_safra', 'exportacao',
#  'extrativismo_vegetal', 'fertilizante', 'importacao', 'leite_industrial',
#  'pecuaria_municipal', 'pib_agro', 'preco_diario', 'producao_anual',
#  'progresso_safra', 'silvicultura']

# Listar produtos de um dataset
datasets.list_products("preco_diario")
# ['soja', 'milho', 'boi', 'cafe', 'cafe_robusta', 'trigo', 'algodao']

# Info de um dataset
datasets.info("preco_diario")
# {'name': 'preco_diario', 'sources': ['cepea', 'cache'], ...}
```

## Fallback Automático

Cada dataset tem múltiplas fontes com prioridade. Se a fonte primária
falhar, o agrobr automaticamente tenta a próxima:

```
preco_diario: CEPEA → cache local
producao_anual: IBGE PAM → CONAB
estimativa_safra: CONAB → IBGE LSPA
balanco: CONAB
credito_rural: BCB/SICOR → BigQuery (basedosdados)
exportacao: ComexStat → ABIOVE
fertilizante: ANDA
custo_producao: CONAB
clima: INMET → NASA POWER
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
print(meta.snapshot)          # Data de corte (modo determinístico)
```
