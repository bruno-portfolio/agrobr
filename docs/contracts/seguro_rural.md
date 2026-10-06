# seguro_rural v2.0

Seguro rural — apólices e sinistros do PSR (MAPA).

## Fontes

| Prioridade | Fonte | Descrição |
|------------|-------|-----------|
| 1 | MAPA PSR | Programa de Subvenção ao Prêmio do Seguro Rural |

## Produtos

100+ culturas dinâmicas do PSR (validação delegada à source).

## Tipo de dados

O dataset suporta dois tipos de consulta via parâmetro `tipo`:

- `tipo="apolices"` (default) — todas as apólices com subvenção federal
- `tipo="sinistros"` — indenizações positivas informadas, com evento preenchido

Cada tipo tem seu próprio contrato (`mapa_psr_apolices` 2.0 e `mapa_psr_sinistros` 1.1). `evento` só filtra `tipo="sinistros"`: com `tipo="apolices"`, gera `InvalidParameterError` antes da rede.

O contrato atual de apólices é `MAPA_PSR_APOLICES_V2`, vigente desde o agrobr 2.0.0.

## Schema — Apólices

| Coluna | Tipo | Nullable | Unidade | Estável |
|--------|------|----------|---------|---------|
| `nr_apolice` | str | ❌ | - | Sim |
| `ano_apolice` | int | ❌ | - | Sim |
| `uf` | str | ❌ | - | Sim |
| `municipio` | str | ✅ | - | Sim |
| `cd_ibge` | str | ✅ | - | Sim |
| `cod_municipio` | int | ✅ | - | Não |
| `cultura` | str | ❌ | - | Sim |
| `classificacao` | str | ✅ | - | Sim |
| `area_total` | float | ✅ | ha | Sim |
| `valor_premio` | float | ✅ | BRL | Sim |
| `valor_subvencao` | float | ✅ | BRL | Sim |
| `valor_limite_garantia` | float | ✅ | BRL | Sim |
| `valor_indenizacao` | float | ✅ | BRL | Sim |
| `evento` | str | ✅ | - | Sim |
| `produtividade_estimada` | float | ✅ | não publicada | Sim |
| `produtividade_segurada` | float | ✅ | não publicada | Sim |
| `nivel_cobertura` | float | ✅ | - | Sim |
| `taxa` | float | ✅ | - | Sim |
| `seguradora` | str | ❌ | - | Sim |

`produtividade_estimada` e `produtividade_segurada` não têm unidade canônica: o MAPA publica os 2 números sem unidade. A
razão entre eles é o `nivel_cobertura`.

## Schema — Sinistros

| Coluna | Tipo | Nullable | Unidade | Estável |
|--------|------|----------|---------|---------|
| `nr_apolice` | str | ❌ | - | Sim |
| `ano_apolice` | int | ❌ | - | Sim |
| `uf` | str | ❌ | - | Sim |
| `municipio` | str | ✅ | - | Sim |
| `cd_ibge` | str | ✅ | - | Sim |
| `cod_municipio` | int | ✅ | - | Não |
| `cultura` | str | ❌ | - | Sim |
| `classificacao` | str | ✅ | - | Sim |
| `evento` | str | ❌ | - | Sim |
| `area_total` | float | ✅ | ha | Sim |
| `valor_indenizacao` | float | ❌ | BRL | Sim |
| `valor_premio` | float | ✅ | BRL | Sim |
| `valor_subvencao` | float | ✅ | BRL | Sim |
| `valor_limite_garantia` | float | ✅ | BRL | Sim |
| `produtividade_estimada` | float | ✅ | não publicada | Sim |
| `produtividade_segurada` | float | ✅ | não publicada | Sim |
| `nivel_cobertura` | float | ✅ | - | Sim |
| `seguradora` | str | ✅ | - | Sim |

## Garantias

- `uf` é código UF válido
- Valores monetários em BRL
- Dados desde 2006 (início do PSR)
- Sinistros: `valor_indenizacao` sempre > 0, `evento` sempre preenchido
- Apólices: `valor_indenizacao` pode ser null/0

## Exemplo

```python
from agrobr import datasets

# Apólices (default)
df = await datasets.seguro_rural()
df = await datasets.seguro_rural("soja", uf="MT", ano=2023)

# Município inteiro, pelo código IBGE ou pelo nome inteiro
df = await datasets.seguro_rural(municipio=4305108, ano=2024)
df = await datasets.seguro_rural(municipio="Caxias do Sul", ano=2024)

# Sinistros
df = await datasets.seguro_rural(tipo="sinistros")
df = await datasets.seguro_rural(tipo="sinistros", evento="SECA")

# Com metadados
df, meta = await datasets.seguro_rural(return_meta=True)

# Sync
from agrobr.sync import datasets
df = datasets.seguro_rural()
```

## Schema JSON

```python
from agrobr.contracts import get_contract
# Apólices
contract = get_contract("mapa_psr_apolices")
# Sinistros
contract = get_contract("mapa_psr_sinistros")
```

## Integridade e período das apólices

O CSV inteiro é validado antes da aplicação dos filtros. Cabeçalho duplicado, registro com campos a mais ou a menos e ano de apólice inválido geram `ParseError` com a posição do registro; a leitura não descarta essas linhas silenciosamente. Campos entre aspas podem conter separadores e quebras de linha. O parser é versão 4; o contrato de apólices está em 2.0 e o de sinistros em 1.1. `ano_apolice` sai em `Int64`, com linhas e vazio.

**Chave e registro publicado em dobro (contrato `mapa_psr_apolices` 2.0).** A chave das apólices é `nr_apolice`, `ano_apolice`, `uf`, `cultura`, `cd_ibge` e `seguradora`: o número da apólice só é único dentro da seguradora (em 2007, 2008, 2009, 2011 e 2012 o MAPA publica o mesmo número em duas seguradoras, com área e prêmio diferentes). Um registro publicado duas vezes e igual em todas as colunas que o agrobr entrega (em 2009, a apólice 1977000249501 da Mapfre, com a proposta reenviada) sai uma vez só, com aviso (`warn_once`) e a contagem em `source_details["duplicatas_colapsadas"]`. Repetição da chave com qualquer valor diferente levanta `ContractViolationError` (no dataset, `SourceUnavailableError`).

**Município e código IBGE.** `municipio=` aceita o código IBGE de 7 dígitos ou o nome inteiro do município (sem caixa e acento; pedaço de nome gera `InvalidParameterError` com os candidatos) e filtra pelo código publicado. O MAPA rotula parte das apólices com o nome do distrito: em Caxias do Sul (4305108), em 2024, 274 das 693 apólices do código trazem o nome do município, e o filtro devolve as 693. As apólices publicadas com "-" no lugar do geocódigo (`cd_ibge` nulo) entram quando o rótulo é o nome inteiro do município, na mesma UF. Detalhes na [fonte MAPA PSR](../sources/mapa_psr.md#municipio-e-codigo-ibge).

`ano_apolice` é o ano de contratação da apólice, conforme o dicionário SISSER; não identifica a data do evento ou do pagamento. `sinistros` seleciona indenização positiva com evento não vazio. Zero publicado continua zero em `apolices`; valores ausentes continuam nulos. Não se arredondam valores monetários a centavos. Números de apólice e códigos geográficos conservam seus zeros iniciais. Fora o registro publicado em dobro e idêntico, descrito acima, nenhuma linha é deduplicada.

Na captura de 18/09/2026, o catálogo disponibilizava três CSVs, até 2025. O arquivo 2025 tinha indenizações ausentes, o que não demonstra ausência de sinistros. O EOF comprova a leitura completa do arquivo publicado, sem garantir cobertura completa do programa ou atualização dos pagamentos.

Campos textuais preservam literais como `NULL`, `NA`, `None` e `N/A`, sujeitos apenas às normalizações de espaços e caixa já documentadas; eles não são convertidos em ausência pelo leitor CSV. Campo textual vazio continua vazio, exceto `cd_ibge`, que sai nulo. Campos numéricos mantêm a conversão vigente: valores ausentes ou não interpretáveis ficam nulos, sem transformar tokens textuais em zero.
