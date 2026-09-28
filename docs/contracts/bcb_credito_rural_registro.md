# bcb.credito_rural_registro v1.0

Crédito rural registro a registro do SICOR, sem agregar: a saída de `bcb.credito_rural(..., agregacao="registro")` e de `datasets.credito_rural(..., agregacao="registro")`. É o recorte que a chamada padrão da 1.1.0 devolvia (`agregacao="municipio"`, que não agregava), com os nomes pelas tabelas oficiais do BCB.

## Fonte

| Fonte | Entidade | Descrição |
|---|---|---|
| BCB/SICOR (OData v2, Olinda) | `CusteioRegiaoUFProduto`, `InvestRegiaoUFProduto` e `ComercRegiaoUFProduto` | Quantidade, valor e, no custeio, área por mês, UF, produto, programa, subprograma, fonte de recursos, tipo de seguro, atividade e modalidade |
| BCB/SICOR (tabelas de domínio) | `Programa.csv`, `FonteRecursos.csv`, `TipoGarantiaEmpreendimento.csv`, `Modalidade.csv` e `Atividade.csv` | Nomes dos códigos |

Sem fallback: a tabela da Base dos Dados agrega por município e não traz programa, subprograma, fonte de recursos, tipo de seguro, modalidade nem atividade. Com o OData fora, a chamada levanta `SourceUnavailableError`, com o motivo na mensagem.

## Schema

| Coluna | Tipo | Nullable | Unidade | Estável |
|---|---|---|---|---|
| `safra` | str | Não | — | Sim |
| `ano_emissao` | int | Não | — | Sim |
| `mes_emissao` | int | Não | — | Sim |
| `produto` | str | Não | — | Sim |
| `regiao` | str | Sim | — | Sim |
| `uf` | str | Não | — | Sim |
| `finalidade` | str | Não | — | Sim |
| `agregacao` | str | Não | — | Sim |
| `programa` | str | Sim | — | Sim |
| `cd_programa` | str | Sim | — | Sim |
| `cd_sub_programa` | str | Sim | — | Sim |
| `fonte_recurso` | str | Sim | — | Sim |
| `cd_fonte_recurso` | str | Sim | — | Sim |
| `tipo_seguro` | str | Sim | — | Sim |
| `cd_tipo_seguro` | str | Sim | — | Sim |
| `modalidade` | str | Sim | — | Sim |
| `cd_modalidade` | str | Sim | — | Sim |
| `atividade` | str | Sim | — | Sim |
| `cd_atividade` | str | Sim | — | Sim |
| `qtd_contratos` | int | Sim | — | Sim |
| `valor` | float | Sim | BRL | Sim |
| `area_financiada` | float | Sim | ha | Sim |
| `fonte` | str | Não | — | Sim |

**Chave primária:** `[ano_emissao, mes_emissao, uf, produto, finalidade, cd_programa, cd_sub_programa, cd_fonte_recurso, cd_tipo_seguro, cd_atividade, cd_modalidade]`

**Restrições:** `ano_emissao >= 2013`, `1 <= mes_emissao <= 12`, `qtd_contratos >= 0`, `valor >= 0`, `area_financiada >= 0`

- **A chave é o grão das entidades.** O `$select` do agrobr é a entidade inteira, com todos os campos do `$metadata` do OData. As 11 dimensões não se repetem em 3.462 registros: 12 consultas da safra 2024/25 por produto e UF, e 2 meses de 2024 sem filtro de UF (custeio da soja em outubro, com 18 UFs, e investimento em bovinos em março, com 25). `regiao` depende da UF, e `safra`, do mês; por isso ficam fora da chave.
- **Nomes pelas tabelas de domínio do BCB:**
  - `programa`: o trecho da descrição oficial antes do primeiro " - ", como na `agregacao="programa"`;
  - `tipo_seguro`: a descrição oficial;
  - `fonte_recurso`, `modalidade` e `atividade`: a descrição oficial inteira. Na fonte de recursos, o trecho antes do " - " juntaria códigos distintos: 4 fontes virariam "POUPANÇA RURAL", e 3, "LETRA DE CRÉDITO DO AGRONEGÓCIO (LCA)";
  - código fora da tabela: `fonte_recurso`, `modalidade` e `atividade` ficam nulos, sem palpite; `programa` e `tipo_seguro` saem `Desconhecido (<código>)`, como nas outras agregações.
- **Códigos como a fonte publica.** `cd_modalidade` sai `"01"`, e a tabela oficial usa `"1"`: o nome é resolvido pelo número. A combinação finalidade × atividade × modalidade dos 2.508 registros das 11 consultas existe na tabela oficial.
- **Subprograma:** só o código, como na 1.1.0. O nome está na tabela de domínio `Subprograma` do BCB e depende do programa.
- **Área:** pelo OData, `area_financiada` sai nula. O custeio publica `AreaCusteio` vazio, e investimento e comercialização não têm área.
- **Soma:** somado por safra, UF, produto e finalidade, o registro é igual à `agregacao="uf"` da mesma chamada.
- **Safra em curso:** o mesmo aviso das outras agregações, em `MetaInfo.validation_warnings` e em `source_details["safra_em_curso"]`.
- **Tipos no pandas:** os do contrato vazio (`str` → `object`, `int` → `Int64`, `float` → `Float64`), inclusive no resultado vazio. Com `as_polars=True`: `Utf8`, `Int64` e `Float64`.
- **`MetaInfo`:** `schema_version` e `contract_version` são `1.0`, e `source_details["contract"]` é `bcb.credito_rural_registro`.

## Histórico de versões

| Versão | Mudança |
|---|---|
| v1.0 | Contrato inicial (2.0.0) |

## Exemplo

```python
from agrobr import bcb, datasets

df = await bcb.credito_rural("soja", safra="2024/25", uf="MT", agregacao="registro")
por_fonte = df.groupby(["mes_emissao", "fonte_recurso"])["valor"].sum()

pelo_dataset = await datasets.credito_rural("milho", safra="2024/25", agregacao="registro")
```

## Schema JSON

Disponível em `agrobr/schemas/bcb_credito_rural_registro.json`.

```python
from agrobr.contracts import get_contract

contract = get_contract("bcb_credito_rural_registro")
print(contract.to_json())
```
