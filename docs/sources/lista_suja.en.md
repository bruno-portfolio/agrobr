# Lista Suja — Cadastro de Empregadores (Forced Labor)

## Overview

| Item | Detail |
|------|---------|
| Provider | Ministerio do Trabalho e Emprego |
| Data | Employers caught using forced labor |
| Access | PDF download |
| Format | PDF |
| Authentication | None |
| License | Free (Lei de Acesso a Informacao) |

## Usage Example

```python
import asyncio
from agrobr import lista_suja

async def main():
    # All employers
    df = await lista_suja.empregadores()

    # Filter by state
    df = await lista_suja.empregadores(uf="PA")

    # With metadata
    df, meta = await lista_suja.empregadores(return_meta=True)

    # Polars
    df = await lista_suja.empregadores(as_polars=True)

asyncio.run(main())
```

## Columns

| Column | Type | Description |
|--------|------|-------------|
| empregador | str | Employer name |
| cpf_cnpj | str | CPF or CNPJ |
| estabelecimento | str | Establishment name |
| uf | str | State |
| cnae | str | CNAE code |
| data_inclusao | datetime | Date added to the list |
| trabalhadores_resgatados | int | Number of rescued workers |
| ano_acao_fiscal | int | Year of the inspection action |

## Specifics

- **PII warning**: emits an automatic warning on the first call (CPF/CNPJ public under Lei de Acesso a Informacao)
- **Single file**: full download without pagination

## Limitations

- Contains personal data (CPF/CNPJ) — public by law
- Single file without pagination
