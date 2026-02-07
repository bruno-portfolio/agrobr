# INMET — Meteorologia

> **Nota (fev/2026):** A API de dados INMET (apitempo.inmet.gov.br) esta retornando
> 404 em todos os endpoints de dados. Para dados climaticos, usar
> [NASA POWER](nasa_power.md) como alternativa (`from agrobr import nasa_power`).

Instituto Nacional de Meteorologia. Dados climáticos de 600+ estações.

## API

```python
from agrobr import inmet

# Listar estações automáticas
df = await inmet.estacoes(tipo="T", uf="MT")

# Dados horários de uma estação
df = await inmet.estacao("A001", inicio="2024-01-01", fim="2024-01-31")

# Dados diários
df = await inmet.estacao("A001", inicio="2024-01-01", fim="2024-01-31", agregacao="diario")

# Clima mensal agregado por UF (todas as estações)
df = await inmet.clima_uf("MT", ano=2024)
```

## Colunas — `clima_uf`

| Coluna | Tipo | Descrição |
|---|---|---|
| `mes` | int | Mês (1-12) |
| `uf` | str | UF da estação |
| `precip_acum_mm` | float | Precipitação acumulada (mm) |
| `temp_media` | float | Temperatura média (C) |
| `temp_max_media` | float | Temperatura máxima média (C) |
| `temp_min_media` | float | Temperatura mínima média (C) |
| `num_estacoes` | int | Estações usadas na agregação |

## MetaInfo

```python
df, meta = await inmet.clima_uf("MT", ano=2024, return_meta=True)
print(meta.source)  # "inmet"
```

## Notas tecnicas

- O client envia User-Agent de navegador (Chrome 120) em todas as
  requisicoes. A API INMET bloqueia com 403 requisicoes sem User-Agent.
- A API divide automaticamente periodos longos em chunks de 365 dias.
- Concorrencia limitada a 5 estacoes simultaneas por UF.

## Status (fev/2026)

A API de dados (`/estacao/dados/`) esta retornando 404 em todos os
endpoints de dados. A listagem de estacoes (`/estacoes/T`) funciona
normalmente. Problema externo, sem previsao de resolucao.

## Fonte

- API: `https://apitempo.inmet.gov.br`
- Atualizacao: diaria
- Historico: 2000+
