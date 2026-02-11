# INMET — Meteorologia

Instituto Nacional de Meteorologia. Dados climáticos de 600+ estações.

## Autenticação

A API de dados observacionais do INMET requer token de autenticação.
Configure a variável de ambiente `AGROBR_INMET_TOKEN` antes de usar:

```bash
export AGROBR_INMET_TOKEN="seu-token-aqui"
```

Sem o token, requisições de dados observacionais retornam HTTP 204 (sem conteúdo).
A listagem de estações (`/estacoes/T`, `/estacoes/M`) funciona sem token.

Para obter o token, consulte o portal INMET: https://portal.inmet.gov.br

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

- **Token obrigatório**: Dados observacionais exigem `AGROBR_INMET_TOKEN`.
  Sem token, o client loga warning e retorna lista vazia (HTTP 204).
- O client envia User-Agent de navegador (Chrome 120) em todas as
  requisicoes. A API INMET bloqueia com 403 requisicoes sem User-Agent.
- A API divide automaticamente periodos longos em chunks de 365 dias.
- Concorrencia limitada a 5 estacoes simultaneas por UF.
- Endpoint de dados: `/estacao/{codigo}/{inicio}/{fim}` (atualizado fev/2026).
- Para dados climaticos sem token, [NASA POWER](nasa_power.md) é uma
  alternativa funcional (`from agrobr import nasa_power`).

## Fonte

- API: `https://apitempo.inmet.gov.br`
- Atualizacao: diaria
- Historico: 2000+
