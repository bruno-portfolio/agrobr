# CONAB Progresso de Safra

## Visao Geral

| Campo | Valor |
|-------|-------|
| **Provedor** | CONAB — Companhia Nacional de Abastecimento |
| **Dados** | % semeadura e colheita semanal por cultura e UF |
| **Acesso** | XLSX via portal gov.br (Plone CMS) |
| **Formato** | XLSX (openpyxl, fallback calamine) |
| **Autenticação** | Nenhuma |
| **Licença** | CC BY-ND 3.0 (rodapé da ficha da planilha), classificação `livre`: reprodução comercial com atribuição à CONAB, sem distribuir adaptações protegidas. Veja [Licenças](../licenses.md) |
| **Frequência** | Semanal |

## Origem dos Dados

A CONAB publica semanalmente o "Progresso de Safra" com informações sobre os percentuais de plantio e colheita das principais culturas anuais do Brasil. Os dados são coletados pelos escritorios regionais da companhia e consolidados nacionalmente.

Os percentuais por UF são compilados pela CONAB a partir dos levantamentos estaduais, e a data da coluna é a semana da publicação
da CONAB. No Paraná, o valor repete o levantamento do DERAL da segunda-feira anterior (no boletim de 18/09/2026, o DERAL de
14/09, em 5 de 5 culturas); `deral.condicao_lavouras` traz a data do levantamento.

O agrobr acessa os XLSX publicados na página de Progresso de Safra do portal gov.br/conab. Cada semana possui um arquivo XLSX com dados de semeadura e colheita por cultura e estado.

## Culturas Monitoradas

| Cultura | Período | Estados |
|---------|---------|---------|
| Soja | Safra verao (out-mar) | 12 estados |
| Milho 1a | Safra verao (set-mar) | 9 estados |
| Milho 2a | Safrinha (jan-jul) | 9 estados |
| Arroz | Safra verao (out-abr) | 6 estados |
| Feijao 1a | Safra verao (set-mar) | 8 estados |
| Algodao | Safra verao (nov-mar) | 7 estados |
| Trigo | Safra inverno (abr-nov) | Variável |

## Estrutura dos Dados

O XLSX semanal contem uma sheet "Progresso de safra" com blocos repetidos por cultura:

1. **Header da cultura**: "Soja - Safra 2025/26"
2. **Nota de cobertura**: "(Esses N estados correspondem a X% da área cultivada)", lida em `n_estados` e `cobertura_area_pct`
3. **Semeadura**: tabela com Estado, ano anterior, semana anterior, semana atual, media 5 anos
4. **Colheita**: mesma estrutura (quando aplicavel); o percentual dos blocos marcados com `*` é calculado sobre o semeado acumulado
5. **Linha "N estados"** no fim de cada bloco: média da própria CONAB dos estados monitorados, publicada como `uf =
   "MEDIA_ESTADOS"`, não como Brasil

Valores são fracoes (0.0-1.0), não percentuais.

## Fluxo de Acesso

1. Listing page no gov.br (paginação Plone `?b_start:int=N`)
2. Cada semana tem sub-link "Plantio e Colheita" que retorna XLSX direto
3. HEAD retorna 403 (peculiaridade Plone), GET retorna 200

## Limitacoes

- Apenas culturas anuais monitoradas pela CONAB (6-7 culturas)
- Número de estados varia por cultura (apenas os mais representativos)
- Dados são publicados apenas durante o período da safra (não ha dados no entre-safra)
- URL dos XLSX não e previsivel — precisa crawl da listing page
- Trigo so aparece durante a safra de inverno

## Cache e Atualização

- Não há cache local: cada chamada baixa os dados da CONAB.
- A publicação é semanal, tipicamente às sextas-feiras.
- Recomenda-se usar `semanas_disponiveis()` para listar as datas e buscar uma semana específica.

## Links

- [Progresso de Safra](https://www.gov.br/conab/pt-br/atuacao/informacoes-agropecuarias/safras/progresso-de-safra)
- [CONAB](https://www.gov.br/conab/pt-br)
