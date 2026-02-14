# CONAB Progresso de Safra

## Visao Geral

| Campo | Valor |
|-------|-------|
| **Provedor** | CONAB — Companhia Nacional de Abastecimento |
| **Dados** | % semeadura e colheita semanal por cultura e UF |
| **Acesso** | XLSX via portal gov.br (Plone CMS) |
| **Formato** | XLSX (openpyxl) |
| **Autenticacao** | Nenhuma |
| **Licenca** | Dados publicos governo federal (livre) |
| **Frequencia** | Semanal |

## Origem dos Dados

A CONAB publica semanalmente o "Progresso de Safra" com informacoes sobre os percentuais de plantio e colheita das principais culturas anuais do Brasil. Os dados sao coletados pelos escritorios regionais da companhia e consolidados nacionalmente.

O agrobr acessa os XLSX publicados na pagina de Progresso de Safra do portal gov.br/conab. Cada semana possui um arquivo XLSX com dados de semeadura e colheita por cultura e estado.

## Culturas Monitoradas

| Cultura | Periodo | Estados |
|---------|---------|---------|
| Soja | Safra verao (out-mar) | 12 estados |
| Milho 1a | Safra verao (set-mar) | 9 estados |
| Milho 2a | Safrinha (jan-jul) | 9 estados |
| Arroz | Safra verao (out-abr) | 6 estados |
| Feijao 1a | Safra verao (set-mar) | 8 estados |
| Algodao | Safra verao (nov-mar) | 7 estados |
| Trigo | Safra inverno (abr-nov) | Variavel |

## Estrutura dos Dados

O XLSX semanal contem uma sheet "Progresso de safra" com blocos repetidos por cultura:

1. **Header da cultura**: "Soja - Safra 2025/26"
2. **Nota de cobertura**: "(Esses N estados correspondem a X% da area)"
3. **Semeadura**: tabela com Estado, ano anterior, semana anterior, semana atual, media 5 anos
4. **Colheita**: mesma estrutura (quando aplicavel)

Valores sao fracoes (0.0-1.0), nao percentuais.

## Fluxo de Acesso

1. Listing page no gov.br (paginacao Plone `?b_start:int=N`)
2. Cada semana tem sub-link "Plantio e Colheita" que retorna XLSX direto
3. HEAD retorna 403 (peculiaridade Plone), GET retorna 200

## Limitacoes

- Apenas culturas anuais monitoradas pela CONAB (6-7 culturas)
- Numero de estados varia por cultura (apenas os mais representativos)
- Dados sao publicados apenas durante o periodo da safra (nao ha dados no entre-safra)
- URL dos XLSX nao e previsivel — precisa crawl da listing page
- Trigo so aparece durante a safra de inverno

## Cache e Atualizacao

- **TTL**: 12 horas (publicacao semanal, tipicamente sexta-feira)
- Recomendado: usar `semanas_disponiveis()` para listar datas e buscar especifica

## Links

- [Progresso de Safra](https://www.gov.br/conab/pt-br/atuacao/informacoes-agropecuarias/safras/progresso-de-safra)
- [CONAB](https://www.gov.br/conab/pt-br)
