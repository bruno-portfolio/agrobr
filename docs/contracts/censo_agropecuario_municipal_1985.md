# censo_agropecuario_municipal_1985 v2.0

Censo Agropecuário 1985, tabelas municipais 67 a 119 dos 28 volumes estaduais do IBGE (27 UFs; Minas Gerais em 2 volumes), com 53
temas, 1 por tabela. O agrobr extraiu os números dos PDFs publicados pelo IBGE e os traz num pacote local
(`agrobr/data/censo_1985/`): a consulta não acessa a rede.

**Cada linha é uma casa do PDF**: a do número e também as sem leitura, as sem coluna identificada e as fora da grade, cada uma com
o seu `status`. **`valor` só vem quando a casa foi confirmada pelas somas impressas** (município → microrregião → mesorregião →
UF); **`valor_lido` traz a leitura sempre**. Filtre por `status` para escolher o nível de confiança. São 2.275.606 casas, das quais
579.456 (25,5 %) confirmadas.

Na API 2.0, somente `tema` aceita posição; os demais filtros e flags são passados por nome. Retornos vazios preservam os dtypes do contrato: inteiros em `Int64`, medidas em `float64` e texto no padrão do pandas instalado.

## Fontes

| Prioridade | Fonte | Descrição |
|------------|-------|-----------|
| 1 | IBGE Censo Agropecuário 1985 | Pacote do agrobr extraído dos 28 PDFs do IBGE (`agrobr/data/censo_1985/`) |

## Status de cada casa e precisão medida

A precisão foi medida contra células do PDF de 34 tabelas de 11 UFs.

| `status` | Significado | `valor` | Precisão medida |
|---|---|---|---|
| `confirmado_soma_exata` | A soma dos filhos (≥ 2 com valor > 0) fecha exata com o pai impresso | preenchido | 2.427 certas, 0 errada |
| `confirmado_soma_arredondada_2a_compativel` | A soma fecha dentro do arredondamento, e a 2ª leitura independente (RapidOCR) é compatível | preenchido | 641 certas, 0 errada |
| `concorda_2a_leitura` | A camada de texto e a 2ª leitura dão o mesmo número, sem soma que confirme | nulo | 98,3 % (2.052 de 2.087) |
| `nao_verificado` | Número lido, sem confirmação | nulo | 83,0 % (572 de 689) |
| `pdf_inconsistente_candidato` | O próprio PDF não fecha a soma naquela casa (as outras colunas do grupo fecham) | nulo | 12 de 14 |
| `correcao_colada_nao_confirmavel` | Número colado sobre a página (corpo maior), fora das somas | nulo | 6 de 7 |
| `casa_repetida` | 2 leituras na mesma casa | nulo | — |
| `sem_leitura` | Casa da grade que a extração não leu | nulo | 50,2 % não têm número no PDF; as outras têm, quase sempre de 1 dígito |
| `coluna_incerta` | Número lido sem coluna identificada | nulo | — |
| `fora_de_coluna` | Número fora da grade da tabela | nulo | — |

- O erro que sobra em `concorda_2a_leitura` é o dígito ou o grupo de milhar da esquerda perdido igual nas 2 leituras (684 no lugar
  de 8.684).
- `sem_leitura` não é zero: ler a casa como zero erraria metade das vezes.

**MA, PI, CE e RN** usam a versão que o IBGE republicou em 03/09/2018, com camada de texto (OCR da Adobe, com erros próprios, como
"71 o" por 710). Medidos à parte: exatas 816 certas e 0 errada, arredondadas 132 e 0, `concorda_2a_leitura` 97,6 % (683 de 700).
Contra a cópia de 2008 só em imagem, a versão de hoje confirma 22,6 % casas a mais.

## O nome da coluna

- `coluna_nome_lido`: o texto do cabeçalho como foi lido, sempre que há texto, limpo só por 5 regras listadas (hífen de quebra de
  linha, `!` no lugar de `I`, o título que vaza no grupo).
- `coluna_nome`: só quando confirmado. O `coluna_nome_status` diz qual:
  - `canonico`: a folha lida na página é a mesma (sem acento, só letras e dígitos) que ≥ 3 volumes leem na mesma posição (tabela,
    lado da página, número de colunas, coluna), em ≥ 50 % das leituras. **É o nome impresso confirmado por ≥ 3 volumes, na grafia
    da camada de texto**, com a quebra de palavra e o hífen: por exemplo, "ARRENDATARIO | ESTABELE-1 CIMENTOS" na 70. São 972.311
    casas (42,7 %), em 46 tabelas;
  - `lido`: há texto, mas ele não se repete em 3 volumes;
  - `sem_nome`: a casa não tem coluna, ou a página não tem cabeçalho legível.
- Nas tabelas de produto (111 a 117), o produto muda de página em página: a folha confirmada dá `variavel` e `unidade`, e o produto
  fica só no `coluna_nome_lido` (`coluna_nome` nulo, status `lido`).
- `unidade`: só com a folha confirmada. Vem da marca (1) a (4) do cabeçalho e da nota do volume, que muda de sentido entre volumes
  (no AC, "(1) TONELADAS (2) MIL FRUTOS"; no ES, o contrário). Marca que não abre com unidade é nota ("(2) INCLUSIVE PÉS NOVOS")
  e não vira unidade. Sem marca, vem do rótulo ("ÁREA (HA)", "INFORMANTES"). A unidade que está só no grupo de cima sai nula.
- Precisão contra as colunas do PDF das tabelas 108 a 116 e 111 (AC, ES, GO, PE e PR): `coluna_nome` 21 certos e 0 com a
  variável de outra coluna; `coluna_nome_lido` 411 certos, 26 incompletos e 0 de outra coluna; `unidade` 111 certas e 0 errada.

## Schema

| Coluna | Tipo | Nullable | Descrição |
|--------|------|----------|-----------|
| `ano` | Int64 | ❌ | Sempre 1985 |
| `uf` | str | ❌ | Sigla da UF |
| `volume` | str | ❌ | Volume do IBGE (`n18_p1_mg` e `n18_p2_mg` para Minas Gerais) |
| `tabela` | Int64 | ❌ | Número da tabela (67 a 119), o mesmo tema em todos os volumes |
| `tema` | str | ❌ | Tema da tabela, pelo título impresso |
| `pagina_pdf` | Int64 | ❌ | Página do PDF (contada a partir de 1) |
| `pagina_impressa` | Int64 | ✅ | Número impresso no rodapé |
| `linha` | Int64 | ❌ | Linha da localidade no bloco de páginas |
| `coluna` | Int64 | ❌ | Coluna física na página (0 à esquerda); negativa quando a casa não tem coluna identificada |
| `nivel` | str | ❌ | `uf`, `mesorregiao`, `microrregiao` ou `municipio` |
| `localidade` | str | ❌ | Nome como lido, com o ruído da leitura: "SERTÓES OE SENADOR POMPEU" (CE), "!NHAP!" (AL) |
| `coluna_nome` | str | ✅ | Nome da coluna, só quando confirmado (ver acima) |
| `coluna_nome_lido` | str | ✅ | Texto do cabeçalho como foi lido |
| `coluna_nome_status` | str | ❌ | `canonico`, `lido` ou `sem_nome` |
| `variavel` | str | ✅ | A folha da coluna, só quando confirmada |
| `unidade` | str | ✅ | Unidade, só quando a folha é confirmada |
| `unidade_lida` | str | ✅ | Unidade pela marca da nota do volume ou pelo rótulo da coluna |
| `valor` | Int64 | ✅ | Número confirmado |
| `valor_lido` | Int64 | ✅ | Número lido |
| `marcador` | str | ✅ | "-", "...", "x" quando a casa traz um sinal |
| `status` | str | ❌ | Ver a tabela acima |
| `reparado` | bool | ❌ | O grupo de milhar da esquerda foi recuperado por releitura do recorte; `False` em todas as casas desta versão |

## Primary Key

`(volume, tabela, pagina_pdf, linha, coluna)`. Não há código de município: os municípios de 1985 não correspondem 1:1 aos códigos
atuais.

## Consultas recusadas

- Tema, UF ou nível inválidos: `InvalidParameterError`, antes de ler o pacote.
- A UF cujo volume não traz a tabela (o IBGE omite a tabela que não se aplica ao estado): `InvalidParameterError`, com as UFs que
  têm a tabela.
- A tabela que está no volume, mas de que a extração não leu nenhuma casa (AM 80, AP 80, RR 80 e RR 119, páginas com menos de 10
  números): `ParseError`, com as páginas e o motivo.
- O filtro de nível que não acha linha devolve um DataFrame vazio.

## Cobertura

53 temas em 27 UFs. O DF tem só 4 tabelas; nas outras UFs, falta a tabela que o volume omite. A tabela abaixo dá, por tema, as
UFs com casas no pacote.

| Tema | UFs | Faltantes |
|---|---:|---|
| `animais_pessoal_residente` | 27 | — |
| `assistencia_tecnica` | 26 | DF |
| `classe_atividade_economica` | 27 | — |
| `colheita_lav_temporaria` | 26 | DF |
| `combustiveis_energia` | 26 | DF |
| `compra_venda_aves_ovos` | 26 | DF |
| `condicao_legal_terras` | 26 | DF |
| `condicao_produtor` | 26 | DF |
| `conservacao_solo` | 26 | DF |
| `cooperativas` | 26 | DF |
| `depositos_producao` | 26 | DF |
| `despesas_receitas` | 26 | DF |
| `efetivo_asininos` | 16 | AC, AM, AP, DF, MT, PR, RJ, RO, RR, RS, SC |
| `efetivo_aves` | 26 | DF |
| `efetivo_bovinos` | 26 | DF |
| `efetivo_bubalinos` | 17 | AC, AL, BA, CE, DF, GO, PE, PI, SE, TO |
| `efetivo_caprinos` | 26 | DF |
| `efetivo_coelhos` | 6 | AC, AL, AM, AP, BA, CE, DF, GO, MA, MS, MT, PA, PB, PE, PI, RJ, RN, RO, RR, SE, TO |
| `efetivo_equinos` | 26 | DF |
| `efetivo_muares` | 23 | AM, AP, DF, RR |
| `efetivo_ovinos` | 23 | DF, MT, RJ, RO |
| `efetivo_silvicultura` | 16 | AC, AL, AM, CE, DF, PB, PI, RO, RR, SE, TO |
| `efetivo_suinos` | 26 | DF |
| `empregados_temporarios` | 26 | DF |
| `fertilizantes_defensivos` | 26 | DF |
| `forma_administracao` | 26 | DF |
| `grupos_area_lavouras` | 26 | DF |
| `grupos_area_total` | 26 | DF |
| `grupos_pessoal_ocupado` | 26 | DF |
| `horticultura` | 26 | DF |
| `inseminacao_ordenha` | 24 | AM, AP, RR |
| `irrigacao` | 26 | DF |
| `lavoura_permanente` | 26 | DF |
| `maquinas_instrumentos` | 26 | DF |
| `meios_transporte` | 26 | DF |
| `parcelas` | 26 | DF |
| `pessoal_ocupado` | 26 | DF |
| `producao_la_casulos_mel` | 11 | AC, AL, AM, AP, DF, GO, MA, MT, PA, PB, RJ, RN, RO, RR, SE, TO |
| `producao_leite` | 26 | DF |
| `producao_ovos` | 26 | DF |
| `producao_particular` | 26 | RR |
| `produtos_extrativos` | 26 | DF |
| `propriedade_terras` | 26 | DF |
| `residencia_produtor` | 26 | DF |
| `servicos_empreitada` | 26 | DF |
| `silos_forragens` | 25 | AP, DF |
| `silvicultura` | 16 | AC, AL, AP, CE, DF, PB, PI, RN, RO, RR, SE |
| `terras_fora_area` | 26 | DF |
| `terras_proprias_terceiros` | 26 | DF |
| `transformacao_beneficiamento` | 26 | DF |
| `uso_forca_trabalho` | 26 | DF |
| `utilizacao_terras` | 26 | DF |
| `valor_bens_invest_financ` | 26 | DF |

Por volume, as páginas das faixas das tabelas, as com coluna identificada, as sem grade (números lidos, nenhuma coluna) e as
células lidas com coluna. No total, 9.231 páginas, 7.299 com coluna e 1.477 sem grade; 85,8 % das células lidas têm coluna.

| UF | Volume | Páginas | Com coluna | Sem grade | Células com coluna |
|---|---|---:|---:|---:|---:|
| AC | `n3_ac` | 85 | 65 | 13 | 86,9 % |
| AL | `n15_al` | 205 | 147 | 43 | 77,0 % |
| AM | `n4_am` | 163 | 127 | 25 | 82,9 % |
| AP | `n7_ap` | 84 | 67 | 8 | 88,6 % |
| BA | `n17_ba` | 652 | 519 | 106 | 85,0 % |
| CE | `n11_ce` | 353 | 292 | 49 | 85,0 % |
| DF | `n28_df` | 11 | 8 | 2 | 85,7 % |
| ES | `n19_es` | 188 | 153 | 21 | 91,8 % |
| GO | `n27_go` | 440 | 258 | 157 | 67,2 % |
| MA | `n9_ma` | 311 | 219 | 58 | 81,7 % |
| MG | `n18_p1_mg` | 661 | 553 | 79 | 90,8 % |
| MG | `n18_p2_mg` | 702 | 619 | 68 | 91,4 % |
| MS | `n25_ms` | 192 | 139 | 47 | 75,6 % |
| MT | `n26_mt` | 167 | 110 | 48 | 74,7 % |
| PA | `n6_pa` | 247 | 212 | 22 | 90,3 % |
| PB | `n13_pb` | 320 | 259 | 42 | 86,0 % |
| PE | `n14_pe` | 381 | 300 | 65 | 84,1 % |
| PI | `n10_pi` | 237 | 196 | 32 | 88,6 % |
| PR | `n22_pr` | 678 | 550 | 105 | 87,1 % |
| RJ | `n20_rj` | 210 | 159 | 41 | 85,1 % |
| RN | `n12_rn` | 285 | 236 | 32 | 91,3 % |
| RO | `n2_ro` | 88 | 78 | 5 | 95,2 % |
| RR | `n5_rr` | 83 | 63 | 8 | 90,2 % |
| RS | `n24_rs` | 583 | 510 | 56 | 92,1 % |
| SC | `n23_sc` | 496 | 379 | 83 | 86,5 % |
| SE | `n16_se` | 159 | 128 | 19 | 88,0 % |
| SP | `n21_sp` | 1087 | 837 | 207 | 84,4 % |
| TO | `n8_to` | 163 | 116 | 36 | 78,7 % |

## Proveniência

- `source_url`: o PDF do volume no IBGE, quando a consulta usa 1 volume; o catálogo, quando usa mais.
- `raw_content_hash`: o SHA-256 do PDF (com mais de 1 volume, o SHA-256 da lista `arquivo sha256`).
- `fetch_timestamp`: a data da conferência dos PDFs contra o IBGE.
- `source_method`: `"pacote"`; `from_cache`: `False`.
- `source_details`: a tabela e o título, os volumes (arquivo, URL, SHA-256, bytes), o SHA-256 do pacote e a cobertura por página da
  consulta.

## Limites

- As páginas sem grade (1.477) trazem os números com `status` `coluna_incerta`: sem coluna, sem `valor`.
- O DF é 1 município: não há soma com ≥ 2 filhos, e nenhuma casa é confirmada.
- A PB 80 (6 casas) e a SC 115 p. 606 (a camada de texto não traz os números das primeiras linhas) ficam quase sem leitura.
- O nome do produto das tabelas 111 a 117 fica só lido.

## Exemplo

```python
from agrobr import ibge

df, meta = await ibge.censo_agro_municipal_1985("efetivo_bovinos", uf="ES", return_meta=True)
confirmadas = df[df["valor"].notna()]
largo = confirmadas.pivot_table(index="localidade", columns="coluna", values="valor")
```

## Schema JSON

Disponível em `agrobr/schemas/censo_agropecuario_municipal_1985.json`.

## Relação com outros contratos

| Contrato | Escopo | Períodos |
|----------|--------|----------|
| `censo_agropecuario` | 11 temas temáticos (SIDRA) | 1995, 2006, 2017 |
| `censo_agropecuario_legado` | 6 temas legados (FTP) | 1995 |
| `censo_agropecuario_historico` | 9 temas série histórica (SIDRA, até UF) | 1920-2006 |
| **`censo_agropecuario_municipal_1985`** | **53 tabelas municipais, casa a casa** | **1985** |

São contratos separados, sem conflito.
