# censo_agropecuario_municipal_1985 v2.0

Agricultural Census 1985, municipal tables 67 to 119 of the IBGE's 28 state volumes (27 states; Minas Gerais in 2 volumes), with
53 themes, 1 per table. agrobr extracted the numbers from the PDFs published by the IBGE and ships them in a local package
(`agrobr/data/censo_1985/`): queries do not use the network.

**Each row is a PDF cell**: the one with the number and also the unread ones, the ones without an identified column and the ones
outside the grid, each with its `status`. **`valor` is filled only when the cell was confirmed by the printed sums** (municipality →
microregion → mesoregion → state); **`valor_lido` always carries the reading**. Filter by `status` to choose the confidence level.
There are 2,275,606 cells, of which 579,456 (25.5%) are confirmed.

In API 2.0, only `tema` accepts positional arguments; all other filters and flags are passed by keyword. Empty results preserve contract dtypes: integers use `Int64`, measures use `float64`, and text follows the installed pandas default.

## Sources

| Priority | Source | Description |
|------------|-------|-----------|
| 1 | IBGE Agricultural Census 1985 | agrobr package extracted from the IBGE's 28 PDFs (`agrobr/data/censo_1985/`) |

## Status of each cell and measured precision

Precision was measured against PDF cells from 34 tables in 11 states.

| `status` | Meaning | `valor` | Measured precision |
|---|---|---|---|
| `confirmado_soma_exata` | The children's sum (≥ 2 non-zero) matches the printed parent exactly | filled | 2,427 right, 0 wrong |
| `confirmado_soma_arredondada_2a_compativel` | The sum matches within rounding, and the independent 2nd reading (RapidOCR) is compatible | filled | 641 right, 0 wrong |
| `concorda_2a_leitura` | The text layer and the 2nd reading give the same number, with no confirming sum | null | 98.3% (2,052 of 2,087) |
| `nao_verificado` | Number read, not confirmed | null | 83.0% (572 of 689) |
| `pdf_inconsistente_candidato` | The PDF itself does not add up in that cell (the group's other columns do) | null | 12 of 14 |
| `correcao_colada_nao_confirmavel` | Number pasted over the page (larger type), outside the sums | null | 6 of 7 |
| `casa_repetida` | 2 readings in the same cell | null | — |
| `sem_leitura` | Grid cell the extraction did not read | null | 50.2% have no number in the PDF; the others do, almost always 1 digit |
| `coluna_incerta` | Number read without an identified column | null | — |
| `fora_de_coluna` | Number outside the table grid | null | — |

- The error left in `concorda_2a_leitura` is the digit or the leftmost thousands group lost the same way in both readings (684
  instead of 8,684).
- `sem_leitura` is not zero: reading the cell as zero would be wrong half of the time.

**MA, PI, CE and RN** use the version the IBGE republished on 2018-09-03, with a text layer (Adobe OCR, with its own errors, such as
"71 o" for 710). Measured separately: exact 816 right and 0 wrong, rounded 132 and 0, `concorda_2a_leitura` 97.6% (683 of 700).
Against the 2008 image-only copy, today's version confirms 22.6% more cells.

## The column name

- `coluna_nome_lido`: the header text as read, whenever there is text, cleaned only by 5 listed rules (line-break hyphen, `!` for
  `I`, the title leaking into the group).
- `coluna_nome`: only when confirmed. `coluna_nome_status` says how:
  - `canonico`: the leaf read on the page is the same (without accents, letters and digits only) as ≥ 3 volumes read in the same
    position (table, page side, number of columns, column), in ≥ 50% of the readings. **It is the printed name confirmed by ≥ 3
    volumes, in the text layer's spelling**, word break and hyphen included: for example, "ARRENDATARIO | ESTABELE-1 CIMENTOS" in
    table 70. 972,311 cells (42.7%), in 46 tables;
  - `lido`: there is text, but it does not repeat in 3 volumes;
  - `sem_nome`: the cell has no column, or the page has no legible header.
- In the product tables (111 to 117), the product changes from page to page: the confirmed leaf gives `variavel` and `unidade`, and
  the product stays only in `coluna_nome_lido` (`coluna_nome` null, status `lido`).
- `unidade`: only with a confirmed leaf. It comes from the header mark (1) to (4) and the volume's note, whose meaning changes
  between volumes (in AC, "(1) TONELADAS (2) MIL FRUTOS"; in ES, the reverse). A mark that does not open with a unit is a note
  ("(2) INCLUSIVE PÉS NOVOS") and does not become a unit. Without a mark, it comes from the label ("ÁREA (HA)", "INFORMANTES"). A
  unit that is only in the group above comes out null.
- Precision against the PDF columns of tables 108 to 116 and 111 (AC, ES, GO, PE and PR): `coluna_nome` 21 right and 0
  with another column's variable; `coluna_nome_lido` 411 right, 26 incomplete and 0 from another column; `unidade` 111 right and 0
  wrong.

## Schema

| Column | Type | Nullable | Description |
|--------|------|----------|-----------|
| `ano` | Int64 | ❌ | Always 1985 |
| `uf` | str | ❌ | State code |
| `volume` | str | ❌ | IBGE volume (`n18_p1_mg` and `n18_p2_mg` for Minas Gerais) |
| `tabela` | Int64 | ❌ | Table number (67 to 119), the same theme in every volume |
| `tema` | str | ❌ | Table theme, from the printed title |
| `pagina_pdf` | Int64 | ❌ | PDF page (counted from 1) |
| `pagina_impressa` | Int64 | ✅ | Number printed in the footer |
| `linha` | Int64 | ❌ | Row of the place in the page block |
| `coluna` | Int64 | ❌ | Physical column on the page (0 on the left); negative when the cell has no identified column |
| `nivel` | str | ❌ | `uf`, `mesorregiao`, `microrregiao` or `municipio` |
| `localidade` | str | ❌ | Name as read, with the reading noise: "SERTÓES OE SENADOR POMPEU" (CE), "!NHAP!" (AL) |
| `coluna_nome` | str | ✅ | Column name, only when confirmed (see above) |
| `coluna_nome_lido` | str | ✅ | Header text as read |
| `coluna_nome_status` | str | ❌ | `canonico`, `lido` or `sem_nome` |
| `variavel` | str | ✅ | The column's leaf, only when confirmed |
| `unidade` | str | ✅ | Unit, only when the leaf is confirmed |
| `unidade_lida` | str | ✅ | Unit from the volume's note mark or from the column label |
| `valor` | Int64 | ✅ | Confirmed number |
| `valor_lido` | Int64 | ✅ | Number read |
| `marcador` | str | ✅ | "-", ".." or "..." when the cell carries a sign |
| `status` | str | ❌ | See the table above |
| `reparado` | bool | ❌ | The leftmost thousands group was recovered by re-reading the crop; `False` in every cell of this version |

## Primary Key

`(volume, tabela, pagina_pdf, linha, coluna)`. There is no municipality code: 1985 municipalities do not map 1:1 to today's codes.

## Refused queries

- Invalid theme, state or level: `InvalidParameterError`, before reading the package.
- A state whose volume lacks the table (the IBGE omits a table that does not apply to the state): `InvalidParameterError`, with the
  states that have the table.
- A table that is in the volume but of which the extraction read no cell (AM 80, AP 80, RR 80 and RR 119, pages with fewer than 10
  numbers): `ParseError`, with the pages and the reason.
- A level filter that finds no row returns an empty DataFrame.

## Coverage

53 themes in 27 states. DF has only 4 tables; in the other states, the table the volume omits is missing. The table below gives,
per theme, the states with cells in the package.

| Theme | States | Missing |
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

Per volume, the pages in the tables' ranges, those with an identified column, those without a grid (numbers read, no column) and
the cells read with a column. In total, 9,231 pages, 7,299 with a column and 1,477 without a grid; 85.8% of the cells read have a
column.

| State | Volume | Pages | With column | Without grid | Cells with column |
|---|---|---:|---:|---:|---:|
| AC | `n3_ac` | 85 | 65 | 13 | 86.9% |
| AL | `n15_al` | 205 | 147 | 43 | 77.0% |
| AM | `n4_am` | 163 | 127 | 25 | 82.9% |
| AP | `n7_ap` | 84 | 67 | 8 | 88.6% |
| BA | `n17_ba` | 652 | 519 | 106 | 85.0% |
| CE | `n11_ce` | 353 | 292 | 49 | 85.0% |
| DF | `n28_df` | 11 | 8 | 2 | 85.7% |
| ES | `n19_es` | 188 | 153 | 21 | 91.8% |
| GO | `n27_go` | 440 | 258 | 157 | 67.2% |
| MA | `n9_ma` | 311 | 219 | 58 | 81.7% |
| MG | `n18_p1_mg` | 661 | 553 | 79 | 90.8% |
| MG | `n18_p2_mg` | 702 | 619 | 68 | 91.4% |
| MS | `n25_ms` | 192 | 139 | 47 | 75.6% |
| MT | `n26_mt` | 167 | 110 | 48 | 74.7% |
| PA | `n6_pa` | 247 | 212 | 22 | 90.3% |
| PB | `n13_pb` | 320 | 259 | 42 | 86.0% |
| PE | `n14_pe` | 381 | 300 | 65 | 84.1% |
| PI | `n10_pi` | 237 | 196 | 32 | 88.6% |
| PR | `n22_pr` | 678 | 550 | 105 | 87.1% |
| RJ | `n20_rj` | 210 | 159 | 41 | 85.1% |
| RN | `n12_rn` | 285 | 236 | 32 | 91.3% |
| RO | `n2_ro` | 88 | 78 | 5 | 95.2% |
| RR | `n5_rr` | 83 | 63 | 8 | 90.2% |
| RS | `n24_rs` | 583 | 510 | 56 | 92.1% |
| SC | `n23_sc` | 496 | 379 | 83 | 86.5% |
| SE | `n16_se` | 159 | 128 | 19 | 88.0% |
| SP | `n21_sp` | 1087 | 837 | 207 | 84.4% |
| TO | `n8_to` | 163 | 116 | 36 | 78.7% |

## Provenance

- `source_url`: the volume's PDF at the IBGE, when the query uses 1 volume; the catalog, when it uses more.
- `raw_content_hash`: the PDF's SHA-256 (with more than 1 volume, the SHA-256 of the `file sha256` list).
- `fetch_timestamp`: the date the PDFs were checked against the IBGE.
- `source_method`: `"pacote"`; `from_cache`: `False`.
- `source_details`: the table and its title, the volumes (file, URL, SHA-256, bytes), the package's SHA-256 and the query's per-page
  coverage.

## Limits

- The pages without a grid (1,477) carry their numbers with `status` `coluna_incerta`: no column, no `valor`.
- DF is 1 municipality: there is no sum with ≥ 2 children, and no cell is confirmed.
- PB 80 (6 cells) and SC 115 p. 606 (the text layer lacks the numbers of the first rows) are almost unread.
- The product name in tables 111 to 117 is only read.

## Example

```python
from agrobr import ibge

df, meta = await ibge.censo_agro_municipal_1985("efetivo_bovinos", uf="ES", return_meta=True)
confirmed = df[df["valor"].notna()]
wide = confirmed.pivot_table(index="localidade", columns="coluna", values="valor")
```

## JSON Schema

Available at `agrobr/schemas/censo_agropecuario_municipal_1985.json`.

## Relationship with other contracts

| Contract | Scope | Periods |
|----------|--------|----------|
| `censo_agropecuario` | 11 thematic themes (SIDRA) | 1995, 2006, 2017 |
| `censo_agropecuario_legado` | 6 legacy themes (FTP) | 1995 |
| `censo_agropecuario_historico` | 9 historical series themes (SIDRA, up to state) | 1920-2006 |
| **`censo_agropecuario_municipal_1985`** | **53 municipal tables, cell by cell** | **1985** |

These are separate contracts, no conflict.
