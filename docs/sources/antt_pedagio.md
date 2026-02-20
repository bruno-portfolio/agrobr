# ANTT Pedagio

## Sobre a fonte

A **ANTT** (Agencia Nacional de Transportes Terrestres) publica dados abertos de fluxo de veiculos em pracas de pedagio rodoviario no portal [dados.antt.gov.br](https://dados.antt.gov.br).

Os dados sao um proxy de escoamento de safra: veiculos comerciais pesados (3+ eixos) correlacionam fortemente com transporte de graos por rodovias.

## Datasets

### Volume de Trafego (principal)
- **Cobertura:** 2010-2025 (16 CSVs anuais)
- **Granularidade:** Mensal, por praca/concessionaria/categoria de veiculo
- **Formato:** CSV separado por `;`
- **Schema V1 (2010-2023):** Com header, categoria como texto ("Categoria 1"-"Categoria 9")
- **Schema V2 (2024+):** Sem header, eixos como numero (2-18)

### Cadastro de Pracas (referencia)
- **Formato:** CSV unico (~200+ pracas ativas)
- **Colunas:** concessionaria, praca, rodovia, UF, km, municipio, lat/lon, situacao
- **Uso:** Join automatico com dados de trafego para enriquecer com UF/rodovia/municipio

## Licenca

CC-BY (Creative Commons Attribution). Dados abertos sem necessidade de autenticacao.

## Mapeamento de Categorias

| Categoria V1 | Eixos | Tipo |
|-------------|-------|------|
| 1 | 2 | Passeio |
| 2 | 2 | Comercial |
| 3 | 3 | Passeio |
| 4 | 3 | Comercial |
| 5 | 4 | Passeio |
| 6 | 4 | Comercial |
| 7 | 5 | Comercial |
| 8 | 6 | Comercial |
| 9 | 2 | Moto |

## Notas tecnicas

- `apenas_pesados=True` filtra `n_eixos >= 3 AND tipo_veiculo == "Comercial"`
- Volume e agregado por praca/mes/eixo (tipo de cobranca automatica/manual e somado)
- Default sem filtro de ano = ano atual + anterior (evita baixar 16 CSVs)
- Encoding: Windows-1252 com fallback chain automatico
