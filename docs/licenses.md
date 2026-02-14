# Licenças e Termos de Uso das Fontes

> **Aviso:** O agrobr é licenciado sob MIT, mas os **dados** acessados pertencem
> às suas respectivas fontes e estão sujeitos aos termos de cada uma.
> É responsabilidade do usuário verificar se seu caso de uso está em conformidade.

## Tabela de Fontes

| Fonte | Licença | Uso Comercial | Classificação | URL dos Termos |
|-------|---------|---------------|---------------|----------------|
| **CEPEA/ESALQ** | CC BY-NC 4.0 | Requer autorização do CEPEA | `nc` | [Licença](https://www.cepea.org.br/br/licenca-de-uso-de-dados.aspx) |
| **CONAB** | Dados públicos governo federal | Sim (dados públicos) | `livre` | [Gov.br](https://www.gov.br/conab/) |
| **IBGE/SIDRA** | Dados públicos governo federal | Sim (dados públicos) | `livre` | [SIDRA](https://sidra.ibge.gov.br) |
| **NASA POWER** | CC BY 4.0 | Sim (com citação) | `livre` | [Earthdata Policy](https://earthdata.nasa.gov/collaborate/open-data-services-and-software) |
| **BCB/SICOR** | Dados públicos governo federal | Sim (dados públicos) | `livre` | [BCB OData](https://olinda.bcb.gov.br) |
| **ComexStat** | Dados públicos governo federal | Sim (dados públicos) | `livre` | [MDIC](https://comexstat.mdic.gov.br) |
| **ANDA** | Sem termos públicos; autorização solicitada (fev/2026) | Aguardando resposta | `zona_cinza` | [anda.org.br](https://anda.org.br) |
| **ABIOVE** | Sem termos públicos; autorização solicitada (fev/2026) | Aguardando resposta | `zona_cinza` | [abiove.org.br](https://abiove.org.br) |
| **USDA PSD** | U.S. Public Domain | Sim (governo EUA) | `livre` | [Ag Data Commons](https://data.nal.usda.gov/dataset/usda-foreign-agricultural-service-production-supply-and-distribution-database) |
| **IMEA** | Restritivo: redistribuição proibida sem autorização escrita | Não | `restrito` | [Termo de Uso](https://imea.com.br/imea-site/termo-de-uso.html) |
| **DERAL** | Dados públicos governo estadual PR | Sim (dados públicos) | `livre` | [SEAB/PR](https://www.agricultura.pr.gov.br) |
| **INMET** | Dados públicos governo federal | Sim (dados públicos, token requerido) | `livre` | [INMET](https://portal.inmet.gov.br) |
| **Notícias Agrícolas** | Todos os direitos reservados (Lei 9.610/98) | Não | `restrito` | — |
| **Queimadas/INPE** | Dados públicos governo federal | Sim (dados públicos, com citação) | `livre` | [BDQueimadas](https://queimadas.dgi.inpe.br) |
| **Desmatamento PRODES/DETER** | Dados públicos governo federal | Sim (dados públicos, com citação) | `livre` | [TerraBrasilis](https://terrabrasilis.dpi.inpe.br) |

### Legenda de Classificação

| Classificação | Significado |
|---------------|-------------|
| `livre` | Dados públicos sem restrição a uso comercial. Citar a fonte é boa prática. |
| `nc` | Non-commercial. Uso comercial requer autorização explícita do detentor. |
| `zona_cinza` | Termos de uso não localizados publicamente ou ambíguos. Autorização solicitada. |
| `restrito` | Redistribuição proibida ou condicionada a autorização escrita. |

## Detalhes por Fonte

### CEPEA/ESALQ

- **Licença:** Creative Commons BY-NC 4.0
- **Resumo:** Permite uso não-comercial com atribuição ao CEPEA. Uso comercial
  (revenda de dados, integração em produtos pagos, etc.) requer autorização
  escrita do CEPEA.
- **Contato:** cepea@usp.br
- **Referência EN:** [Non-commercial use of data](https://cepea.esalq.usp.br/en/non-commercial-use-of-data.aspx)

### IMEA

- **Classificação:** `restrito`
- **Termos:** "Todo o arquivo não público disponibilizado pela plataforma, seja
  ele relatório ou dado, é de uso exclusivo do usuário não podendo ser
  compartilhado sem prévia autorização por escrito."
- **Situação:** API pública (`api1.imea.com.br`) sem autenticação, mas o Termo
  de Uso é explícito sobre proibição de redistribuição. O endpoint `/v2/mobile/`
  não é documentado oficialmente.
- **Recomendação:** Módulo mantido para uso pessoal/educacional direto. Não
  incluir em datasets de fallback automático. Usuários que redistribuam dados
  devem obter autorização escrita do IMEA.

### ANDA

- **Classificação:** `zona_cinza`
- **Situação:** Associação setorial que publica dados de entregas de fertilizantes.
  Sem página de termos de uso pública localizada.
- **Ação:** Autorização formal solicitada em fevereiro/2026. Aguardando resposta.

### ABIOVE

- **Classificação:** `zona_cinza`
- **Situação:** Associação setorial que publica dados de exportação do complexo soja.
  Sem página de termos de uso pública localizada.
- **Ação:** Autorização formal solicitada em fevereiro/2026. Aguardando resposta.

### Notícias Agrícolas

- **Classificação:** `restrito`
- **Situação:** Empresa privada (Olivi Produções de Vídeo e Comunicação LTDA)
  sem termos de uso públicos sobre republicação de cotações. Pela Lei 9.610/98,
  ausência de licença explícita implica todos os direitos reservados.
- **Decisão no agrobr:** Mantido temporariamente como fallback técnico do CEPEA
  (contorna Cloudflare). Pendente deprecação em favor de acesso direto ao CEPEA
  ou outras fontes primárias (DERAL, etc.).

### Fontes Governamentais Brasileiras

CONAB, IBGE, BCB, ComexStat, DERAL, INMET e INPE (Queimadas, PRODES/DETER) são órgãos públicos brasileiros.
Dados produzidos por órgãos públicos no exercício de suas funções são, em regra,
de acesso público (Lei de Acesso à Informação — Lei 12.527/2011). Não há
restrição a uso comercial de dados públicos governamentais, mas a citação da
fonte é recomendada.

O agrobr embute códigos IBGE de 5571 municípios (arquivo `_municipios_ibge.json`)
obtidos da [API IBGE Localidades](https://servicodados.ibge.gov.br/api/docs/localidades),
que é pública e livre para uso.

### NASA POWER

Dados NASA são disponibilizados sob CC BY 4.0. Uso livre (incluindo comercial)
com citação obrigatória ao NASA POWER Project.

### USDA PSD

Dados do governo americano estão em domínio público nos EUA (17 U.S.C. 105).
Uso livre sem restrições de copyright dentro dos EUA. Para uso internacional,
verificar se CC0 foi aplicado explicitamente.
