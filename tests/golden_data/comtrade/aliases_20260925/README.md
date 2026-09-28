# Comtrade: aliases × nomenclatura HS (25/09/2026)

Oráculo do mapa `HS_PRODUTOS_AGRO` (`agrobr/comtrade/models.py`).

- `referencia/`: **recorte** das referências oficiais da UN Comtrade (`https://comtradeapi.un.org/files/v1/app/reference/{HS,H0,...,H6}.json`),
  só com as posições do mapa e as alternativas avaliadas (0201, 0202, 0203, 0207, 0901, 1001, 1005, 1006, 1201, 1507, 1701,
  2009, 2207, 2304, 2401 a 2404, 4701 a 4706, 5201 a 5203) e as subposições delas. Mesma estrutura do arquivo inteiro
  (`results` com `id`, `text`, `parent`). Os arquivos inteiros não entram no repositório; URL, data, tamanho e SHA-256 de cada
  um estão em `manifest.json` (`referencia[].inteiro`).
- Os corpos do preview público de 2025 (`/public/v1/preview/C/A/HS`, Brasil → mundo, exportação) que mediram o impacto dos aliases
  saíram do repositório (nenhum teste os consome); o recibo e o SHA-256 de cada um seguem em
  `manifest.json` (`previews[]`, com `no_repositorio: false`).
