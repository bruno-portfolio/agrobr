# Acervo Fundiário — estado do cache após revalidação (23/09/2026)

Oráculo da conferência de 23/09/2026. `RR.json` é o sidecar real do cache do agrobr para
SNCI/RR, gravado pela API pública: ETag, Last-Modified, tamanho, SHA-256 e `fetched_at` da coleta original.

O ZIP daquele cache não foi copiado: guarda nomes de imóvel que são nome de pessoa. O teste usa o recorte
`../snci_rr_20260922/response.zip` (mesmo arquivo oficial, mesmos validadores) e ajusta só `size_bytes` e
`sha256` do sidecar a ele; ver `manifest.json`.
