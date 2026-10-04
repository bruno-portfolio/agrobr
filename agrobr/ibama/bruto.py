from __future__ import annotations

from agrobr.bruto import arquivo

from .models import CSV_URL

adaptador = arquivo.AdaptadorArquivo(CSV_URL, None, arquivo.conferir_csv("SEQ_TAD", "NUM_TAD"))
