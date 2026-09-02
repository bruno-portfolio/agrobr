from __future__ import annotations

from datetime import datetime


def validate_uf(uf: str | None) -> str | None:
    if uf is None:
        return None
    uf_upper = uf.strip().upper()
    from agrobr.normalize.regions import UFS_VALIDAS

    if uf_upper not in UFS_VALIDAS:
        raise ValueError(f"UF invalida: {uf!r}")
    return uf_upper


def validate_bioma(bioma: str | None) -> str | None:
    if bioma is None:
        return None

    from agrobr.normalize.regions import BIOMAS_VALIDOS, normalizar_bioma

    bioma_normalizado = normalizar_bioma(bioma)
    if bioma_normalizado not in BIOMAS_VALIDOS:
        validos = ", ".join(sorted(BIOMAS_VALIDOS))
        raise ValueError(f"Bioma inválido: {bioma!r}. Valores válidos: {validos}")
    return bioma_normalizado


def validate_year_uf(
    *,
    uf: str | None = None,
    ano: int | None = None,
    ano_inicio: int | None = None,
    ano_fim: int | None = None,
    ufs_validas: frozenset[str] | None = None,
    ano_min: int = 2000,
) -> None:
    if ufs_validas is None:
        from agrobr.normalize.regions import UFS_VALIDAS

        ufs_validas = UFS_VALIDAS

    if uf and uf.strip().upper() not in ufs_validas:
        raise ValueError(f"UF '{uf}' invalida. Opcoes: {sorted(ufs_validas)}")

    current_year = datetime.now().year
    if ano is not None and (ano < ano_min or ano > current_year):
        raise ValueError(f"Ano {ano} fora do range valido ({ano_min}-{current_year})")
    if ano_inicio is not None and ano_inicio < ano_min:
        raise ValueError(f"ano_inicio {ano_inicio} anterior a {ano_min}")
    if ano_fim is not None and ano_fim > current_year:
        raise ValueError(f"ano_fim {ano_fim} posterior ao ano atual ({current_year})")
    if ano_inicio is not None and ano_fim is not None and ano_inicio > ano_fim:
        raise ValueError(f"ano_inicio ({ano_inicio}) > ano_fim ({ano_fim})")
