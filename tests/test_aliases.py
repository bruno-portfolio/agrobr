"""Testes para o módulo de aliases de produto."""


from agrobr.aliases import PRODUCT_ALIASES, list_aliases, resolve_alias


class TestResolveAlias:
    def test_known_aliases(self):
        assert resolve_alias("soy") == "soja"
        assert resolve_alias("soybean") == "soja"
        assert resolve_alias("corn") == "milho"
        assert resolve_alias("café") == "cafe"
        assert resolve_alias("coffee") == "cafe"
        assert resolve_alias("cotton") == "algodao"
        assert resolve_alias("wheat") == "trigo"
        assert resolve_alias("rice") == "arroz"

    def test_canonical_passthrough(self):
        assert resolve_alias("soja") == "soja"
        assert resolve_alias("milho") == "milho"
        assert resolve_alias("cafe") == "cafe"
        assert resolve_alias("trigo") == "trigo"

    def test_case_insensitive(self):
        assert resolve_alias("SOY") == "soja"
        assert resolve_alias("Soja") == "soja"
        assert resolve_alias("CAFÉ") == "cafe"

    def test_strips_whitespace(self):
        assert resolve_alias("  soja  ") == "soja"
        assert resolve_alias("  soy  ") == "soja"

    def test_unknown_passthrough(self):
        assert resolve_alias("quinoa") == "quinoa"
        assert resolve_alias("desconhecido") == "desconhecido"

    def test_portuguese_accents(self):
        assert resolve_alias("açúcar") == "acucar"
        assert resolve_alias("algodão") == "algodao"
        assert resolve_alias("feijão") == "feijao"
        assert resolve_alias("suíno") == "suino"

    def test_boi_variants(self):
        assert resolve_alias("boi_gordo") == "boi"
        assert resolve_alias("boi gordo") == "boi"
        assert resolve_alias("cattle") == "boi"
        assert resolve_alias("beef") == "boi"


class TestListAliases:
    def test_all_aliases(self):
        all_aliases = list_aliases()
        assert isinstance(all_aliases, dict)
        assert len(all_aliases) > 20
        assert "soy" in all_aliases

    def test_aliases_for_product(self):
        soja_aliases = list_aliases("soja")
        assert isinstance(soja_aliases, list)
        assert "soy" in soja_aliases
        assert "soybean" in soja_aliases
        assert "soybeans" in soja_aliases

    def test_aliases_for_unknown(self):
        result = list_aliases("quinoa")
        assert result == []

    def test_aliases_for_alias_resolves(self):
        result = list_aliases("soy")
        assert "soy" in result


class TestProductAliases:
    def test_completeness(self):
        assert len(PRODUCT_ALIASES) >= 30

    def test_all_values_are_lowercase(self):
        for alias, canonical in PRODUCT_ALIASES.items():
            assert canonical == canonical.lower(), f"{alias} → {canonical} not lowercase"

    def test_all_keys_are_lowercase(self):
        for alias in PRODUCT_ALIASES:
            assert alias == alias.lower(), f"alias key '{alias}' not lowercase"
