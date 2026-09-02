from agrobr.ana.models import ANA_BASE, LAYERS


class TestLayers:
    def test_layer_count(self):
        assert len(LAYERS) == 4

    def test_layer_keys(self):
        expected = {
            "hidrografia",
            "pivos_irrigacao",
            "demanda_irrigacao",
            "disponibilidade_hidrica",
        }
        assert set(LAYERS.keys()) == expected

    def test_each_layer_has_required_config(self):
        required_keys = {
            "service_path",
            "max_record_count",
            "fields",
            "rename_map",
            "colunas_saida",
            "required_cols",
        }
        for key, config in LAYERS.items():
            for rk in required_keys:
                assert rk in config, f"Layer {key!r} missing config key {rk!r}"

    def test_each_layer_uses_mapserver(self):
        assert all(config["service_path"].endswith("MapServer/0") for config in LAYERS.values())


class TestAnaBase:
    def test_base_url_https(self):
        assert ANA_BASE.startswith("https://")

    def test_base_url_domain(self):
        assert "snirh" in ANA_BASE or "ana.gov" in ANA_BASE or "arcgis" in ANA_BASE
