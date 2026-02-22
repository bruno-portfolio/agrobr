"""Tests for plugins module."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from agrobr.plugins import (
    ExporterPlugin,
    ParserPlugin,
    Plugin,
    PluginMeta,
    SourcePlugin,
    ValidatorPlugin,
    get_plugin,
    list_plugins,
    list_plugins_by_type,
    register,
    unload_all,
    unload_plugin,
)


class TestPluginMeta:
    def test_meta_creation(self):
        meta = PluginMeta(
            name="test-plugin",
            version="1.0.0",
            description="Test plugin",
            author="Test Author",
        )
        assert meta.name == "test-plugin"
        assert meta.version == "1.0.0"
        assert meta.requires == []
        assert meta.provides == []

    def test_meta_with_dependencies(self):
        meta = PluginMeta(
            name="dependent-plugin",
            version="1.0.0",
            description="Plugin with deps",
            requires=["other-plugin"],
            provides=["feature-x"],
        )
        assert "other-plugin" in meta.requires
        assert "feature-x" in meta.provides


class TestPluginRegistry:
    def setup_method(self):
        unload_all()

    def teardown_method(self):
        unload_all()

    def test_register_plugin(self):
        @register
        class TestPlugin(Plugin):
            meta = PluginMeta(name="test", version="1.0", description="Test")

            def setup(self) -> None:
                pass

            def teardown(self) -> None:
                pass

        plugins = list_plugins()
        assert any(p.name == "test" for p in plugins)

    def test_get_plugin(self):
        @register
        class MyPlugin(Plugin):
            meta = PluginMeta(name="my-plugin", version="1.0", description="My plugin")

            def setup(self) -> None:
                self.initialized = True

            def teardown(self) -> None:
                pass

        plugin = get_plugin("my-plugin")
        assert plugin is not None
        assert hasattr(plugin, "initialized")
        assert plugin.initialized is True

    def test_get_plugin_not_found(self):
        plugin = get_plugin("nonexistent")
        assert plugin is None

    def test_unload_plugin(self):
        @register
        class ToUnload(Plugin):
            meta = PluginMeta(name="to-unload", version="1.0", description="Will be unloaded")

            def setup(self) -> None:
                pass

            def teardown(self) -> None:
                pass

        assert any(p.name == "to-unload" for p in list_plugins())
        unload_plugin("to-unload")
        assert not any(p.name == "to-unload" for p in list_plugins())


class TestPluginTypes:
    def setup_method(self):
        unload_all()

    def teardown_method(self):
        unload_all()

    def test_source_plugin(self):
        @register
        class MySource(SourcePlugin):
            meta = PluginMeta(name="my-source", version="1.0", description="Source plugin")

            def setup(self) -> None:
                pass

            def teardown(self) -> None:
                pass

            async def fetch(self, **_kwargs: Any) -> Any:
                return "data"

            async def parse(self, content: Any, **_kwargs: Any) -> Any:
                return {"parsed": content}

        sources = list_plugins_by_type(SourcePlugin)
        assert len(sources) == 1
        assert sources[0].name == "my-source"

    def test_parser_plugin(self):
        @register
        class MyParser(ParserPlugin):
            meta = PluginMeta(name="my-parser", version="1.0", description="Parser plugin")

            def setup(self) -> None:
                pass

            def teardown(self) -> None:
                pass

            def can_parse(self, content: str) -> bool:
                return "special" in content

            def parse(self, content: str, **_kwargs: Any) -> Any:
                return {"content": content}

            @property
            def priority(self) -> int:
                return 10

        parsers = list_plugins_by_type(ParserPlugin)
        assert len(parsers) == 1

        plugin = get_plugin("my-parser")
        assert plugin is not None
        assert plugin.priority == 10

    def test_exporter_plugin(self):
        @register
        class MyExporter(ExporterPlugin):
            meta = PluginMeta(name="my-exporter", version="1.0", description="Exporter")

            def setup(self) -> None:
                pass

            def teardown(self) -> None:
                pass

            def export(self, _data: Any, path: Path, **_kwargs: Any) -> Path:
                return path

            def get_extension(self) -> str:
                return ".custom"

        exporters = list_plugins_by_type(ExporterPlugin)
        assert len(exporters) == 1

        plugin = get_plugin("my-exporter")
        assert plugin is not None
        assert plugin.get_extension() == ".custom"

    def test_validator_plugin(self):
        @register
        class MyValidator(ValidatorPlugin):
            meta = PluginMeta(name="my-validator", version="1.0", description="Validator")

            def setup(self) -> None:
                pass

            def teardown(self) -> None:
                pass

            def validate(self, _data: Any, **_kwargs: Any) -> tuple[bool, list[str]]:
                return True, []

        validators = list_plugins_by_type(ValidatorPlugin)
        assert len(validators) == 1

        plugin = get_plugin("my-validator")
        assert plugin is not None
        valid, errors = plugin.validate({"test": "data"})
        assert valid is True
        assert errors == []


class TestLoadPluginFromFile:
    def setup_method(self):
        unload_all()

    def teardown_method(self):
        unload_all()

    def test_file_not_found(self, tmp_path):
        from agrobr.plugins import load_plugin_from_file

        result = load_plugin_from_file(tmp_path / "nonexistent.py")
        assert result is None

    def test_load_valid_plugin(self, tmp_path):
        from agrobr.plugins import load_plugin_from_file

        plugin_file = tmp_path / "my_plugin.py"
        plugin_file.write_text("""
from agrobr.plugins import Plugin, PluginMeta

class MyFilePlugin(Plugin):
    meta = PluginMeta(name="file-plugin", version="1.0", description="From file")
    def setup(self):
        pass
    def teardown(self):
        pass
""")
        result = load_plugin_from_file(plugin_file)
        assert result is not None
        assert any(p.name == "file-plugin" for p in list_plugins())

    def test_no_plugin_in_file(self, tmp_path):
        from agrobr.plugins import load_plugin_from_file

        plugin_file = tmp_path / "empty_plugin.py"
        plugin_file.write_text("x = 42\n")
        result = load_plugin_from_file(plugin_file)
        assert result is None


class TestLoadPluginsFromDir:
    def setup_method(self):
        unload_all()

    def teardown_method(self):
        unload_all()

    def test_empty_dir(self, tmp_path):
        from agrobr.plugins import load_plugins_from_dir

        result = load_plugins_from_dir(tmp_path)
        assert result == []

    def test_nonexistent_dir(self, tmp_path):
        from agrobr.plugins import load_plugins_from_dir

        result = load_plugins_from_dir(tmp_path / "does_not_exist")
        assert result == []

    def test_skips_underscore_files(self, tmp_path):
        from agrobr.plugins import load_plugins_from_dir

        (tmp_path / "__init__.py").write_text("")
        (tmp_path / "_private.py").write_text("")
        result = load_plugins_from_dir(tmp_path)
        assert result == []

    def test_loads_valid_plugins(self, tmp_path):
        from agrobr.plugins import load_plugins_from_dir

        (tmp_path / "plug1.py").write_text("""
from agrobr.plugins import Plugin, PluginMeta

class DirPlugin(Plugin):
    meta = PluginMeta(name="dir-plugin", version="1.0", description="From dir")
    def setup(self):
        pass
    def teardown(self):
        pass
""")
        result = load_plugins_from_dir(tmp_path)
        assert len(result) == 1


class TestPluginLifecycle:
    def setup_method(self):
        unload_all()

    def teardown_method(self):
        unload_all()

    def test_setup_called_on_get(self):
        calls = []

        @register
        class LifecyclePlugin(Plugin):
            meta = PluginMeta(name="lifecycle", version="1.0", description="Test")

            def setup(self):
                calls.append("setup")

            def teardown(self):
                calls.append("teardown")

        get_plugin("lifecycle")
        assert "setup" in calls
        unload_plugin("lifecycle")
        assert "teardown" in calls

    def test_register_without_meta_raises(self):
        with pytest.raises(ValueError, match="meta"):

            @register
            class NoMeta(Plugin):
                def setup(self):
                    pass

                def teardown(self):
                    pass

    def test_override_plugin(self):
        @register
        class First(Plugin):
            meta = PluginMeta(name="override-test", version="1.0", description="First")

            def setup(self):
                pass

            def teardown(self):
                pass

        @register
        class Second(Plugin):
            meta = PluginMeta(name="override-test", version="2.0", description="Second")

            def setup(self):
                pass

            def teardown(self):
                pass

        plugins = list_plugins()
        override_plugins = [p for p in plugins if p.name == "override-test"]
        assert len(override_plugins) == 1
        assert override_plugins[0].version == "2.0"


class TestUnloadAll:
    def setup_method(self):
        unload_all()

    def test_clears_all(self):
        @register
        class P1(Plugin):
            meta = PluginMeta(name="p1", version="1.0", description="")

            def setup(self):
                pass

            def teardown(self):
                pass

        @register
        class P2(Plugin):
            meta = PluginMeta(name="p2", version="1.0", description="")

            def setup(self):
                pass

            def teardown(self):
                pass

        assert len(list_plugins()) == 2
        unload_all()
        assert len(list_plugins()) == 0
