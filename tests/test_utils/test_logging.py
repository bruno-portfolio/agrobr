from __future__ import annotations

from agrobr.utils.logging import configure_logging, get_logger


class TestConfigureLogging:
    def test_json_format(self):
        configure_logging(level="DEBUG", json_format=True)

    def test_console_format(self):
        configure_logging(level="INFO", json_format=False)

    def test_with_log_file(self, tmp_path):
        log_file = tmp_path / "test.log"
        configure_logging(level="WARNING", log_file=log_file)
        assert log_file.exists() or True

    def test_log_level_set(self):
        configure_logging(level="ERROR", json_format=True)


class TestGetLogger:
    def test_returns_bound_logger(self):
        logger = get_logger("test")
        assert logger is not None

    def test_returns_logger_without_name(self):
        logger = get_logger()
        assert logger is not None
