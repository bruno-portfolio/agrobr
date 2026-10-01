from __future__ import annotations

import warnings
from pathlib import Path

import httpx
import pydantic
import pytest

from agrobr import constants
from agrobr.exceptions import InvalidParameterError
from agrobr.http.settings import get_timeout


class TestGetTimeout:
    def test_returns_httpx_timeout(self):
        timeout = get_timeout()
        assert isinstance(timeout, httpx.Timeout)


def test_timeout_read_do_ambiente_vale_como_minimo(monkeypatch):
    assert get_timeout(read=120.0).read == 120.0
    monkeypatch.setenv("AGROBR_HTTP_TIMEOUT_READ", "900")
    assert get_timeout(read=120.0).read == 900.0
    monkeypatch.setenv("AGROBR_HTTP_TIMEOUT_READ", "10")
    assert get_timeout(read=120.0).read == 120.0
    assert get_timeout().read == 10.0


@pytest.mark.parametrize(("valor", "tentativas"), [("0", 1), ("1", 1), ("3", 3), ("5", 5)])
def test_max_retries_zero_vale_uma_tentativa_e_positivos_ficam(monkeypatch, valor, tentativas):
    monkeypatch.setenv("AGROBR_HTTP_MAX_RETRIES", valor)
    assert constants.HTTPSettings().max_retries == tentativas


def test_max_retries_negativo_e_recusado_na_validacao(monkeypatch):
    monkeypatch.setenv("AGROBR_HTTP_MAX_RETRIES", "-1")
    with pytest.raises(pydantic.ValidationError, match="max_retries"):
        constants.HTTPSettings()


@pytest.mark.parametrize("fonte", ["default", "ana", "anp_diesel", "b3", "ibge"])
@pytest.mark.parametrize("valor", ["0", "-1"])
def test_max_concurrent_menor_que_um_e_recusado_na_validacao(monkeypatch, fonte, valor):
    monkeypatch.setenv(f"AGROBR_HTTP_MAX_CONCURRENT_{fonte.upper()}", valor)
    with pytest.raises(pydantic.ValidationError, match=f"max_concurrent_{fonte}"):
        constants.HTTPSettings()


class TestCacheDir:
    @pytest.fixture(autouse=True)
    def _sem_variaveis(self, monkeypatch):
        monkeypatch.delenv("AGROBR_CACHE_DIR", raising=False)
        monkeypatch.delenv("AGROBR_CACHE_CACHE_DIR", raising=False)

    def test_nome_principal_e_alias(self, monkeypatch, tmp_path):
        monkeypatch.setenv("AGROBR_CACHE_DIR", str(tmp_path / "novo"))
        assert constants.CacheSettings().cache_dir == tmp_path / "novo"
        monkeypatch.delenv("AGROBR_CACHE_DIR")
        monkeypatch.setenv("AGROBR_CACHE_CACHE_DIR", str(tmp_path / "antigo"))
        assert constants.CacheSettings().cache_dir == tmp_path / "antigo"

    def test_as_duas_diferentes_vale_a_nova_com_aviso(self, monkeypatch, tmp_path):
        monkeypatch.setenv("AGROBR_CACHE_DIR", str(tmp_path / "novo"))
        monkeypatch.setenv("AGROBR_CACHE_CACHE_DIR", str(tmp_path / "antigo"))
        with pytest.warns(UserWarning, match="vale AGROBR_CACHE_DIR"):
            assert constants.CacheSettings().cache_dir == tmp_path / "novo"

    def test_as_duas_iguais_nao_avisam(self, monkeypatch, tmp_path):
        monkeypatch.setenv("AGROBR_CACHE_DIR", str(tmp_path))
        monkeypatch.setenv("AGROBR_CACHE_CACHE_DIR", str(tmp_path))
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            assert constants.CacheSettings().cache_dir == tmp_path

    @pytest.mark.parametrize("variavel", ["AGROBR_CACHE_DIR", "AGROBR_CACHE_CACHE_DIR"])
    @pytest.mark.parametrize("valor", ["", "   "])
    def test_vazia_vale_o_padrao_e_nao_a_pasta_corrente(self, monkeypatch, variavel, valor):
        monkeypatch.setenv(variavel, valor)
        assert constants.CacheSettings().cache_dir == Path.home() / ".agrobr" / "cache"

    @pytest.mark.parametrize(
        "variaveis",
        [
            ("AGROBR_CACHE_DIR",),
            ("AGROBR_CACHE_CACHE_DIR",),
            ("AGROBR_CACHE_DIR", "AGROBR_CACHE_CACHE_DIR"),
        ],
    )
    def test_argumento_python_prevalece(self, monkeypatch, tmp_path, variaveis):
        for variavel in variaveis:
            monkeypatch.setenv(variavel, str(tmp_path / variavel))
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            assert constants.CacheSettings(cache_dir=tmp_path / "arg").cache_dir == tmp_path / "arg"

    @pytest.mark.parametrize("valor", ["", "   "])
    def test_nova_vazia_com_a_antiga_definida_vale_o_padrao(self, monkeypatch, tmp_path, valor):
        monkeypatch.setenv("AGROBR_CACHE_DIR", valor)
        monkeypatch.setenv("AGROBR_CACHE_CACHE_DIR", str(tmp_path / "antigo"))
        assert constants.CacheSettings().cache_dir == Path.home() / ".agrobr" / "cache"

    @pytest.mark.parametrize("nome", ["agrobr_cache_dir", "Agrobr_Cache_Dir"])
    def test_nome_novo_sem_diferenciar_caixa(self, monkeypatch, tmp_path, nome):
        monkeypatch.setenv(nome, str(tmp_path / "novo"))
        assert constants.CacheSettings().cache_dir == tmp_path / "novo"

    def test_nome_novo_em_minusculas_vence_o_antigo(self, monkeypatch, tmp_path):
        monkeypatch.setenv("agrobr_cache_dir", str(tmp_path / "novo"))
        monkeypatch.setenv("AGROBR_CACHE_CACHE_DIR", str(tmp_path / "antigo"))
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            assert constants.CacheSettings().cache_dir == tmp_path / "novo"

    def test_nome_novo_em_minusculas_vazio_com_o_antigo_vale_o_padrao(self, monkeypatch, tmp_path):
        monkeypatch.setenv("agrobr_cache_dir", "")
        monkeypatch.setenv("AGROBR_CACHE_CACHE_DIR", str(tmp_path / "antigo"))
        assert constants.CacheSettings().cache_dir == Path.home() / ".agrobr" / "cache"

    def test_nome_novo_no_env_file(self, tmp_path):
        arquivo = tmp_path / ".env"
        arquivo.write_text(f"AGROBR_CACHE_DIR={tmp_path / 'dotenv'}\n", encoding="utf-8")
        assert constants.CacheSettings(_env_file=arquivo).cache_dir == tmp_path / "dotenv"

    def test_nome_novo_no_secrets_dir(self, tmp_path):
        (tmp_path / "AGROBR_CACHE_DIR").write_text(str(tmp_path / "segredo"), encoding="utf-8")
        assert constants.CacheSettings(_secrets_dir=tmp_path).cache_dir == tmp_path / "segredo"


@pytest.mark.parametrize(
    ("valor", "esperado"),
    [
        ("1", True),
        ("true", True),
        ("True", True),
        (" YES ", True),
        ("on", True),
        ("0", False),
        ("false", False),
        ("no", False),
        ("off", False),
        ("", False),
    ],
)
def test_env_flag_aceita_os_booleanos_do_pydantic(monkeypatch, valor, esperado):
    monkeypatch.setenv("AGROBR_TESTE_FLAG", valor)
    assert constants.env_flag("AGROBR_TESTE_FLAG") is esperado


def test_env_flag_ausente_e_falso_e_valor_invalido_e_recusado(monkeypatch):
    monkeypatch.delenv("AGROBR_TESTE_FLAG", raising=False)
    assert constants.env_flag("AGROBR_TESTE_FLAG") is False
    monkeypatch.setenv("AGROBR_TESTE_FLAG", "sim")
    with pytest.raises(InvalidParameterError, match=r"AGROBR_TESTE_FLAG='sim'.*1, true, yes"):
        constants.env_flag("AGROBR_TESTE_FLAG")
