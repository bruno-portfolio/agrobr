from __future__ import annotations

import importlib
import pkgutil
import subprocess
import sys
from pathlib import Path

import agrobr

repository = Path(__file__).resolve().parent.parent
package = Path(agrobr.__file__).resolve()
if package.is_relative_to(repository):
    raise RuntimeError(f"Import veio do checkout, não do pacote instalado: {package}")

for path in package.parent.rglob("*"):
    if path.name in {".claude", ".env"} or path.name.startswith(".env."):
        raise RuntimeError(
            f"Configuração local incluída no pacote: {path.relative_to(package.parent)}"
        )

for module in pkgutil.walk_packages(agrobr.__path__, prefix="agrobr."):
    if module.name != "agrobr.__main__":
        importlib.import_module(module.name)

for args in [
    ["--version"],
    ["--help"],
    ["cepea", "--help"],
    ["conab", "--help"],
    ["ibge", "--help"],
    ["snapshot", "--help"],
    ["doctor", "--help"],
]:
    subprocess.run([sys.executable, "-I", "-m", "agrobr", *args], check=True)
print(f"Installed package verified: {package}")
