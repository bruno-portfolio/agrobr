"""Pool rotativo de User-Agents."""

from __future__ import annotations

import random
from collections.abc import Sequence

USER_AGENT_POOL: Sequence[str] = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
)

DEFAULT_HEADERS: dict[str, str] = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
}


class UserAgentRotator:
    """Rotaciona User-Agents de forma determinística por fonte."""

    _counters: dict[str, int] = {}

    @classmethod
    def get(cls, source: str | None = None) -> str:
        """Retorna próximo User-Agent do pool."""
        key = source or "default"

        if key not in cls._counters:
            cls._counters[key] = random.randint(0, len(USER_AGENT_POOL) - 1)

        ua = USER_AGENT_POOL[cls._counters[key] % len(USER_AGENT_POOL)]
        cls._counters[key] += 1

        return ua

    @classmethod
    def get_random(cls) -> str:
        """Retorna User-Agent aleatório."""
        return random.choice(USER_AGENT_POOL)

    @classmethod
    def get_headers(cls, source: str | None = None) -> dict[str, str]:
        """Retorna headers completos incluindo User-Agent."""
        headers = DEFAULT_HEADERS.copy()
        headers["User-Agent"] = cls.get(source)
        return headers

    @classmethod
    def reset(cls) -> None:
        """Reseta contadores."""
        cls._counters.clear()
