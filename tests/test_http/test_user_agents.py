from __future__ import annotations

from agrobr.http import user_agents
from agrobr.http.user_agents import USER_AGENT_POOL, UserAgentRotator


def test_rotacao_segue_o_pool_a_partir_do_sorteio(monkeypatch):
    monkeypatch.setattr(UserAgentRotator, "_counters", {})
    monkeypatch.setattr(user_agents.random, "randint", lambda _inicio, _fim: 0)
    assert [UserAgentRotator.get("fonte") for _ in range(3)] == list(USER_AGENT_POOL[:3])
    assert UserAgentRotator.get_random() in USER_AGENT_POOL
