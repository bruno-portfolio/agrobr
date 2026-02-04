"""Alertas multi-canal - Slack, Discord, Email."""

from __future__ import annotations

from agrobr.alerts.notifier import send_alert, AlertLevel

__all__ = ["send_alert", "AlertLevel"]
