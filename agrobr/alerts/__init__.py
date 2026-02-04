"""Alertas multi-canal - Slack, Discord, Email."""

from __future__ import annotations

from agrobr.alerts.notifier import AlertLevel, send_alert

__all__ = ["send_alert", "AlertLevel"]
