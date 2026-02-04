"""Dispatcher de alertas multi-canal."""

from __future__ import annotations

import asyncio
import json
from enum import StrEnum
from typing import Any

import httpx
import structlog

from agrobr import constants

logger = structlog.get_logger()


class AlertLevel(StrEnum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


async def send_alert(
    level: AlertLevel | str,
    title: str,
    details: dict[str, Any],
    source: str | None = None,
) -> None:
    """Envia alerta para todos os canais configurados."""
    settings = constants.AlertSettings()

    if not settings.enabled:
        logger.debug("alerts_disabled", title=title)
        return

    if isinstance(level, str):
        level = AlertLevel(level)

    tasks = []

    if settings.slack_webhook:
        tasks.append(_send_slack(settings.slack_webhook, level, title, details, source))

    if settings.discord_webhook:
        tasks.append(_send_discord(settings.discord_webhook, level, title, details, source))

    if settings.sendgrid_api_key and settings.email_to:
        tasks.append(_send_email(settings, level, title, details, source))

    if tasks:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error("alert_send_failed", channel=i, error=str(result))
    else:
        logger.warning("no_alert_channels_configured", title=title)


async def _send_slack(
    webhook: str,
    level: AlertLevel,
    title: str,
    details: dict[str, Any],
    source: str | None,
) -> None:
    emoji = {"info": "info", "warning": "warning", "critical": "rotating_light"}[level.value]
    color = {"info": "#36a64f", "warning": "#ff9800", "critical": "#dc3545"}[level.value]

    blocks: list[dict[str, Any]] = [
        {"type": "header", "text": {"type": "plain_text", "text": f":{emoji}: {title}"}},
    ]

    if source:
        blocks.append(
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Source:* {source}"},
                    {"type": "mrkdwn", "text": f"*Level:* {level.value.upper()}"},
                ],
            }
        )

    if details:
        detail_text = json.dumps(details, indent=2, default=str)[:2900]
        blocks.append(
            {"type": "section", "text": {"type": "mrkdwn", "text": f"```{detail_text}```"}}
        )

    payload = {"attachments": [{"color": color, "blocks": blocks}]}

    async with httpx.AsyncClient() as client:
        response = await client.post(webhook, json=payload, timeout=10.0)
        response.raise_for_status()

    logger.info("alert_sent", channel="slack", level=level.value, title=title)


async def _send_discord(
    webhook: str,
    level: AlertLevel,
    title: str,
    details: dict[str, Any],
    source: str | None,
) -> None:
    emoji = {"info": "info", "warning": "warning", "critical": "rotating_light"}[level.value]
    color = {"info": 0x36A64F, "warning": 0xFF9800, "critical": 0xDC3545}[level.value]

    detail_text = json.dumps(details, indent=2, default=str)[:1900]

    embed: dict[str, Any] = {
        "title": f":{emoji}: {title}",
        "color": color,
        "fields": [],
    }

    if source:
        embed["fields"].append({"name": "Source", "value": source, "inline": True})
        embed["fields"].append({"name": "Level", "value": level.value.upper(), "inline": True})

    if details:
        embed["description"] = f"```json\n{detail_text}\n```"

    payload = {"embeds": [embed]}

    async with httpx.AsyncClient() as client:
        response = await client.post(webhook, json=payload, timeout=10.0)
        response.raise_for_status()

    logger.info("alert_sent", channel="discord", level=level.value, title=title)


async def _send_email(
    settings: constants.AlertSettings,
    level: AlertLevel,
    title: str,
    details: dict[str, Any],
    source: str | None,
) -> None:
    detail_text = json.dumps(details, indent=2, default=str)

    html_content = f"""
    <h2>{title}</h2>
    <p><strong>Level:</strong> {level.value.upper()}</p>
    {"<p><strong>Source:</strong> " + source + "</p>" if source else ""}
    <h3>Details</h3>
    <pre>{detail_text}</pre>
    """

    payload = {
        "personalizations": [{"to": [{"email": e} for e in settings.email_to]}],
        "from": {"email": settings.email_from},
        "subject": f"[agrobr {level.value.upper()}] {title}",
        "content": [{"type": "text/html", "value": html_content}],
    }

    async with httpx.AsyncClient() as client:
        response = await client.post(
            "https://api.sendgrid.com/v3/mail/send",
            json=payload,
            headers={"Authorization": f"Bearer {settings.sendgrid_api_key}"},
            timeout=10.0,
        )
        response.raise_for_status()

    logger.info("alert_sent", channel="email", level=level.value, title=title)
