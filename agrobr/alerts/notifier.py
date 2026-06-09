from __future__ import annotations

import asyncio
import json
from datetime import datetime
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


class AlertCategory(StrEnum):
    SOFT_BLOCK = "soft_block"
    SOURCE_DOWN = "source_down"
    PARSE_ERROR = "parse_error"
    LAYOUT_CHANGE = "layout_change"
    ANOMALY = "anomaly"
    API_KEY_MISSING = "api_key_missing"
    SLOW = "slow"


CATEGORY_LABELS: dict[str, str] = {
    AlertCategory.SOFT_BLOCK: "IP Blocked (Cloudflare)",
    AlertCategory.SOURCE_DOWN: "Source Down",
    AlertCategory.PARSE_ERROR: "Parse Error",
    AlertCategory.LAYOUT_CHANGE: "Layout Change",
    AlertCategory.ANOMALY: "Anomaly Detected",
    AlertCategory.API_KEY_MISSING: "API Key Missing",
    AlertCategory.SLOW: "Slow Response",
}

_SLACK_EMOJI: dict[str, str] = {
    "info": "info",
    "warning": "warning",
    "critical": "rotating_light",
}

_SLACK_COLOR: dict[str, str] = {
    "info": "#36a64f",
    "warning": "#ff9800",
    "critical": "#dc3545",
}

_DISCORD_EMOJI: dict[str, str] = {
    "info": "info",
    "warning": "warning",
    "critical": "rotating_light",
}

_DISCORD_COLOR: dict[str, int] = {
    "info": 0x36A64F,
    "warning": 0xFF9800,
    "critical": 0xDC3545,
}

_DISCORD_COLOR_RECOVERY = 0x36A64F
_DISCORD_COLOR_SOFT_BLOCK = 0x7289DA


async def send_alert(
    level: AlertLevel | str,
    title: str,
    details: dict[str, Any],
    source: str | None = None,
    *,
    category: str | None = None,
    is_recovery: bool = False,
    affected_datasets: list[str] | None = None,
    consecutive_failures: int | None = None,
    last_success_at: datetime | None = None,
) -> None:
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
        tasks.append(
            _send_discord(
                settings.discord_webhook,
                level,
                title,
                details,
                source,
                category=category,
                is_recovery=is_recovery,
                char_limit=settings.discord_embed_char_limit,
                affected_datasets=affected_datasets,
                consecutive_failures=consecutive_failures,
                last_success_at=last_success_at,
            )
        )

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
    emoji = _SLACK_EMOJI[level.value]
    color = _SLACK_COLOR[level.value]

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
    *,
    category: str | None = None,
    is_recovery: bool = False,
    char_limit: int = 3900,
    affected_datasets: list[str] | None = None,
    consecutive_failures: int | None = None,
    last_success_at: datetime | None = None,
) -> None:
    if category == AlertCategory.SOFT_BLOCK:
        emoji = "shield"
        color = _DISCORD_COLOR_SOFT_BLOCK
    else:
        emoji = _DISCORD_EMOJI[level.value]
        color = _DISCORD_COLOR[level.value]

    if is_recovery:
        color = _DISCORD_COLOR_RECOVERY

    detail_text = json.dumps(details, indent=2, default=str)[:char_limit]

    embed: dict[str, Any] = {
        "title": f":{emoji}: {title}",
        "color": color,
        "fields": [],
    }

    if source:
        embed["fields"].append({"name": "Source", "value": source, "inline": True})
        embed["fields"].append({"name": "Level", "value": level.value.upper(), "inline": True})

    if category and category in CATEGORY_LABELS:
        embed["fields"].append(
            {"name": "Category", "value": CATEGORY_LABELS[category], "inline": True}
        )

    if affected_datasets:
        embed["fields"].append(
            {
                "name": "Affected Datasets",
                "value": ", ".join(affected_datasets),
                "inline": False,
            }
        )

    if consecutive_failures and consecutive_failures > 0:
        embed["fields"].append(
            {
                "name": "Consecutive Failures",
                "value": str(consecutive_failures),
                "inline": True,
            }
        )

    if last_success_at:
        embed["fields"].append(
            {
                "name": "Last Success",
                "value": last_success_at.isoformat(),
                "inline": True,
            }
        )

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
