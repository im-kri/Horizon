"""Export the day's selected items to Fikri HQ (Physis News tab).

Writes data/hq-items.json and, when HQ_* env vars are set, upserts the items
into the HQ Supabase database through the token-protected `ingest_news` RPC.
"""

import json
import logging
import os
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List

import httpx

from ..models import ContentItem

logger = logging.getLogger(__name__)

SECTIONS = {"world", "economy", "tech", "indonesia"}
PROFILE_SECTION = {"hq-world": "world", "hq-economy": "economy", "hq-tech": "tech"}


def cap_per_source(items: List[ContentItem]) -> List[ContentItem]:
    """Keep the newest N items per feed so free AI quotas are not exhausted."""
    limit = int(os.environ.get("HQ_MAX_PER_FEED", "8") or 8)
    groups: Dict[str, List[ContentItem]] = defaultdict(list)
    for it in items:
        key = str((it.metadata or {}).get("feed_name") or it.source_type.value)
        groups[key].append(it)
    kept: List[ContentItem] = []
    for group in groups.values():
        group.sort(key=lambda i: i.published_at, reverse=True)
        kept.extend(group[:limit])
    return kept


def _section(item: ContentItem) -> str:
    cat = str((item.metadata or {}).get("category") or "").lower()
    if cat in SECTIONS:
        return cat
    profile = item.processing.classification.profile if item.processing else None
    return PROFILE_SECTION.get(profile or "", "world")


def _block(item: ContentItem, block_id: str) -> str:
    if not item.processing:
        return ""
    arts = item.processing.artifacts or {}
    art = arts.get("en") or next(iter(arts.values()), None)
    if not art:
        return ""
    for b in art.blocks:
        if b.id == block_id:
            return b.content.strip()
    return ""


def _title(item: ContentItem) -> str:
    if item.processing and item.processing.artifacts:
        arts = item.processing.artifacts
        art = arts.get("en") or next(iter(arts.values()))
        if art and art.title:
            return art.title
    return item.title


def build_payload(items: List[ContentItem]) -> List[Dict[str, Any]]:
    out = []
    for it in items:
        analysis = it.processing.analysis if it.processing else None
        out.append({
            "url": str(it.url),
            "title": _title(it),
            "source": str((it.metadata or {}).get("feed_name") or it.source_type.value),
            "section": _section(it),
            "score": analysis.score if analysis and analysis.score is not None else None,
            "summary": _block(it, "summary") or (analysis.summary if analysis else None),
            "why_it_matters": _block(it, "why") or (analysis.reason if analysis else None),
            "published_at": it.published_at.isoformat() if it.published_at else None,
        })
    return out


async def export_to_hq(items: List[ContentItem], data_dir: str = "data") -> int:
    payload = build_payload(items)
    try:
        Path(data_dir).mkdir(parents=True, exist_ok=True)
        (Path(data_dir) / "hq-items.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except OSError as exc:
        logger.warning("Could not write hq-items.json: %s", exc)

    url = os.environ.get("HQ_SUPABASE_URL")
    anon = os.environ.get("HQ_SUPABASE_ANON")
    token = os.environ.get("HQ_NEWS_TOKEN")
    if not (url and anon and token and payload):
        logger.info("HQ export skipped (missing env or no items)")
        return 0
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(
            f"{url.rstrip('/')}/rest/v1/rpc/ingest_news",
            headers={"apikey": anon, "Authorization": f"Bearer {anon}", "Content-Type": "application/json"},
            json={"p_token": token, "p_items": payload},
        )
        if r.status_code >= 300:
            logger.error("HQ export failed: %s %s", r.status_code, r.text[:300])
            raise RuntimeError(f"HQ export failed with HTTP {r.status_code}")
        n = r.json()
    logger.info("HQ export: %s items upserted", n)
    return int(n or 0)
