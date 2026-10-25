"""Export the day's selected items to Fikri HQ (Physis News tab).

Writes data/hq-items.json and, when HQ_* env vars are set, upserts the items
into the HQ Supabase database through the token-protected `ingest_news` RPC.
"""

import asyncio
import json
import logging
import os
import re
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


_OG_RE = [
    re.compile(r'<meta[^>]+property=["\']og:image(?::secure_url)?["\'][^>]+content=["\']([^"\']+)', re.I),
    re.compile(r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:image["\']', re.I),
    re.compile(r'<meta[^>]+name=["\']twitter:image["\'][^>]+content=["\']([^"\']+)', re.I),
]


async def _og_image(client: httpx.AsyncClient, url: str) -> str | None:
    """Fetch a page's preview image (og:image). Best effort, never raises."""
    try:
        r = await client.get(url, headers={"User-Agent": "Mozilla/5.0 (compatible; PhysisNews/1.0)"}, follow_redirects=True)
        if r.status_code >= 400:
            return None
        head = r.text[:200_000]
        for rx in _OG_RE:
            m = rx.search(head)
            if m:
                img = m.group(1).strip().replace("&amp;", "&")
                if img.startswith("//"):
                    img = "https:" + img
                return img if img.startswith("https://") else None
    except Exception:
        return None
    return None


async def add_images(payload: List[Dict[str, Any]]) -> None:
    async with httpx.AsyncClient(timeout=8) as client:
        sem = asyncio.Semaphore(6)

        async def one(row):
            async with sem:
                row["image_url"] = await _og_image(client, row["url"])

        await asyncio.gather(*(one(r) for r in payload))
    logger.info("HQ images: %s of %s found", sum(1 for r in payload if r.get("image_url")), len(payload))


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
    await add_images(payload)
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


async def report_to_hq(report: Dict[str, Any]) -> None:
    """Send run diagnostics to HQ (shown as the News agent's status)."""
    url, anon, token = os.environ.get("HQ_SUPABASE_URL"), os.environ.get("HQ_SUPABASE_ANON"), os.environ.get("HQ_NEWS_TOKEN")
    if not (url and anon and token):
        return
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            await client.post(
                f"{url.rstrip('/')}/rest/v1/rpc/news_report",
                headers={"apikey": anon, "Authorization": f"Bearer {anon}", "Content-Type": "application/json"},
                json={"p_token": token, "p_report": report},
            )
    except Exception as exc:  # diagnostics must never fail the run
        logger.warning("HQ report failed: %s", exc)


def build_report(all_items, capped_items, analyzed_items, selected_items, fetch_report) -> Dict[str, Any]:
    def feed(it):
        return str((it.metadata or {}).get("feed_name") or it.source_type.value)
    per: Dict[str, Dict[str, Any]] = defaultdict(lambda: {"fetched": 0, "analysed": 0, "failed": 0, "selected": 0, "top": 0.0})
    for it in all_items:
        per[feed(it)]["fetched"] += 1
    for it in analyzed_items:
        a = it.processing.analysis if it.processing else None
        if a and a.score is not None:
            per[feed(it)]["analysed"] += 1
            per[feed(it)]["top"] = max(per[feed(it)]["top"], float(a.score))
        else:
            per[feed(it)]["failed"] += 1
    for it in selected_items:
        per[feed(it)]["selected"] += 1
    errors: Dict[str, str] = {}
    if fetch_report:
        for o in fetch_report.outcomes:
            if o.error:
                errors[o.source_name] = o.error[:200]
            for k, v in (o.failures or {}).items():
                errors[k] = str(v)[:200]
    analysed_ok = sum(v["analysed"] for v in per.values())
    return {
        "fetched": len(all_items), "capped": len(capped_items), "analyzed_ok": analysed_ok,
        "analyzed_failed": len(analyzed_items) - analysed_ok, "selected": len(selected_items),
        "feed_errors": len(errors), "errors": errors, "per_feed": per,
    }
