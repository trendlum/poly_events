from pathlib import Path
import re
import sys
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv
from requests.exceptions import HTTPError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from poly.common import (
    require_env,
    require_env_any,
    require_env_int_any,
    supabase_headers,
    upsert_rows,
)


def normalize_text(value: Any) -> str:
    text = str(value or "").lower()
    text = re.sub(r"[^a-z0-9\s]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def text_matches_keyword(text: str, keyword: str, match_type: str) -> bool:
    normalized_text = normalize_text(text)
    normalized_keyword = normalize_text(keyword)
    if not normalized_text or not normalized_keyword:
        return False
    if match_type == "exact":
        return normalized_keyword in normalized_text.split(" ")
    return normalized_keyword in normalized_text


def fetch_rows(
    supabase_url: str,
    supabase_key: str,
    table: str,
    select: str,
    *,
    filters: Optional[List[str]] = None,
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    query_parts = [f"select={select}"]
    if filters:
        query_parts.extend(filters)
    if limit is not None:
        query_parts.append(f"limit={limit}")
    endpoint = f"{supabase_url}/rest/v1/{table}?{'&'.join(query_parts)}"

    response = requests.get(endpoint, headers=supabase_headers(supabase_key), timeout=30)
    try:
        response.raise_for_status()
    except HTTPError as exc:
        if response.status_code == 404:
            raise ValueError(f"No existe la tabla '{table}' en Supabase.") from exc
        raise

    rows = response.json()
    if not isinstance(rows, list):
        raise ValueError(f"Respuesta inesperada al leer la tabla '{table}'.")
    return [row for row in rows if isinstance(row, dict)]


def delete_existing_enrichments(
    supabase_url: str,
    supabase_key: str,
    table: str,
    event_ids: List[int],
) -> None:
    if not event_ids:
        return
    joined_ids = ",".join(str(event_id) for event_id in event_ids)
    endpoint = f"{supabase_url}/rest/v1/{table}?poly_event_id=in.({joined_ids})"
    headers = supabase_headers(supabase_key)
    headers["Prefer"] = "return=minimal"
    response = requests.delete(endpoint, headers=headers, timeout=30)
    try:
        response.raise_for_status()
    except HTTPError as exc:
        if response.status_code == 404:
            raise ValueError(f"No existe la tabla '{table}' en Supabase.") from exc
        raise


def match_taxonomy_keywords(
    event_row: Dict[str, Any],
    taxonomy_keywords: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    search_text = str(event_row.get("search_text") or "")
    matches: List[Dict[str, Any]] = []
    for keyword_row in taxonomy_keywords:
        if not text_matches_keyword(
            search_text,
            str(keyword_row.get("keyword") or ""),
            str(keyword_row.get("match_type") or "contains"),
        ):
            continue
        matches.append(
            {
                "poly_event_id": event_row["id"],
                "match_type": "taxonomy_keyword",
                "taxonomy_category_id": keyword_row.get("category_id"),
                "taxonomy_keyword_id": keyword_row.get("id"),
                "entity_keyword_id": None,
                "canonical_name": None,
                "entity_type": None,
                "matched_keyword": keyword_row.get("keyword"),
                "keyword_match_type": keyword_row.get("match_type"),
                "weight": keyword_row.get("weight") or 0,
            }
        )
    return matches


def match_entity_keywords(
    event_row: Dict[str, Any],
    entity_keywords: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    search_text = str(event_row.get("search_text") or "")
    matches: List[Dict[str, Any]] = []
    for entity_row in entity_keywords:
        if not text_matches_keyword(
            search_text,
            str(entity_row.get("keyword") or ""),
            str(entity_row.get("match_type") or "contains"),
        ):
            continue
        matches.append(
            {
                "poly_event_id": event_row["id"],
                "match_type": "entity_keyword",
                "taxonomy_category_id": None,
                "taxonomy_keyword_id": None,
                "entity_keyword_id": entity_row.get("id"),
                "canonical_name": entity_row.get("canonical_name"),
                "entity_type": entity_row.get("entity_type"),
                "matched_keyword": entity_row.get("keyword"),
                "keyword_match_type": entity_row.get("match_type"),
                "weight": entity_row.get("weight") or 0,
            }
        )
    return matches


def build_enrichment_rows(
    pending_events: List[Dict[str, Any]],
    taxonomy_keywords: List[Dict[str, Any]],
    entity_keywords: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    deduped_rows: Dict[tuple[int, str, str], Dict[str, Any]] = {}
    for event_row in pending_events:
        candidate_rows = match_taxonomy_keywords(event_row, taxonomy_keywords)
        candidate_rows.extend(match_entity_keywords(event_row, entity_keywords))
        for row in candidate_rows:
            key = (
                int(row["poly_event_id"]),
                str(row["match_type"]),
                str(row.get("matched_keyword") or "").strip(),
            )
            existing = deduped_rows.get(key)
            if existing is None:
                deduped_rows[key] = row
                continue

            # Keep one row per upsert conflict key to avoid PostgREST bulk upsert failures.
            existing["weight"] = max(existing.get("weight") or 0, row.get("weight") or 0)
            if existing.get("taxonomy_keyword_id") is None:
                existing["taxonomy_keyword_id"] = row.get("taxonomy_keyword_id")
            if existing.get("entity_keyword_id") is None:
                existing["entity_keyword_id"] = row.get("entity_keyword_id")
            if existing.get("taxonomy_category_id") is None:
                existing["taxonomy_category_id"] = row.get("taxonomy_category_id")
            if existing.get("canonical_name") is None:
                existing["canonical_name"] = row.get("canonical_name")
            if existing.get("entity_type") is None:
                existing["entity_type"] = row.get("entity_type")
            if existing.get("keyword_match_type") is None:
                existing["keyword_match_type"] = row.get("keyword_match_type")
    return list(deduped_rows.values())


def process_pending_batch(
    supabase_url: str,
    supabase_key: str,
    events_table: str,
    events_enriched_table: str,
    pending_events: List[Dict[str, Any]],
    taxonomy_keywords: List[Dict[str, Any]],
    entity_keywords: List[Dict[str, Any]],
) -> int:
    if not pending_events:
        return 0

    event_ids = [int(row["id"]) for row in pending_events]
    enrichment_rows = build_enrichment_rows(
        pending_events,
        taxonomy_keywords,
        entity_keywords,
    )

    delete_existing_enrichments(
        supabase_url,
        supabase_key,
        events_enriched_table,
        event_ids,
    )
    upsert_rows(
        supabase_url,
        supabase_key,
        events_enriched_table,
        enrichment_rows,
        on_conflict="poly_event_id,match_type,matched_keyword",
        batch_size=25,
        timeout=90,
        max_retries=4,
    )
    mark_events_as_enriched(
        supabase_url,
        supabase_key,
        events_table,
        event_ids,
    )

    return len(enrichment_rows)


def mark_events_as_enriched(
    supabase_url: str,
    supabase_key: str,
    events_table: str,
    event_ids: List[int],
) -> None:
    if not event_ids:
        return
    endpoint = (
        f"{supabase_url}/rest/v1/{events_table}"
        f"?id=in.({','.join(str(event_id) for event_id in event_ids)})"
    )
    headers = supabase_headers(supabase_key, include_json_content_type=True)
    headers["Prefer"] = "return=minimal"
    response = requests.patch(
        endpoint,
        headers=headers,
        json={"is_enriched": True},
        timeout=30,
    )
    try:
        response.raise_for_status()
    except HTTPError as exc:
        if response.status_code == 404:
            raise ValueError(f"No existe la tabla '{events_table}' en Supabase.") from exc
        raise


def main() -> None:
    load_dotenv()

    supabase_url = require_env("SUPABASE_URL").rstrip("/")
    supabase_key = require_env("SUPABASE_KEY")
    events_table = require_env_any(
        ["POLY_SUPABASE_EVENTS_TABLE", "SUPABASE_POLY_EVENTS_TABLE", "POLY_EVENTS_TABLE"]
    )
    events_enriched_table = require_env_any(
        [
            "POLY_SUPABASE_EVENTS_ENRICHED_TABLE",
            "SUPABASE_POLY_EVENTS_ENRICHED_TABLE",
            "POLY_EVENTS_ENRICHED_TABLE",
        ]
    )
    taxonomy_keywords_table = require_env_any(
        ["POLY_EVENT_TAXONOMY_KEYWORDS_TABLE", "EVENT_TAXONOMY_KEYWORDS_TABLE"]
    )
    entity_keywords_table = require_env_any(
        ["POLY_EVENT_ENTITY_KEYWORDS_TABLE", "EVENT_ENTITY_KEYWORDS_TABLE"]
    )
    batch_size = require_env_int_any(["POLY_EVENTS_ENRICH_BATCH_SIZE"], min_value=1)

    taxonomy_keywords = fetch_rows(
        supabase_url,
        supabase_key,
        taxonomy_keywords_table,
        "id,category_id,keyword,match_type,weight",
        filters=["active=eq.true"],
    )
    entity_keywords = fetch_rows(
        supabase_url,
        supabase_key,
        entity_keywords_table,
        "id,entity_type,canonical_name,keyword,match_type,weight",
        filters=["active=eq.true"],
    )

    total_events_processed = 0
    total_enrichment_rows = 0

    while True:
        pending_events = fetch_rows(
            supabase_url,
            supabase_key,
            events_table,
            "id,title,search_text",
            filters=["is_enriched=eq.false", "closed=eq.false"],
            limit=batch_size,
        )
        if not pending_events:
            break

        inserted_rows = process_pending_batch(
            supabase_url,
            supabase_key,
            events_table,
            events_enriched_table,
            pending_events,
            taxonomy_keywords,
            entity_keywords,
        )
        total_events_processed += len(pending_events)
        total_enrichment_rows += inserted_rows
        print(
            "Lote procesado: "
            f"{len(pending_events)} eventos, {inserted_rows} filas de enrichment"
        )

    print(f"Eventos enriquecidos procesados: {total_events_processed}")
    print(f"Filas de enrichment insertadas: {total_enrichment_rows}")


if __name__ == "__main__":
    main()
