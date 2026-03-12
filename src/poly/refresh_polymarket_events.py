from pathlib import Path
import sys
from typing import Any, Dict, List

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from poly.common import (
    as_int,
    build_event_row,
    fetch_existing_event_state,
    fetch_polymarket_events_by_tag_slug,
    require_env,
    require_env_any,
    require_env_int_any,
    upsert_rows,
)


def build_category_from_existing_row(existing_row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": existing_row.get("category_id"),
        "label": str(existing_row.get("category_label") or "").strip(),
        "slug": str(existing_row.get("tag_slug") or "").strip(),
    }


def collect_existing_event_updates(
    polymarket_events_url: str,
    events_page_size: int,
    existing_events_by_id: Dict[int, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    selected_events_by_id: Dict[int, Dict[str, Any]] = {}
    tracked_slugs = sorted(
        {
            str(row.get("tag_slug") or "").strip()
            for row in existing_events_by_id.values()
            if str(row.get("tag_slug") or "").strip()
        }
    )

    for tag_slug in tracked_slugs:
        category_events = fetch_polymarket_events_by_tag_slug(
            polymarket_events_url,
            events_page_size,
            tag_slug,
        )
        for event in category_events:
            event_id = as_int(event.get("id"))
            if event_id is None:
                continue
            existing_row = existing_events_by_id.get(event_id)
            if existing_row is None:
                continue

            row = build_event_row(
                event,
                build_category_from_existing_row(existing_row),
                existing_row=existing_row,
            )
            if row is None:
                continue

            existing = selected_events_by_id.get(row["id"])
            if existing is None or row["volume"] > existing["volume"]:
                selected_events_by_id[row["id"]] = row

    updated_events = list(selected_events_by_id.values())
    updated_events.sort(key=lambda row: row["volume"], reverse=True)
    return updated_events


def count_invalidated_events(
    updated_rows: List[Dict[str, Any]],
    existing_events_by_id: Dict[int, Dict[str, Any]],
) -> int:
    total = 0
    for row in updated_rows:
        existing_row = existing_events_by_id.get(int(row["id"]))
        if not existing_row:
            continue
        if bool(existing_row.get("is_enriched")) and not bool(row.get("is_enriched")):
            total += 1
    return total


def main() -> None:
    load_dotenv()

    polymarket_events_url = require_env_any(
        ["POLY_GAMMA_EVENTS_URL", "POLYMARKET_EVENTS_URL"]
    )
    supabase_url = require_env("SUPABASE_URL").rstrip("/")
    supabase_key = require_env("SUPABASE_KEY")
    supabase_events_table = require_env_any(
        ["POLY_SUPABASE_EVENTS_TABLE", "SUPABASE_POLY_EVENTS_TABLE", "POLY_EVENTS_TABLE"]
    )
    events_page_size = require_env_int_any(["POLY_EVENTS_PAGE_SIZE"], min_value=1)

    existing_events_by_id = fetch_existing_event_state(
        supabase_url,
        supabase_key,
        supabase_events_table,
    )
    updated_rows = collect_existing_event_updates(
        polymarket_events_url,
        events_page_size,
        existing_events_by_id,
    )
    invalidated_events = count_invalidated_events(updated_rows, existing_events_by_id)

    upsert_rows(
        supabase_url,
        supabase_key,
        supabase_events_table,
        updated_rows,
        on_conflict="id",
        batch_size=50,
        timeout=90,
        max_retries=4,
    )

    print(f"Eventos existentes revisados: {len(existing_events_by_id)}")
    print(f"Eventos actualizados desde Polymarket: {len(updated_rows)}")
    print(f"Eventos marcados de nuevo para enrichment: {invalidated_events}")


if __name__ == "__main__":
    main()
