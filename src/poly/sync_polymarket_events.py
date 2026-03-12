from pathlib import Path
import sys
from typing import Any, Dict, List

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from poly.common import (
    as_int,
    build_event_row,
    fetch_categories_by_flag,
    fetch_existing_event_state,
    fetch_polymarket_events_by_tag_slug,
    require_env,
    require_env_any,
    require_env_int_any,
    upsert_rows,
)


def collect_all_events(
    polymarket_events_url: str,
    events_page_size: int,
    active_categories: List[Dict[str, Any]],
    existing_events_by_id: Dict[int, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    selected_events_by_id: Dict[int, Dict[str, Any]] = {}

    for category in active_categories:
        category_events = fetch_polymarket_events_by_tag_slug(
            polymarket_events_url,
            events_page_size,
            category["slug"],
        )
        for event in category_events:
            event_id = as_int(event.get("id"))
            existing_row = existing_events_by_id.get(event_id) if event_id is not None else None
            row = build_event_row(
                event,
                category,
                existing_row=existing_row,
            )
            if row is None:
                continue
            if existing_row is None and row["closed"]:
                continue

            existing = selected_events_by_id.get(row["id"])
            if existing is None or row["volume"] > existing["volume"]:
                selected_events_by_id[row["id"]] = row

    all_events = list(selected_events_by_id.values())
    all_events.sort(key=lambda row: row["volume"], reverse=True)
    return all_events


def main() -> None:
    load_dotenv()

    polymarket_events_url = require_env_any(
        ["POLY_GAMMA_EVENTS_URL", "POLYMARKET_EVENTS_URL"]
    )
    supabase_url = require_env("SUPABASE_URL").rstrip("/")
    supabase_key = require_env("SUPABASE_KEY")
    supabase_categories_table = require_env_any(
        ["POLY_SUPABASE_CATEGORIES_TABLE", "SUPABASE_POLY_CATEGORIES_TABLE"]
    )
    supabase_events_table = require_env_any(
        ["POLY_SUPABASE_EVENTS_TABLE", "SUPABASE_POLY_EVENTS_TABLE", "POLY_EVENTS_TABLE"]
    )
    events_page_size = require_env_int_any(["POLY_EVENTS_PAGE_SIZE"], min_value=1)

    active_categories = fetch_categories_by_flag(
        supabase_url,
        supabase_key,
        supabase_categories_table,
        flag_column="activeForTrendlum",
    )
    existing_events_by_id = fetch_existing_event_state(
        supabase_url,
        supabase_key,
        supabase_events_table,
    )
    all_events = collect_all_events(
        polymarket_events_url,
        events_page_size,
        active_categories,
        existing_events_by_id,
    )
    upsert_rows(
        supabase_url,
        supabase_key,
        supabase_events_table,
        all_events,
        on_conflict="id",
        batch_size=50,
        timeout=90,
        max_retries=4,
    )

    print(f"Categorias activas encontradas: {len(active_categories)}")
    print(f"Eventos sincronizados en Supabase: {len(all_events)}")


if __name__ == "__main__":
    main()
