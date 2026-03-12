from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import sys
from typing import Any, Dict, List, Set

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from poly.common import (
    as_int,
    build_event_row,
    fetch_categories_by_flag,
    fetch_existing_event_ids,
    fetch_polymarket_events_by_tag_slug,
    require_env,
    require_env_any,
    require_env_int_any,
    upsert_rows,
)


def require_env_int_any_or_default(
    names: List[str],
    *,
    default: int,
    min_value: int = 1,
) -> int:
    for name in names:
        raw = os.getenv(name, "").strip()
        if not raw:
            continue
        try:
            value = int(raw)
        except ValueError as exc:
            joined = ", ".join(names)
            raise ValueError(f"Valor entero invalido para {joined}: {raw!r}") from exc
        if value < min_value:
            joined = ", ".join(names)
            raise ValueError(f"El valor de {joined} debe ser >= {min_value}.")
        return value
    return default


def fetch_new_events_for_category(
    polymarket_events_url: str,
    events_page_size: int,
    category: Dict[str, Any],
    existing_event_ids: Set[int],
) -> Dict[int, Dict[str, Any]]:
    selected_events_by_id: Dict[int, Dict[str, Any]] = {}
    category_events = fetch_polymarket_events_by_tag_slug(
        polymarket_events_url,
        events_page_size,
        category["slug"],
    )
    for event in category_events:
        event_id = as_int(event.get("id"))
        if event_id is None or event_id in existing_event_ids:
            continue

        row = build_event_row(
            event,
            category,
            existing_row=None,
        )
        if row is None or row["closed"]:
            continue

        existing = selected_events_by_id.get(row["id"])
        if existing is None or row["volume"] > existing["volume"]:
            selected_events_by_id[row["id"]] = row
    return selected_events_by_id


def collect_all_events(
    polymarket_events_url: str,
    events_page_size: int,
    active_categories: List[Dict[str, Any]],
    existing_event_ids: Set[int],
    *,
    max_workers: int,
) -> List[Dict[str, Any]]:
    selected_events_by_id: Dict[int, Dict[str, Any]] = {}

    if not active_categories:
        return []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(
                fetch_new_events_for_category,
                polymarket_events_url,
                events_page_size,
                category,
                existing_event_ids,
            )
            for category in active_categories
            if str(category.get("slug") or "").strip()
        ]
        for future in as_completed(futures):
            for row in future.result().values():
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
    sync_max_workers = require_env_int_any_or_default(
        ["POLY_SYNC_MAX_WORKERS"],
        default=8,
        min_value=1,
    )

    active_categories = fetch_categories_by_flag(
        supabase_url,
        supabase_key,
        supabase_categories_table,
        flag_column="activeForTrendlum",
    )
    existing_event_ids = fetch_existing_event_ids(
        supabase_url,
        supabase_key,
        supabase_events_table,
    )
    all_events = collect_all_events(
        polymarket_events_url,
        events_page_size,
        active_categories,
        existing_event_ids,
        max_workers=sync_max_workers,
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
    print(f"Eventos ya existentes en Supabase: {len(existing_event_ids)}")
    print(f"Eventos nuevos insertados en Supabase: {len(all_events)}")


if __name__ == "__main__":
    main()
