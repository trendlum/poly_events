import os
import re
import time
from typing import Any, Dict, List, Optional

import requests
from requests.exceptions import HTTPError, RequestException


def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"Falta variable de entorno requerida: {name}")
    return value


def require_env_any(names: List[str]) -> str:
    for name in names:
        value = os.getenv(name, "").strip()
        if value:
            return value
    joined = ", ".join(names)
    raise ValueError(f"Falta variable de entorno requerida. Define una de: {joined}")


def require_env_int_any(names: List[str], min_value: int = 1) -> int:
    raw = require_env_any(names)
    try:
        value = int(raw)
    except ValueError as exc:
        joined = ", ".join(names)
        raise ValueError(f"Valor entero invalido para {joined}: {raw!r}") from exc
    if value < min_value:
        joined = ", ".join(names)
        raise ValueError(f"El valor de {joined} debe ser >= {min_value}.")
    return value


def supabase_headers(
    supabase_key: str,
    *,
    include_json_content_type: bool = False,
) -> Dict[str, str]:
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Accept": "application/json",
    }
    if include_json_content_type:
        headers["Content-Type"] = "application/json"
    return headers


def as_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def as_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def fetch_polymarket_events_by_tag_slug(
    polymarket_events_url: str,
    events_page_size: int,
    tag_slug: str,
) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    offset = 0

    while True:
        response = requests.get(
            polymarket_events_url,
            params={"limit": events_page_size, "offset": offset, "tag_slug": tag_slug},
            headers={"accept": "application/json"},
            timeout=45,
        )
        response.raise_for_status()
        page = response.json()

        if not isinstance(page, list):
            raise ValueError("Respuesta inesperada de Polymarket: se esperaba una lista.")
        if not page:
            break

        events.extend(item for item in page if isinstance(item, dict))
        if len(page) < events_page_size:
            break

        offset += events_page_size
        if offset > 10000:
            break

    return events


def fetch_categories_by_flag(
    supabase_url: str,
    supabase_key: str,
    supabase_categories_table: str,
    *,
    flag_column: str = "activeForTrendlum",
) -> List[Dict[str, Any]]:
    endpoint = (
        f"{supabase_url}/rest/v1/{supabase_categories_table}"
        f"?select=id,label,slug&{flag_column}=eq.true"
    )
    response = requests.get(endpoint, headers=supabase_headers(supabase_key), timeout=30)
    try:
        response.raise_for_status()
    except HTTPError as exc:
        if response.status_code == 404:
            raise ValueError(
                f"No existe la tabla '{supabase_categories_table}' o la columna '{flag_column}' en Supabase."
            ) from exc
        raise

    raw_rows = response.json()
    if not isinstance(raw_rows, list):
        raise ValueError("Respuesta inesperada al leer categorias habilitadas de Supabase.")

    categories: List[Dict[str, Any]] = []
    for row in raw_rows:
        if not isinstance(row, dict):
            continue
        category_id = as_int(row.get("id"))
        slug = str(row.get("slug") or "").strip()
        if category_id is None or not slug:
            continue
        categories.append(
            {
                "id": category_id,
                "slug": slug,
                "label": str(row.get("label") or "").strip(),
            }
        )
    return categories


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return re.sub(r"\s+", " ", text)


def _unique_texts(values: List[Any]) -> List[str]:
    seen = set()
    unique_values: List[str] = []
    for value in values:
        cleaned = _clean_text(value)
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        unique_values.append(cleaned)
    return unique_values


def _coerce_text_list(value: Any) -> List[str]:
    if not isinstance(value, list):
        return []
    return _unique_texts(value)


def _build_enrichment_basis_text(parts: List[Any]) -> str:
    return " || ".join(_unique_texts(parts))


def build_existing_enrichment_basis_text(
    row: Dict[str, Any],
    *,
    include_category_fields: bool = True,
) -> str:
    return _build_enrichment_basis_text(
        [
            row.get("title"),
            row.get("slug"),
            row.get("ticker"),
            row.get("description"),
            row.get("resolutionSource"),
            row.get("resolution_source"),
            row.get("category"),
            row.get("polymarket_category"),
            row.get("category_label") if include_category_fields else None,
            row.get("tag_slug") if include_category_fields else None,
            *_coerce_text_list(row.get("market_questions")),
            *_coerce_text_list(row.get("tags")),
            *_coerce_text_list(row.get("tag_slugs")),
        ]
    )


def should_reset_enrichment(
    existing_row: Optional[Dict[str, Any]],
    candidate_row: Dict[str, Any],
) -> bool:
    if not existing_row or not bool(existing_row.get("is_enriched")):
        return False

    tracked_fields = [
        "title",
        "description",
        "market_descriptions",
        "search_text",
    ]
    for field in tracked_fields:
        if existing_row.get(field) != candidate_row.get(field):
            return True
    return False


def build_event_row(
    event: Dict[str, Any],
    category: Dict[str, Any],
    *,
    existing_row: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    event_id = as_int(event.get("id"))
    if event_id is None:
        return None

    markets = event.get("markets")
    if not isinstance(markets, list):
        markets = []

    market_questions = _unique_texts(
        [market.get("question") for market in markets if isinstance(market, dict)]
    )
    market_titles = _unique_texts(
        [market.get("slug") for market in markets if isinstance(market, dict)]
    )
    market_descriptions = _unique_texts(
        [market.get("description") for market in markets if isinstance(market, dict)]
    )

    tags = event.get("tags")
    tag_labels = _unique_texts(
        [tag.get("label") for tag in tags if isinstance(tags, list) and isinstance(tag, dict)]
    )
    tag_slugs = _unique_texts(
        [tag.get("slug") for tag in tags if isinstance(tags, list) and isinstance(tag, dict)]
    )

    searchable_parts = _unique_texts(
        [
            event.get("title"),
            event.get("slug"),
            event.get("ticker"),
            event.get("description"),
            event.get("resolutionSource"),
            event.get("category"),
            category.get("label"),
            category.get("slug"),
            *market_questions,
            *market_descriptions,
            *tag_labels,
            *tag_slugs,
        ]
    )
    search_text = " || ".join(searchable_parts)

    current_enrichment_basis = _build_enrichment_basis_text(
        [
            event.get("title"),
            event.get("slug"),
            event.get("ticker"),
            event.get("description"),
            event.get("resolutionSource"),
            event.get("category"),
            category.get("label"),
            category.get("slug"),
            *market_questions,
            *tag_labels,
            *tag_slugs,
        ]
    )
    previous_is_enriched = bool(existing_row.get("is_enriched")) if existing_row else False
    previous_enrichment_basis = (
        build_existing_enrichment_basis_text(existing_row) if existing_row else ""
    )
    row = {
        "id": event_id,
        "title": _clean_text(event.get("title")),
        "slug": _clean_text(event.get("slug")),
        "ticker": _clean_text(event.get("ticker")),
        "category_id": category["id"],
        "category_label": _clean_text(category.get("label")),
        "tag_slug": _clean_text(category.get("slug")),
        "polymarket_category": _clean_text(event.get("category")),
        "description": _clean_text(event.get("description")),
        "resolution_source": _clean_text(event.get("resolutionSource")),
        "start_date": event.get("startDate"),
        "end_date": event.get("endDate"),
        "created_at_poly": event.get("createdAt") or event.get("creationDate"),
        "updated_at_poly": event.get("updatedAt"),
        "active": bool(event.get("active")),
        "closed": bool(event.get("closed")),
        "archived": bool(event.get("archived")),
        "featured": bool(event.get("featured")),
        "restricted": bool(event.get("restricted")),
        "comment_count": as_int(event.get("commentCount")),
        "liquidity": as_float(event.get("liquidity")),
        "liquidity_amm": as_float(event.get("liquidityAmm")),
        "volume": as_float(event.get("volume")),
        "open_interest": as_float(event.get("openInterest")),
        "volume24hr": as_float(event.get("volume24hr")),
        "volume1wk": as_float(event.get("volume1wk")),
        "volume1mo": as_float(event.get("volume1mo")),
        "volume1yr": as_float(event.get("volume1yr")),
        "market_count": len(markets),
        "market_questions": market_questions,
        "market_titles": market_titles,
        "market_descriptions": market_descriptions,
        "tags": tag_labels,
        "tag_slugs": tag_slugs,
        "search_text": search_text,
        "is_enriched": previous_is_enriched and previous_enrichment_basis == current_enrichment_basis,
    }
    if should_reset_enrichment(existing_row, row):
        row["is_enriched"] = False
    return row


def fetch_paginated_rows(
    supabase_url: str,
    supabase_key: str,
    table: str,
    select: str,
    *,
    filters: Optional[List[str]] = None,
    page_size: int = 1000,
) -> List[Dict[str, Any]]:
    all_rows: List[Dict[str, Any]] = []
    offset = 0

    while True:
        query_parts = [f"select={select}", f"limit={page_size}", f"offset={offset}"]
        if filters:
            query_parts.extend(filters)
        endpoint = f"{supabase_url}/rest/v1/{table}?{'&'.join(query_parts)}"
        response = requests.get(endpoint, headers=supabase_headers(supabase_key), timeout=30)
        try:
            response.raise_for_status()
        except HTTPError as exc:
            if response.status_code == 404:
                raise ValueError(f"No existe la tabla '{table}' en Supabase.") from exc
            raise

        page = response.json()
        if not isinstance(page, list):
            raise ValueError(f"Respuesta inesperada al leer la tabla '{table}'.")

        rows = [row for row in page if isinstance(row, dict)]
        all_rows.extend(rows)
        if len(page) < page_size:
            break
        offset += page_size

    return all_rows


def fetch_existing_event_state(
    supabase_url: str,
    supabase_key: str,
    supabase_events_table: str,
) -> Dict[int, Dict[str, Any]]:
    rows = fetch_paginated_rows(
        supabase_url,
        supabase_key,
        supabase_events_table,
        (
            "id,category_id,category_label,tag_slug,is_enriched,title,slug,ticker,"
            "description,resolution_source,polymarket_category,market_questions,"
            "market_descriptions,tags,tag_slugs,search_text"
        ),
    )

    existing_by_id: Dict[int, Dict[str, Any]] = {}
    for row in rows:
        row_id = as_int(row.get("id"))
        if row_id is None:
            continue
        existing_by_id[row_id] = row
    return existing_by_id


def upsert_rows(
    supabase_url: str,
    supabase_key: str,
    table: str,
    rows: List[Dict[str, Any]],
    *,
    on_conflict: str,
    batch_size: int = 100,
    timeout: int = 60,
    max_retries: int = 3,
) -> None:
    if not rows:
        return

    endpoint = f"{supabase_url}/rest/v1/{table}?on_conflict={on_conflict}"
    headers = supabase_headers(supabase_key, include_json_content_type=True)
    headers["Prefer"] = "resolution=merge-duplicates,return=minimal"

    def submit_batch(batch: List[Dict[str, Any]], batch_start: int, batch_end: int) -> None:
        last_error: Optional[Exception] = None
        response: Optional[requests.Response] = None

        for attempt in range(1, max_retries + 1):
            try:
                response = requests.post(endpoint, headers=headers, json=batch, timeout=timeout)
                response.raise_for_status()
                last_error = None
                break
            except HTTPError as exc:
                if response.status_code == 404:
                    raise ValueError(f"No existe la tabla '{table}' en Supabase.") from exc
                last_error = exc
            except RequestException as exc:
                last_error = exc

            if attempt < max_retries:
                time.sleep(attempt)

        if last_error is not None:
            if len(batch) > 1:
                midpoint = len(batch) // 2
                submit_batch(batch[:midpoint], batch_start, batch_start + midpoint)
                submit_batch(batch[midpoint:], batch_start + midpoint, batch_end)
                return
            response_details = ""
            if response is not None and getattr(response, "text", ""):
                response_details = f" Response body: {response.text[:500]}"
            raise ValueError(
                f"Fallo haciendo upsert en '{table}' para el lote "
                f"{batch_start + 1}-{batch_end} de {len(rows)} filas: {last_error}"
                f"{response_details}"
            ) from last_error

    for start in range(0, len(rows), batch_size):
        batch = rows[start : start + batch_size]
        submit_batch(batch, start, start + len(batch))
