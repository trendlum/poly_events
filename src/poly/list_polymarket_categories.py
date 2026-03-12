import os
from typing import Any, Dict, List, Set

import requests
from dotenv import load_dotenv
from requests.exceptions import HTTPError


def _require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"Falta variable de entorno requerida: {name}")
    return value


def _require_env_any(names: List[str]) -> str:
    for name in names:
        value = os.getenv(name, "").strip()
        if value:
            return value
    joined = ", ".join(names)
    raise ValueError(f"Falta variable de entorno requerida. Define una de: {joined}")


def _supabase_headers(
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


def fetch_polymarket_categories(polymarket_categories_url: str) -> List[Dict[str, Any]]:
    response = requests.get(
        polymarket_categories_url,
        headers={"accept": "application/json"},
        timeout=30,
    )
    response.raise_for_status()
    raw_categories = response.json()

    if not isinstance(raw_categories, list):
        raise ValueError("Respuesta inesperada de Polymarket: se esperaba una lista.")

    categories_by_id: Dict[str, Dict[str, Any]] = {}
    for category in raw_categories:
        if not isinstance(category, dict):
            continue

        category_id = category.get("id")
        if category_id in (None, ""):
            continue

        label = (category.get("label") or category.get("name") or "").strip()
        slug = (category.get("slug") or "").strip() or None
        parent_category = (
            category.get("parentCategory")
            or category.get("parent_category")
            or category.get("parent")
        )

        categories_by_id[str(category_id)] = {
            "id": category_id,
            "label": label,
            "slug": slug,
            "parentCategory": parent_category,
            "activeForTrendlum": True,
            "topActive": False,
        }

    return list(categories_by_id.values())


def fetch_existing_supabase_category_ids(
    supabase_url: str,
    supabase_key: str,
    supabase_categories_table: str,
) -> Set[str]:
    endpoint = f"{supabase_url}/rest/v1/{supabase_categories_table}?select=id"
    response = requests.get(endpoint, headers=_supabase_headers(supabase_key), timeout=30)
    try:
        response.raise_for_status()
    except HTTPError as exc:
        if response.status_code == 404:
            raise ValueError(
                f"La tabla '{supabase_categories_table}' no existe en Supabase. "
                "Creala primero y vuelve a ejecutar el script."
            ) from exc
        raise

    rows = response.json()
    if not isinstance(rows, list):
        raise ValueError("Respuesta inesperada de Supabase al consultar IDs existentes.")

    return {
        str(row.get("id"))
        for row in rows
        if isinstance(row, dict) and row.get("id") is not None
    }


def insert_new_categories(
    supabase_url: str,
    supabase_key: str,
    supabase_categories_table: str,
    categories_to_insert: List[Dict[str, Any]],
) -> int:
    if not categories_to_insert:
        return 0

    endpoint = f"{supabase_url}/rest/v1/{supabase_categories_table}?on_conflict=id"
    headers = _supabase_headers(supabase_key, include_json_content_type=True)
    headers["Prefer"] = "resolution=ignore-duplicates,return=representation"

    response = requests.post(endpoint, headers=headers, json=categories_to_insert, timeout=30)
    try:
        response.raise_for_status()
    except HTTPError as exc:
        if response.status_code == 404:
            raise ValueError(
                f"La tabla '{supabase_categories_table}' no existe en Supabase. "
                "Creala primero y vuelve a ejecutar el script."
            ) from exc
        raise

    inserted_rows = response.json()
    if isinstance(inserted_rows, list):
        return len(inserted_rows)
    return 0


def main() -> None:
    load_dotenv()

    polymarket_categories_url = _require_env_any(
        ["POLY_GAMMA_CATEGORIES_URL", "POLYMARKET_CATEGORIES_URL"]
    )
    supabase_url = _require_env("SUPABASE_URL").rstrip("/")
    supabase_key = _require_env("SUPABASE_KEY")
    supabase_categories_table = _require_env_any(
        ["POLY_SUPABASE_CATEGORIES_TABLE", "SUPABASE_POLY_CATEGORIES_TABLE"]
    )

    categories = fetch_polymarket_categories(polymarket_categories_url)
    existing_ids = fetch_existing_supabase_category_ids(
        supabase_url,
        supabase_key,
        supabase_categories_table,
    )
    new_categories = [category for category in categories if str(category["id"]) not in existing_ids]

    inserted_count = insert_new_categories(
        supabase_url,
        supabase_key,
        supabase_categories_table,
        new_categories,
    )

    print(f"Categorias obtenidas de Polymarket: {len(categories)}")
    print(f"Categorias ya existentes en Supabase: {len(existing_ids)}")
    print(f"Categorias nuevas detectadas: {len(new_categories)}")
    print(f"Categorias insertadas: {inserted_count}")


if __name__ == "__main__":
    main()
