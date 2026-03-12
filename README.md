# poly_events

Workflow y scripts para sincronizar eventos de Polymarket en Supabase.

## Setup

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -e .
```

## Run

```bash
python src/poly/list_polymarket_categories.py
python src/poly/sync_polymarket_events.py
python src/poly/enrich_polymarket_events.py
```
