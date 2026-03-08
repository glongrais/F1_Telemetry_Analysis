# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

F1 telemetry ingestion and analysis pipeline: FastF1 → DuckDB → dbt. Team radio transcription via mlx_whisper (Apple Silicon).

## Commands

All Python and dbt commands must run through the `.venv` at the repo root. Either activate it or use the full path.

```bash
# Activate venv (required for python and dbt)
source .venv/bin/activate

# Ingestion (from repo root)
python src/fastf1_fetcher/main.py --start-year 2024 --end-year 2024 --skip-radio
python src/fastf1_fetcher/main.py --start-year 2024 --end-year 2024 --workers 8

# dbt (must cd into dbt/f1_analytics first)
cd dbt/f1_analytics
dbt deps                              # install dbt_utils
dbt build                             # run + test all models
dbt run --select mart__driver_standings  # single model
dbt test --select staging             # test one layer
dbt build --select +mart__driver_standings  # model + all ancestors
```

The dbt profile lives at `~/.dbt/profiles.yml`.

## Architecture

```
INGESTION (Python)                    TRANSFORMATION (dbt)
src/fastf1_fetcher/                   dbt/f1_analytics/models/
  main.py        → orchestrator         staging/     → 14 views (1:1 on raw tables)
  fetcher.py     → parallel FastF1      intermediate/→ 10 views (business logic)
  db_writer.py   → bulk DuckDB insert   marts/       → 4 tables (analysis-ready)
  db_reader.py   → query helpers
  schema.py      → DDL (15 tables)
  downloader.py  → radio mp3 download
  agent.py       → mlx_whisper transcription
```

**Database**: `data/f1_data_v2.duckdb` (DuckDB, dbt-duckdb adapter, schema `main`)

### ID scheme

Synthetic deterministic integer keys:
- `event_id` = `year * 100 + round_number`
- `session_id` = `event_id * 10 + ordinal` (FP1=1, FP2=2, FP3=3, Qualifying=4, Sprint Qualifying=5, Sprint=6, Race=7)

### Key design patterns

- **Parallel fetch, serial write**: ProcessPoolExecutor for FastF1 loads, single DuckDB connection for writes (avoids lock contention)
- **Idempotent**: All inserts use `ON CONFLICT DO NOTHING`; re-runs are safe
- **Atomic per-session**: One transaction per session; rollback on error
- **Watermark-based radio pipeline**: Resumes from max(team_radio_id)

### dbt DAG (key paths)

```
stg__events/sessions/drivers → int__race_results → mart__driver_standings → mart__constructor_standings
                             → int__qualifying_results → mart__head_to_head
stg__laps                    → int__tyre_degradation, int__stints
stg__race_control            → int__race_control_incidents → mart__track_characteristics
stg__weather                 → int__weather_summary        ↗
stg__intervals               → int__gap_analysis
```

Materializations: staging = view, intermediate = view, marts = table.

### Data quirks

- `gap_to_leader` in intervals is VARCHAR — lapped cars show `'1 L'`, `'LAP 23'` etc. Use `TRY_CAST` for numeric conversion.
- `compound` includes string `'None'` for unknown tyres alongside `'UNKNOWN'` and `'TEST_UNKNOWN'`.
- `grid_position = 0` means pit lane start — NULL out `positions_gained`.
- Driver `status` values: `'Finished'`, `'Lapped'`, `'Retired'`, `'Disqualified'`, `'Did not start'`, `''` (empty string).
- Sprint sessions exist (`Sprint`, `Sprint Qualifying`, `Sprint Shootout`) — standings models currently exclude sprint points.
