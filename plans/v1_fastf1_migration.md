# Migration Plan: OpenF1 API → FastF1 Library

## Context

The current pipeline fetches F1 data via the OpenF1 REST API, which is itself built on FastF1. This causes rate limiting and slow ingestion (per-driver, 1-hour batch windows). By using FastF1 directly, we eliminate the middleman: no rate limits, bulk session loading, richer data, and built-in caching.

**Decisions made:**
- Synthetic integer keys (replacing meeting_key/session_key)
- Clean break (new DuckDB file, re-ingest from scratch)
- Derive intervals from FastF1's timing_data() stream
- Year range: 2018–2025 (configurable at runtime)

---

## 1. Key System

Replace OpenF1's `meeting_key`/`session_key` with deterministic synthetic IDs:

- **`event_id`** = `year * 100 + round_number` (e.g., `202401` = 2024 Bahrain GP)
- **`session_id`** = `event_id * 10 + session_ordinal` where ordinals are: FP1=1, FP2=2, FP3=3, Q=4, SQ=5, S=6, R=7 (e.g., `2024017` = 2024 Bahrain Race)

---

## 2. New Database Schema

### Tables to create fresh

**`events`** (replaces `meetings`)
- PK: `event_id`
- Columns: year, round_number, country, location, event_name, event_date, event_format

**`sessions`**
- PK: `session_id`
- FK: event_id
- Columns: year, round_number, session_type, session_name, date_start, date_end

**`drivers`**
- PK: (session_id, driver_number)
- Columns: driver_code, full_name, team_name, team_color, position, grid_position, status, points

**`laps`** (absorbs stints + pits data)
- PK: (session_id, driver_number, lap_number)
- Columns: lap_time, sector_1/2/3_time, speed_i1/i2/fl/st, compound, tyre_life, fresh_tyre, stint, position, is_personal_best, is_accurate, deleted, deleted_reason, pit_in_time, pit_out_time, lap_start_time

**`car_data`**
- PK: (session_id, driver_number, timestamp)
- Columns: speed, rpm, n_gear, throttle, brake, drs

**`locations`** (XYZ position data)
- PK: (session_id, driver_number, timestamp)
- Columns: x, y, z

**`intervals`** (derived from FastF1 timing_data() stream)
- PK: (session_id, driver_number, timestamp)
- Columns: position, gap_to_leader, interval_to_ahead

**`weather`**
- PK: (session_id, timestamp)
- Columns: air_temperature, humidity, pressure, rainfall, track_temperature, wind_direction, wind_speed

**`race_control`**
- PK: auto-increment or (session_id, timestamp, message hash)
- Columns: session_id, timestamp, category, message, flag, scope, sector, driver_number, lap_number

**`team_radio`**
- PK: team_radio_id (auto-increment)
- Columns: session_id, driver_number, timestamp, recording_url

**`team_radio_files`** — keep as-is
**`team_radio_texts`** — keep as-is

**`ingestion_log`** (replaces watermarks + failed_queries)
- PK: session_id
- Columns: event_id, year, round_number, session_type, ingested_at, status ('complete'/'failed'), error_message

**`failed_downloads`** — keep as-is
**`downloader_watermark`** — keep as-is

### Tables dropped
- `meetings` (replaced by `events`)
- `positions` (track position P1-P20 — now a column in `laps`)
- `stints` (absorbed into `laps` columns; derive a dbt view if needed)
- `pits` (absorbed into `laps` pit_in/pit_out; derive a dbt view if needed)
- `failed_queries` (replaced by `ingestion_log`)

---

## 3. New Python Module: `src/fastf1_fetcher/`

### Files to create

**`schema.py`** — DDL statements to create all tables in a fresh DuckDB file.

**`fetcher.py`** — Core ingestion logic:
- `FastF1Fetcher` class with `fetch_season(year)` method
- Iterates `fastf1.get_event_schedule(year)`, skips already-ingested sessions via `ingestion_log`
- For each session: `session.load()` → transform DataFrames → bulk insert into DuckDB
- For intervals: use `fastf1.api.timing_data()` to get the stream DataFrame with Position/GapToLeader/IntervalToPositionAhead
- Atomic per-session: if any part fails, mark session as 'failed' in ingestion_log
- `retry_failed()` method to re-attempt failed sessions
- Enable `fastf1.Cache.enable_cache('../../data/fastf1_cache')` for local caching

**`db_writer.py`** — Bulk insert using DuckDB's native DataFrame support:
- One method per table, each transforms a FastF1 DataFrame → schema-matching DataFrame → `INSERT INTO table SELECT * FROM df ON CONFLICT DO NOTHING`
- Helper: `timedelta_to_seconds()` for converting pd.Timedelta (handles NaT → None)
- No row-by-row loops

**`db_reader.py`** — Simplified:
- `get_ingested_sessions()` → set of session_ids where status='complete'
- `get_failed_sessions()` → list of failed session info for retry
- Team radio queries for downloader/agent

**`main.py`** — Entry point:
```
1. Init schema if new DB
2. For year in range(2018, 2026): fetcher.fetch_season(year)
3. fetcher.retry_failed()
4. Downloader.download_team_radio_audio()
5. Agent.run()
```

### Files to keep (with minor edits)
- **`downloader.py`** — Update queries to use `session_id` instead of `meeting_key`/`session_key`
- **`agent.py`** — Keep as-is (reads from team_radio_watermark view, data-source agnostic)

### Files to delete
- `src/api_fetcher/` — entire directory (api.py, fetcher.py, db_writer.py, db_reader.py, main.py)

---

## 4. dbt Model Changes

### Sources (`sources.yml`)
- Update table list: `events`, `sessions`, `drivers`, `laps`, `car_data`, `locations`, `intervals`, `weather`, `race_control`, `team_radio`, `team_radio_files`, `team_radio_texts`, `ingestion_log`

### Staging models
| Action | Model |
|--------|-------|
| Rename | `stg__meetings` → `stg__events` |
| Update | `stg__sessions`, `stg__drivers`, `stg__laps`, `stg__car_data`, `stg__weather`, `stg__race_control`, `stg__team_radio` (column name changes) |
| Keep   | `stg__team_radio_files`, `stg__team_radio_texts` |
| Rename | `stg__locations` (now XYZ data, was separate from positions) |
| Keep   | `stg__intervals` (now sourced from timing stream data) |
| Delete | `stg__pits`, `stg__stints`, `stg__positions` (old P1-P20 table) |

### Intermediate models
| Action | Model |
|--------|-------|
| Update | `int__fastest_laps` — use session_id, update session_type values |
| New    | `int__stints` — derive from laps (GROUP BY session_id, driver_number, stint) |
| New    | `int__pit_stops` — derive from laps WHERE pit_in_time IS NOT NULL |

### Core / other models
| Action | Model |
|--------|-------|
| Delete | `core__sessions_context` (was for the old fetcher's batching) |
| Delete | `watermarks.sql` (replaced by ingestion_log) |
| Update | `team_radio_watermark.sql` — keep, minor column adjustments |

---

## 5. Implementation Order

### Phase 1: Schema & writer
1. Create `src/fastf1_fetcher/` directory
2. Write `schema.py` with all CREATE TABLE IF NOT EXISTS statements
3. Write `db_writer.py` with bulk DataFrame insert methods
4. Write `db_reader.py`

### Phase 2: Fetcher
5. Write `fetcher.py` with FastF1 session loading + DataFrame transformation
6. Handle: timedelta conversions, NaT→None, column renaming, intervals from timing_data()
7. Write `main.py`

### Phase 3: Validate single session
8. Run for one session (e.g., 2024 Bahrain Race) and verify all tables
9. Spot-check data quality

### Phase 4: Radio pipeline
10. Copy and update `downloader.py` and `agent.py` into new module

### Phase 5: dbt layer
11. Update sources.yml
12. Update/create/delete staging models
13. Update intermediate models, create int__stints and int__pit_stops
14. Delete watermarks.sql and core__sessions_context.sql
15. Run `dbt run` + `dbt test`

### Phase 6: Cleanup
16. Delete `src/api_fetcher/`
17. Update README
18. Add `fastf1` to dependencies

---

## 6. Verification

- Run ingestion for a single session, inspect all tables in DuckDB
- Compare row counts / data with OpenF1 for same session
- Run `dbt run` and `dbt test` end-to-end
- Test team radio download + transcription pipeline
- Run full 2018-2025 ingestion, check ingestion_log for failures
