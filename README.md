# F1 Telemetry Analysis

Ingestion and analysis pipeline for Formula 1 telemetry data, powered by [FastF1](https://github.com/theOehrly/Fast-F1) and [DuckDB](https://duckdb.org/).

## Architecture

```
src/fastf1_fetcher/   Python ingestion pipeline (FastF1 -> DuckDB)
dbt/f1_analytics/     dbt models (staging, intermediate)
data/                 DuckDB database, radio mp3s, FastF1 cache (gitignored)
```

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install dbt-duckdb
brew install ffmpeg  # required for radio transcription
```

## Ingestion

```bash
cd src/fastf1_fetcher

# Ingest telemetry data (skip radio download/transcription)
python main.py --start-year 2024 --end-year 2024 --skip-radio

# Full pipeline including team radio download + transcription
python main.py --start-year 2024 --end-year 2024
```

Options:
- `--start-year` / `--end-year` — season range to ingest
- `--skip-radio` — skip radio audio download and transcription
- `--workers` — parallel FastF1 session loaders (default: 4)

## dbt

```bash
cd dbt/f1_analytics
dbt run
dbt test
```

### Models

**Staging** — 1:1 views on raw tables: `stg__car_data`, `stg__drivers`, `stg__events`, `stg__intervals`, `stg__laps`, `stg__locations`, `stg__race_control`, `stg__session_status`, `stg__sessions`, `stg__team_radio`, `stg__track_status`, `stg__weather`

**Intermediate** — `int__fastest_laps`, `int__pit_stops`, `int__stints`

## Data sources

All data is fetched via FastF1 from the official F1 live timing API and stored in a local DuckDB database (`data/f1_data_v2.duckdb`). Team radio audio files are downloaded as mp3s and transcribed locally using [mlx_whisper](https://github.com/ml-explore/mlx-examples/tree/main/whisper) (Apple Silicon).
