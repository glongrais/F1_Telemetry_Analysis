# Next Steps — Post-Migration

## Status

Phase 1-4 of the FastF1 migration plan are complete. The new pipeline has been validated with a full 2024 season ingestion (120/120 sessions, all tables populated).

---

## 1. Backfill Historical Data (2018–2023)

```bash
cd src/fastf1_fetcher
python main.py --start-year 2018 --end-year 2023 --skip-radio --workers 4
```

- Expect ~600+ sessions across 6 seasons
- Use `--skip-radio` for the initial bulk load to avoid slowing things down
- After completion, run `python main.py --start-year 2018 --end-year 2023` (without `--skip-radio`) to backfill team radio data
- Check for failures: `SELECT * FROM ingestion_log WHERE status = 'failed'`
- Retry failures with the built-in retry mechanism

**Known risks:**
- Older seasons (2018-2019) may have missing data for some fields (e.g., deleted lap reasons, DRS values)
- FastF1 cache will grow significantly (~10-20 GB); ensure disk space is available

---

## 2. Validate dbt Layer ✅

- Updated `~/.dbt/profiles.yml` and `sources.yml` to point to `f1_data_v2.duckdb`
- `dbt run`: 18/18 models pass (14 staging + 3 intermediate + 1 watermark)
- `dbt test`: 4/4 tests pass
- Note: dbt must be run from `dbt/f1_analytics/`, not `dbt/`

---

## 3. Test Team Radio Pipeline ✅

- Reviewed and cleaned up `downloader.py`, `agent.py`, `db_reader.py`, `db_writer.py`
  - Replaced positional tuple access with dict access throughout
  - Shared DB connections instead of one-per-write
  - Added error handling and request timeouts to downloader
  - Made downloader fully idempotent (watermark advances on failure too)
  - Added `UNIQUE(item_id)` constraint on `failed_downloads` to prevent duplicates
- Parallelized downloads with `ThreadPoolExecutor` (8 workers, ~10x speedup)
- Benchmarked transcription models: `mlx_whisper` + `large-v3-turbo` is already optimal (~0.45s/file on M4 Max)
- Installed `ffmpeg` dependency required by mlx_whisper
- Installed `dbt-duckdb` in project venv
- Full 2024 radio pipeline tested: downloads, transcriptions, watermark all working

---

## 4. Commit and Clean Up

- [ ] Review all changes with `git diff`
- [ ] Remove `src/api_fetcher/` if not already deleted
- [ ] Commit the new `src/fastf1_fetcher/` module, updated dbt models, and schema
- [ ] Delete any leftover temp files or test databases
- [ ] Update top-level README if it references the old API fetcher

---

## 5. Future Improvements (Optional)

- **Incremental 2025 ingestion**: Run the pipeline periodically to pick up new race weekends as they happen
- **Data quality checks**: Add dbt tests for row counts, null rates, and cross-table referential integrity
- **Dashboard refresh**: Update any downstream dashboards/notebooks to use the new table names and key system
- **Telemetry analysis models**: Build dbt models for common analysis patterns (tire degradation, sector comparisons, weather impact)
