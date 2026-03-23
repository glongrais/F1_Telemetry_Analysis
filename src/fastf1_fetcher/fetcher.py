import logging
import fastf1
import fastf1.api
import pandas as pd
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm

logging.basicConfig(level=logging.ERROR)
logging.getLogger("fastf1").setLevel(logging.ERROR)

import os
import sys

# Add shared module to path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from shared import SESSION_TYPE_ORDINALS, make_event_id, make_session_id  # noqa: E402

import db_writer  # noqa: E402
import db_reader  # noqa: E402
from schema import DB_PATH  # noqa: E402

CACHE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'data', 'fastf1_cache')
MAX_WORKERS = 4


def _load_session(year, round_number, session_type):
    """Load a FastF1 session in a worker process."""
    logging.basicConfig(level=logging.ERROR)
    logging.getLogger("fastf1").setLevel(logging.ERROR)
    fastf1.Cache.enable_cache(CACHE_PATH)

    session = fastf1.get_session(year, round_number, session_type)
    session.load(telemetry=True, weather=True, messages=True)

    # Also load timing data for Race/Sprint
    timing_df = None
    if session_type in ("Race", "Sprint"):
        try:
            _, stream_df = fastf1.api.timing_data(session.api_path)
            if stream_df is not None and not stream_df.empty:
                timing_df = stream_df.rename(columns={"Driver": "DriverNumber"})
        except Exception:
            pass

    # Load team radio from live timing API
    team_radio_df = _load_team_radio(session.api_path)

    return session, timing_df, team_radio_df


def _load_team_radio(api_path):
    """Fetch team radio data from F1 live timing API."""
    import requests, json
    try:
        base = "https://livetiming.formula1.com"
        r = requests.get(f"{base}{api_path}TeamRadio.jsonStream", timeout=10)
        if r.status_code != 200:
            return None
        rows = []
        for line in r.text.strip().split("\r\n"):
            if not line:
                continue
            # Format: "HH:MM:SS.fff{json}"
            brace = line.index("{")
            ts_str = line[:brace]
            payload = json.loads(line[brace:])
            captures = payload.get("Captures", {})
            # Captures can be a list or dict
            items = captures if isinstance(captures, list) else captures.values()
            for cap in items:
                h, m, rest = ts_str.split(":")
                s = float(rest)
                timestamp = int(h) * 3600 + int(m) * 60 + s
                rows.append({
                    "RacingNumber": cap.get("RacingNumber"),
                    "Path": f"{base}{api_path}{cap.get('Path', '')}",
                    "Time": pd.Timedelta(seconds=timestamp),
                })
        if not rows:
            return None
        return pd.DataFrame(rows)
    except Exception:
        return None


class FastF1Fetcher:
    def __init__(self, db_path=DB_PATH, max_workers=MAX_WORKERS):
        self.db_path = db_path
        self.max_workers = max_workers
        fastf1.Cache.enable_cache(CACHE_PATH)

    def fetch_season(self, year):
        schedule = fastf1.get_event_schedule(year, include_testing=False)
        ingested = db_reader.get_ingested_sessions(self.db_path)

        # Build list of (event_metadata, session_tasks) to process
        tasks = []
        for _, event in schedule.iterrows():
            round_number = int(event["RoundNumber"])
            if round_number == 0:
                continue

            event_id = make_event_id(year, round_number)
            event_meta = {
                "event_id": event_id,
                "year": year,
                "round_number": round_number,
                "country": event.get("Country"),
                "location": event.get("Location"),
                "event_name": event.get("EventName"),
                "event_date": event.get("EventDate"),
                "event_format": event.get("EventFormat"),
            }

            # Insert event eagerly (cheap, idempotent)
            conn = db_writer.get_connection(self.db_path)
            db_writer.insert_event(
                conn, event_id, year, round_number,
                event_meta["country"], event_meta["location"],
                event_meta["event_name"], event_meta["event_date"],
                event_meta["event_format"],
            )
            conn.commit()
            conn.close()

            session_cols = ["Session1", "Session2", "Session3", "Session4", "Session5"]
            for col in session_cols:
                session_type = str(event.get(col, ""))
                if session_type in ("None", "nan", "", "NaT"):
                    continue
                session_id = make_session_id(event_id, session_type)
                if session_id in ingested:
                    continue
                tasks.append((event_meta, session_id, session_type))

        if not tasks:
            print(f"  Season {year}: nothing to ingest.")
            return

        # Process with parallel loading + serial DB writes
        self._process_tasks(tasks)

    def _process_tasks(self, tasks):
        """Submit FastF1 loads in parallel, write to DB as they complete."""
        pbar = tqdm(total=len(tasks), desc="Sessions", unit="sess")

        with ProcessPoolExecutor(max_workers=self.max_workers) as pool:
            # Submit all loads
            future_to_meta = {}
            for event_meta, session_id, session_type in tasks:
                future = pool.submit(
                    _load_session,
                    event_meta["year"],
                    event_meta["round_number"],
                    session_type,
                )
                future_to_meta[future] = (event_meta, session_id, session_type)

            # Write results as they arrive
            for future in as_completed(future_to_meta):
                event_meta, session_id, session_type = future_to_meta[future]
                event_name = event_meta["event_name"]
                event_id = event_meta["event_id"]
                year = event_meta["year"]
                round_number = event_meta["round_number"]

                try:
                    session, timing_df, team_radio_df = future.result()
                    self._write_session(
                        session, timing_df, team_radio_df, session_id, event_id,
                        year, round_number, session_type, event_name,
                    )
                except Exception as e:
                    error_msg = f"{e}\n{traceback.format_exc()}"
                    print(f"    FAILED: {event_name} {session_type}: {e}")
                    try:
                        conn = db_writer.get_connection(self.db_path)
                        db_writer.insert_ingestion_log(
                            conn, session_id, event_id, year, round_number,
                            session_type, status="failed", error_message=error_msg,
                        )
                        conn.commit()
                        conn.close()
                    except Exception:
                        pass

                pbar.update(1)

        pbar.close()

    def _write_session(self, session, timing_df, team_radio_df, session_id, event_id,
                       year, round_number, session_type, event_name):
        """Write loaded session data to DuckDB (single-threaded)."""
        conn = db_writer.get_connection(self.db_path)
        try:
            date_start = session.date if hasattr(session, 'date') else None
            date_end = None
            if hasattr(session, 'session_info') and session.session_info:
                date_end = session.session_info.get('EndDate')
            total_laps = session.total_laps if hasattr(session, 'total_laps') else None
            sst = session.session_start_time.total_seconds() if hasattr(session, 'session_start_time') and session.session_start_time is not None else None

            db_writer.insert_session(
                conn, session_id, event_id, year, round_number,
                session_type, session_type, date_start, date_end,
                total_laps, sst,
            )

            if session.results is not None and not session.results.empty:
                db_writer.insert_drivers(conn, session_id, session.results)

            if session.laps is not None and not session.laps.empty:
                db_writer.insert_laps(conn, session_id, session.laps)

            if session.weather_data is not None and not session.weather_data.empty:
                db_writer.insert_weather(conn, session_id, session.weather_data)

            if session.race_control_messages is not None and not session.race_control_messages.empty:
                db_writer.insert_race_control(conn, session_id, session.race_control_messages)

            if session.session_status is not None and not session.session_status.empty:
                db_writer.insert_session_status(conn, session_id, session.session_status)

            if session.track_status is not None and not session.track_status.empty:
                db_writer.insert_track_status(conn, session_id, session.track_status)

            if team_radio_df is not None and not team_radio_df.empty:
                db_writer.insert_team_radio(conn, session_id, team_radio_df)

            if isinstance(session.car_data, dict):
                try:
                    db_writer.insert_car_data_bulk(conn, session_id, session.car_data)
                except Exception as e:
                    print(f"    Warning: car_data failed: {e}")

            if isinstance(session.pos_data, dict):
                try:
                    db_writer.insert_locations_bulk(conn, session_id, session.pos_data)
                except Exception as e:
                    print(f"    Warning: pos_data failed: {e}")

            if timing_df is not None and not timing_df.empty:
                try:
                    db_writer.insert_intervals(conn, session_id, timing_df)
                except Exception as e:
                    print(f"    Warning: intervals failed: {e}")

            db_writer.insert_ingestion_log(
                conn, session_id, event_id, year, round_number,
                session_type, status="complete",
            )
            conn.commit()  # single atomic commit for all session data + log
            tqdm.write(f"    Done: {event_name} {session_type}")

        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            error_msg = f"{e}\n{traceback.format_exc()}"
            tqdm.write(f"    FAILED: {event_name} {session_type}: {e}")
            try:
                db_writer.insert_ingestion_log(
                    conn, session_id, event_id, year, round_number,
                    session_type, status="failed", error_message=error_msg,
                )
                conn.commit()
            except Exception:
                pass
        finally:
            conn.close()

    def retry_failed(self):
        failed = db_reader.get_failed_sessions(self.db_path)
        if not failed:
            print("No failed sessions to retry.")
            return

        tasks = []
        for row in failed:
            session_id, event_id, year, round_number, session_type, _ = row
            event_meta = {
                "event_id": event_id,
                "year": year,
                "round_number": round_number,
                "country": None, "location": None,
                "event_name": f"R{round_number}",
                "event_date": None, "event_format": None,
            }
            tasks.append((event_meta, session_id, session_type))

        self._process_tasks(tasks)
