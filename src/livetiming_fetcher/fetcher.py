import logging
import os
import sys
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm

# Add fastf1_fetcher to path for db_writer, db_reader, schema imports
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'fastf1_fetcher'))
import db_writer  # noqa: E402
import db_reader  # noqa: E402
from schema import DB_PATH  # noqa: E402

# Local imports (resolved relative to this package)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import client  # noqa: E402
import decoder  # noqa: E402
from schedule import get_season_sessions, SessionInfo  # noqa: E402
from parsers.session_status import parse_session_status, find_session_start  # noqa: E402
from parsers.weather import parse_weather  # noqa: E402
from parsers.track_status import parse_track_status  # noqa: E402
from parsers.race_control import parse_race_control  # noqa: E402
from parsers.team_radio import parse_team_radio  # noqa: E402
from parsers.car_data import parse_car_data  # noqa: E402
from parsers.positions import parse_positions  # noqa: E402
from parsers.laps import LapAccumulator  # noqa: E402
from parsers.drivers import parse_drivers  # noqa: E402
from parsers.intervals import parse_intervals  # noqa: E402

logging.basicConfig(level=logging.WARNING)

MAX_WORKERS = 4

# Topics we need and whether they're compressed
TOPICS = {
    "SessionStatus": False,
    "WeatherData": False,
    "TrackStatus": False,
    "RaceControlMessages": False,
    "TeamRadio": False,
    "DriverList": False,
    "TimingData": False,
    "TimingAppData": False,
    "CarData.z": True,
    "Position.z": True,
}


def _fetch_session(info):
    # type: (SessionInfo) -> dict
    """Fetch and parse all topics for a session. Runs in worker process."""
    session_path = info.session_path

    # Fetch session index to get stream paths
    try:
        index = client.fetch_session_index(session_path)
    except Exception as e:
        raise RuntimeError(f"Failed to fetch session index for {info.session_name}: {e}")

    feeds = index.get("Feeds", {})

    # Fetch and decode each topic
    raw_messages = {}
    for topic, compressed in TOPICS.items():
        feed = feeds.get(topic, {})
        stream_path = feed.get("StreamPath", "")
        if not stream_path:
            continue
        text = client.fetch_topic(session_path, stream_path)
        if text is None:
            continue
        if compressed:
            raw_messages[topic] = decoder.parse_compressed_jsonstream(text)
        else:
            raw_messages[topic] = decoder.parse_jsonstream(text)

    # Determine t0 (session start timestamp)
    session_status_msgs = raw_messages.get("SessionStatus", [])
    t0 = find_session_start(session_status_msgs)

    # Parse all topics
    result = {
        "session_status": parse_session_status(session_status_msgs, t0),
        "weather": parse_weather(raw_messages.get("WeatherData", []), t0),
        "track_status": parse_track_status(raw_messages.get("TrackStatus", []), t0),
        "race_control": parse_race_control(raw_messages.get("RaceControlMessages", []), t0),
        "team_radio": parse_team_radio(raw_messages.get("TeamRadio", []), t0, session_path),
    }

    # Compressed topics
    result["car_data"] = parse_car_data(raw_messages.get("CarData.z", []), t0)
    result["positions"] = parse_positions(raw_messages.get("Position.z", []), t0)

    # Drivers
    driver_list_msgs = raw_messages.get("DriverList", [])
    timing_data_msgs = raw_messages.get("TimingData", [])
    result["drivers"] = parse_drivers(driver_list_msgs, timing_data_msgs)

    # Laps (stateful accumulator)
    lap_acc = LapAccumulator()
    # Set driver info from DriverList
    for ts, data in driver_list_msgs:
        for drv_str, drv_info in data.items():
            if isinstance(drv_info, dict) and "RacingNumber" in drv_info:
                try:
                    drv_num = int(drv_info["RacingNumber"])
                    lap_acc.set_driver_info(
                        drv_num,
                        drv_info.get("Tla", ""),
                        drv_info.get("TeamName", ""),
                    )
                except (ValueError, TypeError):
                    pass
    lap_acc.process_timing_app_data(raw_messages.get("TimingAppData", []))
    lap_acc.process_timing_data(timing_data_msgs)
    result["laps"] = lap_acc.finalize()

    # Intervals (Race/Sprint only)
    if info.session_type in ("Race", "Sprint"):
        result["intervals"] = parse_intervals(timing_data_msgs, t0)
    else:
        result["intervals"] = None

    # Session metadata
    result["start_date"] = info.start_date
    result["end_date"] = info.end_date

    return result


class LivetimingFetcher:
    def __init__(self, db_path=DB_PATH, max_workers=MAX_WORKERS):
        self.db_path = db_path
        self.max_workers = max_workers

    def fetch_season(self, year, round_filter=None):
        # type: (int, int) -> None
        sessions = get_season_sessions(year)
        ingested = db_reader.get_ingested_sessions(self.db_path)

        # Track events we've already inserted
        inserted_events = set()
        tasks = []

        for info in sessions:
            if round_filter is not None and info.round_number != round_filter:
                continue

            # Insert event eagerly (idempotent)
            if info.event_id not in inserted_events:
                conn = db_writer.get_connection(self.db_path)
                db_writer.insert_event(
                    conn, info.event_id, info.year, info.round_number,
                    info.country, info.location, info.event_name,
                    info.event_date, info.event_format,
                )
                conn.commit()
                conn.close()
                inserted_events.add(info.event_id)

            if info.session_id in ingested:
                continue
            tasks.append(info)

        if not tasks:
            print(f"  Season {year}: nothing to ingest.")
            return

        self._process_tasks(tasks)

    def _process_tasks(self, tasks):
        # type: (list) -> None
        pbar = tqdm(total=len(tasks), desc="Sessions", unit="sess")

        with ProcessPoolExecutor(max_workers=self.max_workers) as pool:
            future_to_info = {}
            for info in tasks:
                future = pool.submit(_fetch_session, info)
                future_to_info[future] = info

            for future in as_completed(future_to_info):
                info = future_to_info[future]
                try:
                    data = future.result()
                    self._write_session(info, data)
                except Exception as e:
                    error_msg = f"{e}\n{traceback.format_exc()}"
                    tqdm.write(f"    FAILED: {info.event_name} {info.session_type}: {e}")
                    try:
                        conn = db_writer.get_connection(self.db_path)
                        db_writer.insert_ingestion_log(
                            conn, info.session_id, info.event_id, info.year,
                            info.round_number, info.session_type,
                            status="failed", error_message=error_msg,
                        )
                        conn.commit()
                        conn.close()
                    except Exception:
                        pass
                pbar.update(1)

        pbar.close()

    def _write_session(self, info, data):
        # type: (SessionInfo, dict) -> None
        conn = db_writer.get_connection(self.db_path)
        try:
            db_writer.insert_session(
                conn, info.session_id, info.event_id, info.year,
                info.round_number, info.session_type, info.session_name,
                data.get("start_date"), data.get("end_date"),
            )

            drivers_df = data.get("drivers")
            if drivers_df is not None and not drivers_df.empty:
                db_writer.insert_drivers(conn, info.session_id, drivers_df)

            laps_df = data.get("laps")
            if laps_df is not None and not laps_df.empty:
                db_writer.insert_laps(conn, info.session_id, laps_df)

            weather_df = data.get("weather")
            if weather_df is not None and not weather_df.empty:
                db_writer.insert_weather(conn, info.session_id, weather_df)

            rc_df = data.get("race_control")
            if rc_df is not None and not rc_df.empty:
                db_writer.insert_race_control(conn, info.session_id, rc_df)

            ss_df = data.get("session_status")
            if ss_df is not None and not ss_df.empty:
                db_writer.insert_session_status(conn, info.session_id, ss_df)

            ts_df = data.get("track_status")
            if ts_df is not None and not ts_df.empty:
                db_writer.insert_track_status(conn, info.session_id, ts_df)

            radio_df = data.get("team_radio")
            if radio_df is not None and not radio_df.empty:
                db_writer.insert_team_radio(conn, info.session_id, radio_df)

            car_data = data.get("car_data")
            if car_data:
                try:
                    db_writer.insert_car_data_bulk(conn, info.session_id, car_data)
                except Exception as e:
                    tqdm.write(f"    Warning: car_data failed for {info.event_name} {info.session_type}: {e}")

            positions = data.get("positions")
            if positions:
                try:
                    db_writer.insert_locations_bulk(conn, info.session_id, positions)
                except Exception as e:
                    tqdm.write(f"    Warning: positions failed for {info.event_name} {info.session_type}: {e}")

            intervals_df = data.get("intervals")
            if intervals_df is not None and not intervals_df.empty:
                try:
                    db_writer.insert_intervals(conn, info.session_id, intervals_df)
                except Exception as e:
                    tqdm.write(f"    Warning: intervals failed for {info.event_name} {info.session_type}: {e}")

            conn.commit()
            db_writer.insert_ingestion_log(
                conn, info.session_id, info.event_id, info.year,
                info.round_number, info.session_type, status="complete",
            )
            conn.commit()
            tqdm.write(f"    Done: {info.event_name} {info.session_type}")

        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            error_msg = f"{e}\n{traceback.format_exc()}"
            tqdm.write(f"    FAILED: {info.event_name} {info.session_type}: {e}")
            try:
                db_writer.insert_ingestion_log(
                    conn, info.session_id, info.event_id, info.year,
                    info.round_number, info.session_type,
                    status="failed", error_message=error_msg,
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
        # Re-fetch session info from schedule for failed sessions
        # For simplicity, just log and skip (user can re-run with --round)
        print(f"Found {len(failed)} failed sessions. Re-run with --round to retry specific rounds.")
