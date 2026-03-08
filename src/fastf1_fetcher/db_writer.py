import duckdb
import pandas as pd
import numpy as np

DB_PATH = '../../data/f1_data_v2.duckdb'


def get_connection(db_path=DB_PATH):
    return duckdb.connect(db_path)


def to_seconds_or_timestamp(val):
    if pd.isna(val):
        return None
    if hasattr(val, 'total_seconds'):
        return val.total_seconds()
    if isinstance(val, pd.Timestamp):
        return val.timestamp()
    return float(val)


def _td_col(series):
    """Vectorized timedelta-to-seconds for a Series."""
    return series.dt.total_seconds()


def _safe_td_col(df, col):
    """Extract timedelta column as seconds, returning None if column missing."""
    if col not in df.columns:
        return None
    return df[col].dt.total_seconds()


def _safe_col(df, col):
    """Extract column values, returning None if column missing."""
    if col not in df.columns:
        return None
    return df[col]


def _safe_numeric_col(df, col, dtype="Int64"):
    """Extract column as numeric, returning None if column missing."""
    if col not in df.columns:
        return None
    return pd.to_numeric(df[col], errors="coerce").astype(dtype)


def _bulk_insert(conn, table, df, on_conflict="DO NOTHING"):
    if df.empty:
        return
    cols = ", ".join(df.columns)
    conn.execute(
        f"INSERT INTO {table} ({cols}) SELECT {cols} FROM df ON CONFLICT {on_conflict}"
    )


def insert_event(conn, event_id, year, round_number, country, location,
                 event_name, event_date, event_format):
    conn.execute("""
        INSERT INTO events (event_id, year, round_number, country, location,
                           event_name, event_date, event_format)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT DO NOTHING
    """, [event_id, year, round_number, country, location,
          event_name, event_date, event_format])


def insert_session(conn, session_id, event_id, year, round_number,
                   session_type, session_name, date_start, date_end,
                   total_laps=None, session_start_time=None):
    conn.execute("""
        INSERT INTO sessions (session_id, event_id, year, round_number,
                             session_type, session_name, date_start, date_end,
                             total_laps, session_start_time)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT DO NOTHING
    """, [session_id, event_id, year, round_number,
          session_type, session_name, date_start, date_end,
          total_laps, session_start_time])


def insert_drivers(conn, session_id, results_df):
    if results_df is None or results_df.empty:
        return
    df = pd.DataFrame({
        "session_id": session_id,
        "driver_number": results_df["DriverNumber"].astype(int),
        "driver_code": _safe_col(results_df, "Abbreviation"),
        "driver_id": _safe_col(results_df, "DriverId"),
        "broadcast_name": _safe_col(results_df, "BroadcastName"),
        "full_name": _safe_col(results_df, "FullName"),
        "first_name": _safe_col(results_df, "FirstName"),
        "last_name": _safe_col(results_df, "LastName"),
        "team_name": _safe_col(results_df, "TeamName"),
        "team_color": _safe_col(results_df, "TeamColor"),
        "team_id": _safe_col(results_df, "TeamId"),
        "headshot_url": _safe_col(results_df, "HeadshotUrl"),
        "country_code": _safe_col(results_df, "CountryCode"),
        "position": _safe_numeric_col(results_df, "Position"),
        "classified_position": _safe_col(results_df, "ClassifiedPosition"),
        "grid_position": _safe_numeric_col(results_df, "GridPosition"),
        "q1": _safe_td_col(results_df, "Q1"),
        "q2": _safe_td_col(results_df, "Q2"),
        "q3": _safe_td_col(results_df, "Q3"),
        "finish_time": _safe_td_col(results_df, "Time"),
        "status": _safe_col(results_df, "Status"),
        "points": pd.to_numeric(results_df["Points"], errors="coerce") if "Points" in results_df.columns else None,
        "laps_completed": _safe_numeric_col(results_df, "Laps"),
    })
    df = df.where(df.notna(), None)
    _bulk_insert(conn, "drivers", df)


def insert_laps(conn, session_id, laps_df):
    if laps_df is None or laps_df.empty:
        return
    df = pd.DataFrame({
        "session_id": session_id,
        "driver_number": laps_df["DriverNumber"].astype(int),
        "lap_number": laps_df["LapNumber"].astype(int),
        "driver_code": _safe_col(laps_df, "Driver"),
        "team": _safe_col(laps_df, "Team"),
        "lap_time": _td_col(laps_df["LapTime"]),
        "sector_1_time": _td_col(laps_df["Sector1Time"]),
        "sector_2_time": _td_col(laps_df["Sector2Time"]),
        "sector_3_time": _td_col(laps_df["Sector3Time"]),
        "sector_1_session_time": _safe_td_col(laps_df, "Sector1SessionTime"),
        "sector_2_session_time": _safe_td_col(laps_df, "Sector2SessionTime"),
        "sector_3_session_time": _safe_td_col(laps_df, "Sector3SessionTime"),
        "speed_i1": pd.to_numeric(laps_df["SpeedI1"], errors="coerce"),
        "speed_i2": pd.to_numeric(laps_df["SpeedI2"], errors="coerce"),
        "speed_fl": pd.to_numeric(laps_df["SpeedFL"], errors="coerce"),
        "speed_st": pd.to_numeric(laps_df["SpeedST"], errors="coerce"),
        "compound": _safe_col(laps_df, "Compound"),
        "tyre_life": _safe_numeric_col(laps_df, "TyreLife"),
        "fresh_tyre": _safe_col(laps_df, "FreshTyre"),
        "stint": _safe_numeric_col(laps_df, "Stint"),
        "position": _safe_numeric_col(laps_df, "Position"),
        "is_personal_best": _safe_col(laps_df, "IsPersonalBest"),
        "is_accurate": _safe_col(laps_df, "IsAccurate"),
        "deleted": _safe_col(laps_df, "Deleted"),
        "deleted_reason": _safe_col(laps_df, "DeletedReason"),
        "fast_f1_generated": _safe_col(laps_df, "FastF1Generated"),
        "track_status": _safe_col(laps_df, "TrackStatus"),
        "pit_in_time": _safe_td_col(laps_df, "PitInTime"),
        "pit_out_time": _safe_td_col(laps_df, "PitOutTime"),
        "lap_start_time": _safe_td_col(laps_df, "LapStartTime"),
        "lap_start_date": _safe_col(laps_df, "LapStartDate"),
    })
    df = df.where(df.notna(), None)
    _bulk_insert(conn, "laps", df)


def insert_car_data_bulk(conn, session_id, car_data_dict):
    if not car_data_dict:
        return
    frames = []
    for drv, car_df in car_data_dict.items():
        if car_df is None or car_df.empty:
            continue
        f = pd.DataFrame({
            "session_id": session_id,
            "driver_number": int(drv),
            "timestamp": _td_col(car_df["Time"]),
            "date": _safe_col(car_df, "Date"),
            "session_time": _safe_td_col(car_df, "SessionTime"),
            "speed": _safe_numeric_col(car_df, "Speed"),
            "rpm": _safe_numeric_col(car_df, "RPM"),
            "n_gear": _safe_numeric_col(car_df, "nGear"),
            "throttle": _safe_numeric_col(car_df, "Throttle"),
            "brake": car_df["Brake"].astype(bool),
            "drs": _safe_numeric_col(car_df, "DRS"),
            "source": _safe_col(car_df, "Source"),
        })
        frames.append(f)
    if not frames:
        return
    df = pd.concat(frames, ignore_index=True)
    df = df.dropna(subset=["timestamp"])
    _bulk_insert(conn, "car_data", df)


def insert_locations_bulk(conn, session_id, pos_data_dict):
    if not pos_data_dict:
        return
    frames = []
    for drv, pos_df in pos_data_dict.items():
        if pos_df is None or pos_df.empty:
            continue
        f = pd.DataFrame({
            "session_id": session_id,
            "driver_number": int(drv),
            "timestamp": _td_col(pos_df["Time"]),
            "date": _safe_col(pos_df, "Date"),
            "session_time": _safe_td_col(pos_df, "SessionTime"),
            "x": pd.to_numeric(pos_df["X"], errors="coerce"),
            "y": pd.to_numeric(pos_df["Y"], errors="coerce"),
            "z": pd.to_numeric(pos_df["Z"], errors="coerce"),
            "status": _safe_col(pos_df, "Status"),
            "source": _safe_col(pos_df, "Source"),
        })
        frames.append(f)
    if not frames:
        return
    df = pd.concat(frames, ignore_index=True)
    df = df.dropna(subset=["timestamp"])
    _bulk_insert(conn, "locations", df)


def insert_intervals(conn, session_id, timing_df):
    if timing_df is None or timing_df.empty:
        return
    required = {"Time", "DriverNumber"}
    if not required.issubset(timing_df.columns):
        return
    df = pd.DataFrame({
        "session_id": session_id,
        "driver_number": pd.to_numeric(timing_df["DriverNumber"], errors="coerce").astype("Int64"),
        "timestamp": _td_col(timing_df["Time"]),
        "position": _safe_numeric_col(timing_df, "Position"),
        "gap_to_leader": timing_df["GapToLeader"].astype(str) if "GapToLeader" in timing_df.columns else None,
        "interval_to_ahead": timing_df["IntervalToPositionAhead"].astype(str) if "IntervalToPositionAhead" in timing_df.columns else None,
    })
    df = df.dropna(subset=["timestamp", "driver_number"])
    for col in ["gap_to_leader", "interval_to_ahead"]:
        if col in df.columns:
            df[col] = df[col].replace({"nan": None, "": None})
    _bulk_insert(conn, "intervals", df)


def insert_weather(conn, session_id, weather_df):
    if weather_df is None or weather_df.empty:
        return
    df = pd.DataFrame({
        "session_id": session_id,
        "timestamp": _td_col(weather_df["Time"]),
        "air_temperature": pd.to_numeric(weather_df["AirTemp"], errors="coerce"),
        "humidity": pd.to_numeric(weather_df["Humidity"], errors="coerce"),
        "pressure": pd.to_numeric(weather_df["Pressure"], errors="coerce"),
        "rainfall": weather_df["Rainfall"].astype(bool),
        "track_temperature": pd.to_numeric(weather_df["TrackTemp"], errors="coerce"),
        "wind_direction": pd.to_numeric(weather_df["WindDirection"], errors="coerce").astype("Int64"),
        "wind_speed": pd.to_numeric(weather_df["WindSpeed"], errors="coerce"),
    })
    df = df.dropna(subset=["timestamp"])
    _bulk_insert(conn, "weather", df)


def insert_race_control(conn, session_id, rc_df):
    if rc_df is None or rc_df.empty:
        return
    n = len(rc_df)
    ids = [r[0] for r in conn.execute(f"SELECT nextval('race_control_id_seq') FROM generate_series(1, {n})").fetchall()]
    time_col = rc_df["Time"]
    if hasattr(time_col.dtype, 'kind') and time_col.dtype.kind == 'm':
        ts = time_col.dt.total_seconds()
    else:
        ts = time_col.apply(to_seconds_or_timestamp)
    df = pd.DataFrame({
        "race_control_id": ids,
        "session_id": session_id,
        "timestamp": ts.values,
        "category": rc_df["Category"].values if "Category" in rc_df.columns else None,
        "message": rc_df["Message"].values if "Message" in rc_df.columns else None,
        "status": rc_df["Status"].values if "Status" in rc_df.columns else None,
        "flag": rc_df["Flag"].values if "Flag" in rc_df.columns else None,
        "scope": rc_df["Scope"].values if "Scope" in rc_df.columns else None,
        "sector": pd.to_numeric(rc_df["Sector"], errors="coerce").astype("Int64").values if "Sector" in rc_df.columns else None,
        "driver_number": pd.to_numeric(rc_df["RacingNumber"], errors="coerce").astype("Int64").values if "RacingNumber" in rc_df.columns else None,
        "lap_number": pd.to_numeric(rc_df["Lap"], errors="coerce").astype("Int64").values if "Lap" in rc_df.columns else None,
    })
    _bulk_insert(conn, "race_control", df)


def insert_session_status(conn, session_id, status_df):
    if status_df is None or status_df.empty:
        return
    df = pd.DataFrame({
        "session_id": session_id,
        "timestamp": _td_col(status_df["Time"]),
        "status": status_df["Status"].values,
    })
    df = df.dropna(subset=["timestamp"])
    _bulk_insert(conn, "session_status", df)


def insert_track_status(conn, session_id, track_df):
    if track_df is None or track_df.empty:
        return
    df = pd.DataFrame({
        "session_id": session_id,
        "timestamp": _td_col(track_df["Time"]),
        "status": track_df["Status"].values if "Status" in track_df.columns else None,
        "message": track_df["Message"].values if "Message" in track_df.columns else None,
    })
    df = df.dropna(subset=["timestamp"])
    _bulk_insert(conn, "track_status", df)


def insert_team_radio(conn, session_id, radio_df):
    if radio_df is None or radio_df.empty:
        return
    n = len(radio_df)
    ids = [r[0] for r in conn.execute(f"SELECT nextval('team_radio_id_seq') FROM generate_series(1, {n})").fetchall()]
    time_col = radio_df["Time"]
    if hasattr(time_col.dtype, 'kind') and time_col.dtype.kind == 'm':
        ts = time_col.dt.total_seconds()
    else:
        ts = time_col.apply(to_seconds_or_timestamp)
    df = pd.DataFrame({
        "team_radio_id": ids,
        "session_id": session_id,
        "driver_number": pd.to_numeric(radio_df["RacingNumber"], errors="coerce").astype("Int64").values if "RacingNumber" in radio_df.columns else None,
        "timestamp": ts.values,
        "recording_url": radio_df["Path"].values if "Path" in radio_df.columns else None,
    })
    _bulk_insert(conn, "team_radio", df)


def insert_ingestion_log(conn, session_id, event_id, year, round_number,
                         session_type, status="complete", error_message=None):
    now = pd.Timestamp.now()
    conn.execute("""
        INSERT INTO ingestion_log (session_id, event_id, year, round_number,
                                   session_type, ingested_at, status, error_message)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (session_id) DO UPDATE SET
            status = EXCLUDED.status,
            error_message = EXCLUDED.error_message,
            ingested_at = EXCLUDED.ingested_at
    """, [session_id, event_id, year, round_number,
          session_type, now, status, error_message])


# --- Team radio pipeline helpers (used by downloader/agent) ---

def upsert_team_radio_file(conn, team_radio_id, file_path):
    file_id = conn.execute("SELECT nextval('team_radio_file_id_seq')").fetchone()[0]
    conn.execute("""
        INSERT INTO team_radio_files (team_radio_file_id, team_radio_id, team_radio_file)
        VALUES (?, ?, ?)
        ON CONFLICT(team_radio_id) DO NOTHING
    """, [file_id, team_radio_id, file_path])


def insert_team_radio_text(conn, team_radio_file_id, team_radio_id, text):
    text_id = conn.execute("SELECT nextval('team_radio_text_id_seq')").fetchone()[0]
    conn.execute("""
        INSERT INTO team_radio_texts (team_radio_text_id, team_radio_file_id, team_radio_id, transcription)
        VALUES (?, ?, ?, ?)
    """, [text_id, team_radio_file_id, team_radio_id, text])


def update_downloader_watermark(conn, watermark):
    conn.execute("UPDATE downloader_watermark SET watermark=? WHERE rowid=0", [watermark])


def insert_failed_download(conn, item_id, url):
    dl_id = conn.execute("SELECT nextval('failed_download_id_seq')").fetchone()[0]
    conn.execute("""
        INSERT INTO failed_downloads (download_id, item_id, url)
        VALUES (?, ?, ?)
        ON CONFLICT (item_id) DO NOTHING
    """, [dl_id, item_id, url])
