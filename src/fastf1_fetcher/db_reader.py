import os
import duckdb

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'data', 'f1_data_v2.duckdb')


def get_connection(db_path=DB_PATH):
    return duckdb.connect(db_path)


def get_ingested_sessions(db_path=DB_PATH):
    conn = get_connection(db_path)
    rows = conn.execute(
        "SELECT session_id FROM ingestion_log WHERE status = 'complete'"
    ).fetchall()
    conn.close()
    return {row[0] for row in rows}


def get_failed_sessions(db_path=DB_PATH):
    conn = get_connection(db_path)
    rows = conn.execute(
        "SELECT session_id, event_id, year, round_number, session_type, error_message "
        "FROM ingestion_log WHERE status = 'failed'"
    ).fetchall()
    conn.close()
    return rows


def get_downloader_watermark(db_path=DB_PATH):
    conn = get_connection(db_path)
    result = conn.execute("SELECT watermark FROM downloader_watermark").fetchone()
    conn.close()
    return result[0] if result else 0


def get_team_radios(watermark=0, db_path=DB_PATH):
    conn = get_connection(db_path)
    rows = conn.execute(
        "SELECT team_radio_id, session_id, driver_number, recording_url "
        "FROM team_radio WHERE team_radio_id > ? ORDER BY team_radio_id",
        [watermark]
    ).fetchdf().to_dict('records')
    conn.close()
    return rows


def get_team_radio_files(db_path=DB_PATH):
    conn = get_connection(db_path)
    rows = conn.execute(
        "SELECT f.team_radio_file_id, f.team_radio_id, f.team_radio_file "
        "FROM team_radio_files f "
        "WHERE f.team_radio_file_id NOT IN (SELECT team_radio_file_id FROM team_radio_texts)"
    ).fetchdf().to_dict('records')
    conn.close()
    return rows
