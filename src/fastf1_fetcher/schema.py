import duckdb

DB_PATH = '../../data/f1_data_v2.duckdb'

TABLES = {
    "events": """
        CREATE TABLE IF NOT EXISTS events (
            event_id INTEGER PRIMARY KEY,
            year INTEGER NOT NULL,
            round_number INTEGER NOT NULL,
            country VARCHAR,
            location VARCHAR,
            event_name VARCHAR,
            event_date DATE,
            event_format VARCHAR
        );
    """,
    "sessions": """
        CREATE TABLE IF NOT EXISTS sessions (
            session_id INTEGER PRIMARY KEY,
            event_id INTEGER NOT NULL REFERENCES events(event_id),
            year INTEGER NOT NULL,
            round_number INTEGER NOT NULL,
            session_type VARCHAR,
            session_name VARCHAR,
            date_start TIMESTAMP,
            date_end TIMESTAMP,
            total_laps INTEGER,
            session_start_time DOUBLE
        );
    """,
    "drivers": """
        CREATE TABLE IF NOT EXISTS drivers (
            session_id INTEGER NOT NULL,
            driver_number INTEGER NOT NULL,
            driver_code VARCHAR,
            driver_id VARCHAR,
            broadcast_name VARCHAR,
            full_name VARCHAR,
            first_name VARCHAR,
            last_name VARCHAR,
            team_name VARCHAR,
            team_color VARCHAR,
            team_id VARCHAR,
            headshot_url VARCHAR,
            country_code VARCHAR,
            position INTEGER,
            classified_position VARCHAR,
            grid_position INTEGER,
            q1 DOUBLE,
            q2 DOUBLE,
            q3 DOUBLE,
            finish_time DOUBLE,
            status VARCHAR,
            points DOUBLE,
            laps_completed INTEGER,
            PRIMARY KEY (session_id, driver_number)
        );
    """,
    "laps": """
        CREATE TABLE IF NOT EXISTS laps (
            session_id INTEGER NOT NULL,
            driver_number INTEGER NOT NULL,
            lap_number INTEGER NOT NULL,
            driver_code VARCHAR,
            team VARCHAR,
            lap_time DOUBLE,
            sector_1_time DOUBLE,
            sector_2_time DOUBLE,
            sector_3_time DOUBLE,
            sector_1_session_time DOUBLE,
            sector_2_session_time DOUBLE,
            sector_3_session_time DOUBLE,
            speed_i1 DOUBLE,
            speed_i2 DOUBLE,
            speed_fl DOUBLE,
            speed_st DOUBLE,
            compound VARCHAR,
            tyre_life INTEGER,
            fresh_tyre BOOLEAN,
            stint INTEGER,
            position INTEGER,
            is_personal_best BOOLEAN,
            is_accurate BOOLEAN,
            deleted BOOLEAN,
            deleted_reason VARCHAR,
            fast_f1_generated BOOLEAN,
            track_status VARCHAR,
            pit_in_time DOUBLE,
            pit_out_time DOUBLE,
            lap_start_time DOUBLE,
            lap_start_date TIMESTAMP,
            PRIMARY KEY (session_id, driver_number, lap_number)
        );
    """,
    "car_data": """
        CREATE TABLE IF NOT EXISTS car_data (
            session_id INTEGER NOT NULL,
            driver_number INTEGER NOT NULL,
            timestamp DOUBLE NOT NULL,
            date TIMESTAMP,
            session_time DOUBLE,
            speed INTEGER,
            rpm INTEGER,
            n_gear INTEGER,
            throttle INTEGER,
            brake BOOLEAN,
            drs INTEGER,
            source VARCHAR,
            PRIMARY KEY (session_id, driver_number, timestamp)
        );
    """,
    "locations": """
        CREATE TABLE IF NOT EXISTS locations (
            session_id INTEGER NOT NULL,
            driver_number INTEGER NOT NULL,
            timestamp DOUBLE NOT NULL,
            date TIMESTAMP,
            session_time DOUBLE,
            x DOUBLE,
            y DOUBLE,
            z DOUBLE,
            status VARCHAR,
            source VARCHAR,
            PRIMARY KEY (session_id, driver_number, timestamp)
        );
    """,
    "intervals": """
        CREATE TABLE IF NOT EXISTS intervals (
            session_id INTEGER NOT NULL,
            driver_number INTEGER NOT NULL,
            timestamp DOUBLE NOT NULL,
            position INTEGER,
            gap_to_leader VARCHAR,
            interval_to_ahead VARCHAR,
            PRIMARY KEY (session_id, driver_number, timestamp)
        );
    """,
    "weather": """
        CREATE TABLE IF NOT EXISTS weather (
            session_id INTEGER NOT NULL,
            timestamp DOUBLE NOT NULL,
            air_temperature DOUBLE,
            humidity DOUBLE,
            pressure DOUBLE,
            rainfall BOOLEAN,
            track_temperature DOUBLE,
            wind_direction INTEGER,
            wind_speed DOUBLE,
            PRIMARY KEY (session_id, timestamp)
        );
    """,
    "race_control": """
        CREATE TABLE IF NOT EXISTS race_control (
            race_control_id INTEGER PRIMARY KEY,
            session_id INTEGER NOT NULL,
            timestamp DOUBLE,
            category VARCHAR,
            message VARCHAR,
            status VARCHAR,
            flag VARCHAR,
            scope VARCHAR,
            sector INTEGER,
            driver_number INTEGER,
            lap_number INTEGER,
            UNIQUE (session_id, timestamp, category, message)
        );
    """,
    "session_status": """
        CREATE TABLE IF NOT EXISTS session_status (
            session_id INTEGER NOT NULL,
            timestamp DOUBLE NOT NULL,
            status VARCHAR,
            PRIMARY KEY (session_id, timestamp)
        );
    """,
    "track_status": """
        CREATE TABLE IF NOT EXISTS track_status (
            session_id INTEGER NOT NULL,
            timestamp DOUBLE NOT NULL,
            status VARCHAR,
            message VARCHAR,
            PRIMARY KEY (session_id, timestamp)
        );
    """,
    "team_radio": """
        CREATE TABLE IF NOT EXISTS team_radio (
            team_radio_id INTEGER PRIMARY KEY,
            session_id INTEGER NOT NULL,
            driver_number INTEGER,
            timestamp DOUBLE,
            recording_url VARCHAR,
            UNIQUE (session_id, driver_number, timestamp)
        );
    """,
    "team_radio_files": """
        CREATE TABLE IF NOT EXISTS team_radio_files (
            team_radio_file_id INTEGER PRIMARY KEY,
            team_radio_id INTEGER NOT NULL,
            team_radio_file VARCHAR NOT NULL,
            UNIQUE(team_radio_id)
        );
    """,
    "team_radio_texts": """
        CREATE TABLE IF NOT EXISTS team_radio_texts (
            team_radio_text_id INTEGER PRIMARY KEY,
            team_radio_file_id INTEGER NOT NULL,
            team_radio_id INTEGER NOT NULL,
            transcription VARCHAR NOT NULL
        );
    """,
    "ingestion_log": """
        CREATE TABLE IF NOT EXISTS ingestion_log (
            session_id INTEGER PRIMARY KEY,
            event_id INTEGER NOT NULL,
            year INTEGER NOT NULL,
            round_number INTEGER NOT NULL,
            session_type VARCHAR,
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status VARCHAR NOT NULL DEFAULT 'complete',
            error_message VARCHAR
        );
    """,
    "failed_downloads": """
        CREATE TABLE IF NOT EXISTS failed_downloads (
            download_id INTEGER PRIMARY KEY,
            item_id INTEGER UNIQUE,
            url VARCHAR
        );
    """,
    "downloader_watermark": """
        CREATE TABLE IF NOT EXISTS downloader_watermark (
            watermark INTEGER DEFAULT 0
        );
    """,
}

SEQUENCES = [
    "CREATE SEQUENCE IF NOT EXISTS race_control_id_seq;",
    "CREATE SEQUENCE IF NOT EXISTS team_radio_id_seq;",
    "CREATE SEQUENCE IF NOT EXISTS team_radio_file_id_seq;",
    "CREATE SEQUENCE IF NOT EXISTS team_radio_text_id_seq;",
    "CREATE SEQUENCE IF NOT EXISTS failed_download_id_seq;",
]


def init_schema(db_path=DB_PATH):
    conn = duckdb.connect(db_path)
    for seq in SEQUENCES:
        conn.execute(seq)
    for table_name, ddl in TABLES.items():
        conn.execute(ddl)
    count = conn.execute("SELECT COUNT(*) FROM downloader_watermark").fetchone()[0]
    if count == 0:
        conn.execute("INSERT INTO downloader_watermark VALUES (0)")
    conn.close()


if __name__ == "__main__":
    init_schema()
    print("Schema initialized successfully.")
