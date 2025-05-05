import sqlite3
import duckdb
import json
from concurrent.futures import ThreadPoolExecutor

DB_PATH_SQLITE = '../../data/f1_data.db'
DB_PATH_DUCKDB = '../../data/f1_data.duckdb'

class DatabaseWriter:

    @classmethod
    def upsert_data(cls, endpoint, data):
        table_methods = {
            "meetings": cls.upsert_meetings,
            "car_data": cls.upsert_car_data,
            "drivers": cls.upsert_drivers,
            "intervals": cls.upsert_intervals,
            "laps": cls.upsert_laps,
            "locations": cls.upsert_locations,
            "pits": cls.upsert_pits,
            "positions": cls.upsert_positions,
            "race_control": cls.upsert_race_control,
            "sessions": cls.upsert_sessions,
            "stints": cls.upsert_stints,
            "team_radio": cls.upsert_team_radio,
            "weather": cls.upsert_weather,
        }
        table_methods[endpoint](data, "duckdb")

        # Use ThreadPoolExecutor to run upserts in parallel
        #with ThreadPoolExecutor() as executor:
        #    executor.submit(upsert_method, data, "sqlite")
        #    executor.submit(upsert_method, data, "duckdb")

    @classmethod
    def get_connection(cls, db_type):
        """
        Returns a database connection for the specified database type.
        """
        if db_type == "sqlite":
            return sqlite3.connect(DB_PATH_SQLITE)
        elif db_type == "duckdb":
            return duckdb.connect(DB_PATH_DUCKDB)
        else:
            raise ValueError(f"Unsupported database type: {db_type}")

    @classmethod
    def upsert_car_data(cls, data, db_type):
        """
        Inserts or updates car data into the database.
        """
        conn = cls.get_connection(db_type)
        cursor = conn.cursor()

        for car_data in data:
            cursor.execute('''
            INSERT INTO car_data (
                brake, date, driver_number, drs, meeting_key,
                n_gear, rpm, session_key, speed, throttle
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(date, driver_number, session_key) DO NOTHING
            ''', (
                car_data['brake'], car_data['date'], car_data['driver_number'],
                car_data['drs'], car_data['meeting_key'], car_data['n_gear'],
                car_data['rpm'], car_data['session_key'], car_data['speed'],
                car_data['throttle']
            ))

        conn.commit()
        conn.close()

    @classmethod
    def upsert_drivers(cls, data, db_type):
        """
        Inserts or updates driver data into the database.
        """
        conn = cls.get_connection(db_type)
        cursor = conn.cursor()

        for driver in data:
            cursor.execute('''
            INSERT INTO drivers (
                broadcast_name, country_code, driver_number, first_name,
                full_name, headshot_url, last_name, meeting_key,
                name_acronym, session_key, team_colour, team_name
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(driver_number, session_key) DO NOTHING
            ''', (
                driver['broadcast_name'], driver['country_code'],
                driver['driver_number'], driver['first_name'],
                driver['full_name'], driver['headshot_url'],
                driver['last_name'], driver['meeting_key'],
                driver['name_acronym'], driver['session_key'],
                driver['team_colour'], driver['team_name']
            ))

        conn.commit()
        conn.close()

    @classmethod
    def upsert_intervals(cls, data, db_type):
        """
        Inserts or updates interval data into the database.
        """
        conn = cls.get_connection(db_type)
        cursor = conn.cursor()

        for interval in data:
            cursor.execute('''
            INSERT INTO intervals (
                date, driver_number, gap_to_leader, interval,
                meeting_key, session_key
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(date, driver_number, session_key) DO NOTHING
            ''', (
                interval['date'], interval['driver_number'],
                interval['gap_to_leader'], interval['interval'],
                interval['meeting_key'], interval['session_key']
            ))

        conn.commit()
        conn.close()

    @classmethod
    def upsert_laps(cls, data, db_type):
        """
        Inserts or updates lap data into the database.
        """
        conn = cls.get_connection(db_type)
        cursor = conn.cursor()

        for lap in data:
            cursor.execute('''
            INSERT INTO laps (
                date_start, driver_number, duration_sector_1, duration_sector_2,
                duration_sector_3, i1_speed, i2_speed, is_pit_out_lap,
                lap_duration, lap_number, meeting_key, segments_sector_1,
                segments_sector_2, segments_sector_3, session_key, st_speed
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(lap_number, driver_number, session_key) DO NOTHING
            ''', (
                lap['date_start'], lap['driver_number'], lap['duration_sector_1'],
                lap['duration_sector_2'], lap['duration_sector_3'], lap['i1_speed'],
                lap['i2_speed'], lap['is_pit_out_lap'], lap['lap_duration'],
                lap['lap_number'], lap['meeting_key'],
                json.dumps(lap['segments_sector_1']),
                json.dumps(lap['segments_sector_2']),
                json.dumps(lap['segments_sector_3']),
                lap['session_key'], lap['st_speed']
            ))

        conn.commit()
        conn.close()

    @classmethod
    def upsert_locations(cls, data, db_type):
        """
        Inserts or updates location data into the database.
        """
        conn = cls.get_connection(db_type)
        cursor = conn.cursor()

        for location in data:
            cursor.execute('''
            INSERT INTO locations (
                date, driver_number, meeting_key, session_key, x, y, z
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(date, driver_number, session_key) DO NOTHING
            ''', (
                location['date'], location['driver_number'],
                location['meeting_key'], location['session_key'],
                location['x'], location['y'], location['z']
            ))

        conn.commit()
        conn.close()

    @classmethod
    def upsert_meetings(cls, data, db_type):
        conn = cls.get_connection(db_type)
        cursor = conn.cursor()

        for meeting in data:
            cursor.execute('''
            INSERT INTO meetings (
                circuit_key, circuit_short_name, country_code, country_key,
                country_name, date_start, gmt_offset, location, meeting_key,
                meeting_name, meeting_official_name, year)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(meeting_key) DO NOTHING
            ''', (
                meeting['circuit_key'], meeting['circuit_short_name'],
                meeting['country_code'], meeting['country_key'],
                meeting['country_name'], meeting['date_start'],
                meeting['gmt_offset'], meeting['location'],
                meeting['meeting_key'], meeting['meeting_name'],
                meeting['meeting_official_name'], meeting['year']
            ))

        conn.commit()
        conn.close()

    @classmethod
    def upsert_pits(cls, data, db_type):
        """
        Inserts or updates pit data into the database.
        """
        conn = cls.get_connection(db_type)
        cursor = conn.cursor()

        for pit in data:
            cursor.execute('''
            INSERT INTO pits (
                date, driver_number, lap_number, meeting_key,
                pit_duration, session_key
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(date, driver_number, session_key) DO NOTHING
            ''', (
                pit['date'], pit['driver_number'], pit['lap_number'],
                pit['meeting_key'], pit['pit_duration'], pit['session_key']
            ))

        conn.commit()
        conn.close()

    @classmethod
    def upsert_positions(cls, data, db_type):
        """
        Inserts or updates position data into the database.
        """
        conn = cls.get_connection(db_type)
        cursor = conn.cursor()

        for position in data:
            cursor.execute('''
            INSERT INTO positions (
                date, driver_number, meeting_key, position, session_key
            )
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(date, driver_number, session_key) DO NOTHING
            ''', (
                position['date'], position['driver_number'],
                position['meeting_key'], position['position'],
                position['session_key']
            ))

        conn.commit()
        conn.close()

    @classmethod
    def upsert_race_control(cls, data, db_type):
        """
        Inserts or updates race control data into the database.
        """
        conn = cls.get_connection(db_type)
        cursor = conn.cursor()

        for event in data:
            cursor.execute('''
            INSERT INTO race_control (
                category, date, driver_number, flag, lap_number,
                meeting_key, message, scope, sector, session_key
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(date, category, message) DO NOTHING
            ''', (
                event['category'], event['date'], event['driver_number'],
                event['flag'], event['lap_number'], event['meeting_key'],
                event['message'], event['scope'], event['sector'],
                event['session_key']
            ))

        conn.commit()
        conn.close()

    @classmethod
    def upsert_sessions(cls, data, db_type):
        """
        Inserts or updates sessions data into the database.
        """
        conn = cls.get_connection(db_type)
        cursor = conn.cursor()

        for session in data:
            cursor.execute('''
            INSERT INTO sessions (
                circuit_key, circuit_short_name, country_code, country_key,
                country_name, date_end, date_start, gmt_offset, location,
                meeting_key, session_key, session_name, session_type, year
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(session_key) DO NOTHING
            ''', (
                session['circuit_key'], session['circuit_short_name'],
                session['country_code'], session['country_key'],
                session['country_name'], session['date_end'],
                session['date_start'], session['gmt_offset'],
                session['location'], session['meeting_key'],
                session['session_key'], session['session_name'],
                session['session_type'], session['year']
            ))

        conn.commit()
        conn.close()

    @classmethod
    def upsert_stints(cls, data, db_type):
        conn = cls.get_connection(db_type)
        cursor = conn.cursor()

        for weather in data:
            cursor.execute('''
            INSERT INTO stints (
                compound, driver_number, lap_end, lap_start,
                meeting_key, session_key, stint_number, tyre_age_at_start)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(meeting_key, session_key, driver_number, stint_number) DO NOTHING
            ''', (
                weather['compound'], weather['driver_number'],
                weather['lap_end'], weather['lap_start'],
                weather['meeting_key'], weather['session_key'],
                weather['stint_number'], weather['tyre_age_at_start']
            ))

        conn.commit()
        conn.close()

    @classmethod
    def upsert_team_radio(cls, data, db_type):
        """
        Inserts or updates team radio data into the database.
        """
        conn = cls.get_connection(db_type)
        cursor = conn.cursor()

        for radio in data:
            cursor.execute('''
            INSERT INTO team_radio (
                date, driver_number, meeting_key, recording_url, session_key
            )
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(date, driver_number, session_key) DO NOTHING
            ''', (
                radio['date'], radio['driver_number'], radio['meeting_key'],
                radio['recording_url'], radio['session_key']
            ))

        conn.commit()
        conn.close()
    
    @classmethod
    def upsert_weather(cls, data, db_type):
        conn = cls.get_connection(db_type)
        cursor = conn.cursor()

        for weather in data:
            cursor.execute('''
            INSERT INTO weather (
                meeting_key, session_key, date, air_temperature, humidity,
                pressure, rainfall, track_temperature, wind_direction, wind_speed)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(meeting_key, session_key, date) DO NOTHING
            ''', (
                weather['meeting_key'], weather['session_key'],
                weather['date'], weather['air_temperature'],
                weather['humidity'], weather['pressure'],
                weather['rainfall'], weather['track_temperature'],
                weather['wind_direction'], weather['wind_speed']
            ))

        conn.commit()
        conn.close()

    @classmethod
    def insert_failed_query(cls, endpoint_name, api_url, db_type="duckdb"):
        """
        Inserts failed query into the database.
        """
        conn = cls.get_connection(db_type)
        cursor = conn.cursor()

        cursor.execute('''
        INSERT INTO failed_queries (endpoint_name, query)
        VALUES (?, ?)
        ''', (endpoint_name, api_url))

        conn.commit()
        conn.close()
    
    @classmethod
    def delete_failed_query(cls, id, db_type="duckdb"):
        """
        Deletes failed query from the database.
        """
        conn = cls.get_connection(db_type)
        cursor = conn.cursor()

        cursor.execute('''
        DELETE FROM failed_queries WHERE query_id=?
        ''', (id,))

        conn.commit()
        conn.close()

    @classmethod
    def insert_failed_download(cls, item_id, url, db_type="duckdb"):
        """
        Inserts failed query into the database.
        """
        conn = cls.get_connection(db_type)
        cursor = conn.cursor()

        cursor.execute('''
        INSERT INTO failed_downloads (item_id, url)
        VALUES (?, ?)
        ''', (item_id, url))

        conn.commit()
        conn.close()
    
    @classmethod
    def delete_failed_download(cls, id, db_type="duckdb"):
        """
        Deletes failed query from the database.
        """
        conn = cls.get_connection(db_type)
        cursor = conn.cursor()

        cursor.execute('''
        DELETE FROM failed_downloads WHERE download_id=?
        ''', (id,))

        conn.commit()
        conn.close()

    @classmethod
    def update_downloader_watermark(cls, watermark, db_type="duckdb"):
        conn = cls.get_connection(db_type)
        cursor = conn.cursor()

        cursor.execute('''
        UPDATE downloader_watermark SET watermark=? WHERE rowid=0
        ''', (watermark,))

        conn.commit()
        conn.close()

    @classmethod
    def upsert_team_radio_file(cls, team_radio_id, file_path, db_type="duckdb"):
        conn = cls.get_connection(db_type)
        cursor = conn.cursor()

        cursor.execute('''
        INSERT INTO team_radio_files (team_radio_id, team_radio_file)
        VALUES (?, ?)
        ON CONFLICT(team_radio_id) DO NOTHING
        ''', (team_radio_id, file_path))

        conn.commit()
        conn.close()
    
    @classmethod
    def insert_team_radio_text(cls, team_radio_file_id, team_radio_id, text, db_type="duckdb"):
        conn = cls.get_connection(db_type)
        cursor = conn.cursor()

        cursor.execute('''
        INSERT INTO team_radio_texts (team_radio_file_id, team_radio_id, transcription)
        VALUES (?, ?, ?)
        ''', (team_radio_file_id, team_radio_id, text))

        conn.commit()
        conn.close()