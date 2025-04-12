import sqlite3
import json

DB_PATH = '../../data/f1_data.db'

class DatabaseWriter:

    @classmethod
    def upsert_car_data(cls, data):
        """
        Inserts or updates car data into the database.
        """
        conn = sqlite3.connect(DB_PATH)
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
    def upsert_drivers(cls, data):
        """
        Inserts or updates driver data into the database.
        """
        conn = sqlite3.connect(DB_PATH)
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
    def upsert_intervals(cls, data):
        """
        Inserts or updates interval data into the database.
        """
        conn = sqlite3.connect(DB_PATH)
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
    def upsert_laps(cls, data):
        """
        Inserts or updates lap data into the database.
        """
        conn = sqlite3.connect(DB_PATH)
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
    def upsert_locations(cls, data):
        """
        Inserts or updates location data into the database.
        """
        conn = sqlite3.connect(DB_PATH)
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
    def upsert_meetings(cls, data):
        conn = sqlite3.connect(DB_PATH)
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
    def upsert_pits(cls, data):
        """
        Inserts or updates pit data into the database.
        """
        conn = sqlite3.connect(DB_PATH)
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
    def upsert_positions(cls, data):
        """
        Inserts or updates position data into the database.
        """
        conn = sqlite3.connect(DB_PATH)
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
    def upsert_race_control(cls, data):
        """
        Inserts or updates race control data into the database.
        """
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        for event in data:
            cursor.execute('''
            INSERT INTO race_control (
                category, date, driver_number, flag, lap_number,
                meeting_key, message, scope, sector, session_key
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(date, driver_number, session_key) DO NOTHING
            ''', (
                event['category'], event['date'], event['driver_number'],
                event['flag'], event['lap_number'], event['meeting_key'],
                event['message'], event['scope'], event['sector'],
                event['session_key']
            ))

        conn.commit()
        conn.close()

    @classmethod
    def upsert_sessions(cls, data):
        """
        Inserts or updates sessions data into the database.
        """
        conn = sqlite3.connect(DB_PATH)
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
    def upsert_stints(cls, data):
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        for weather in data:
            cursor.execute('''
            INSERT OR REPLACE INTO stints (
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
    def upsert_team_radio(cls, data):
        """
        Inserts or updates team radio data into the database.
        """
        conn = sqlite3.connect(DB_PATH)
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
    def upsert_weather(cls, data):
        conn = sqlite3.connect(DB_PATH)
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