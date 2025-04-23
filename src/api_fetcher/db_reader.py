import sqlite3
import duckdb

DB_PATH_SQLITE = '../../data/f1_data.db'
DB_PATH_DUCKDB = '../../data/f1_data.duckdb'

class DatabaseReader:

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
    def get_sessions_context(cls, db_type="duckdb"):
        conn = cls.get_connection(db_type)
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM core__sessions_context")
        sessions_context = cursor.fetchall()
        conn.close()
        for i in range(len(sessions_context)):
            s = list(sessions_context[i]) 
            s[-1] = s[-1].split(',')
            sessions_context[i] = s
        return sessions_context
    
    @classmethod
    def get_watermarks(cls, db_type="duckdb"):
        conn = cls.get_connection(db_type)
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM watermarks")
        answers = cursor.fetchall()
        conn.close()

        watermarks = {}
        for answer in answers:
            watermarks[answer[0]] = (answer[1], answer[2], answer[3])
        return watermarks
    
    @classmethod
    def get_failed_queries(cls, db_type="duckdb"):
        conn = cls.get_connection(db_type)
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM failed_queries")
        answers = cursor.fetchall()
        conn.close()

        failed_queries = {}
        for answer in answers:
            failed_queries[answer[0]] = (answer[1], answer[2])
        return failed_queries
    
    @classmethod
    def get_downloader_watermark(cls, db_type="duckdb"):
        conn = cls.get_connection(db_type)
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM downloader_watermark")
        answer = cursor.fetchone()
        conn.close()

        return answer[0]
    
    @classmethod
    def get_team_radios(cls, team_radio_watermark=0, db_type="duckdb"):
        conn = cls.get_connection(db_type)
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM team_radio WHERE team_radio_id > ?", (team_radio_watermark,))
        answers = cursor.fetchall()
        conn.close()

        team_radios = {}
        for answer in answers:
            team_radios[answer[5]] = (answer[0], answer[1], answer[2], answer[3], answer[4])

        return team_radios
    
    @classmethod
    def get_team_radio_files(cls, db_type="duckdb"):
        conn = cls.get_connection(db_type)
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM team_radio_files")
        answers = cursor.fetchall()
        conn.close()

        team_radio_files = []
        for answer in answers:
            team_radio_files.append(answer[2])

        return team_radio_files