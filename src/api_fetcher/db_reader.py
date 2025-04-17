import sqlite3

DB_PATH = '../../data/f1_data.db'

class DatabaseReader:

    @classmethod
    def get_sessions_context(cls):
        conn = sqlite3.connect(DB_PATH)
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
    def get_watermarks(cls):
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM watermarks")
        answers = cursor.fetchall()
        conn.close()

        watermarks = {}
        for answer in answers:
            watermarks[answer[0]] = (answer[1], answer[2], answer[3])
        return watermarks