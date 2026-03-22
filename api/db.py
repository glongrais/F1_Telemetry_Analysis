from pathlib import Path
from typing import List, Optional

import duckdb

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "f1_data_v2.duckdb"


def get_connection() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(DB_PATH), read_only=True)


def query(sql: str, params: Optional[List] = None) -> List[dict]:
    conn = get_connection()
    try:
        result = conn.execute(sql, params or [])
        columns = [desc[0] for desc in result.description]
        return [dict(zip(columns, row)) for row in result.fetchall()]
    finally:
        conn.close()
