"""Utility helpers for searchable dropdowns in Streamlit / Snowflake.

These functions mirror the approach used in the Cohort Builder app so you can
reuse the same UX patterns (search-as-you-type select boxes that cascade
Database ➜ Schema ➜ Table) inside any Streamlit page.
"""

import snowflake.connector
from typing import List

# ---------------------------------------------------------------------------
# LOW-LEVEL HELPER
# ---------------------------------------------------------------------------

def read_sql(conn: snowflake.connector.SnowflakeConnection, query: str, col: int = 0) -> List[str]:
    """Run *query* and return column *col* as a Python list."""
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    return [row[col] for row in rows]

# ---------------------------------------------------------------------------
# CASCADING DROPDOWN QUERIES
# ---------------------------------------------------------------------------

def list_databases(conn) -> List[str]:
    """Return all databases visible to the current Snowflake role."""
    return read_sql(conn, "SHOW DATABASES", 1)

def list_schemas(conn, database: str) -> List[str]:
    """Return schemas for *database*."""
    q = f"SHOW SCHEMAS IN DATABASE {database}"
    return read_sql(conn, q, 1)

def list_tables(conn, database: str, schema: str) -> List[str]:
    """Return tables & views for given database.schema."""
    q = (
        f"SELECT TABLE_NAME\n"
        f"FROM {database}.INFORMATION_SCHEMA.TABLES\n"
        f"WHERE TABLE_SCHEMA = '{schema}'\n"
        f"  AND TABLE_TYPE IN ('BASE TABLE','VIEW')\n"
        f"ORDER BY 1"
    )
    return read_sql(conn, q, 0)

# ---------------------------------------------------------------------------
# STREAMLIT WIDGET RENDERERS (OPTIONAL)
# ---------------------------------------------------------------------------

def database_selectbox(st, conn, key="db_select") -> str:
    dbs = [""] + list_databases(conn)
    return st.selectbox("Database", dbs, index=dbs.index(st.session_state.get(key, "")), key=key)

def schema_selectbox(st, conn, database: str, key="schema_select") -> str:
    schemas = [""] + (list_schemas(conn, database) if database else [])
    return st.selectbox("Schema", schemas, index=schemas.index(st.session_state.get(key, "")), key=key)

def table_multiselect(st, conn, database: str, schema: str, key="table_select") -> List[str]:
    tables = list_tables(conn, database, schema) if database and schema else []
    return st.multiselect("Table(s)", tables, default=st.session_state.get(key, []), key=key)
