"""
Utility helpers for searchable dropdowns in Streamlit / Snowflake.
These functions mirror the approach used in the Cohort Builder app so you can
reuse the same UX patterns (search-as-you-type select boxes that cascade
Database ➜ Schema ➜ Table) inside any Streamlit page.
"""

import snowflake.connector
import pandas as pd
import streamlit as st
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


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------
def _quote(ident: str) -> str:
    """Snowflake identifier quoting."""
    if not ident:
        raise ValueError("Database, schema and table names must not be empty.")
    return '"' + ident.replace('"', '""') + '"'

def fetch_table(sess, db: str, sch: str, tbl: str) -> pd.DataFrame:
    """Load an entire table into a pandas DataFrame, safely quoted."""
    fq_name = f"{_quote(db)}.{_quote(sch)}.{_quote(tbl)}"
    return sess.table(fq_name).to_pandas()  # same as SELECT * FROM "DB"."SCH"."TBL"

def get_common_columns(df1: pd.DataFrame, df2: pd.DataFrame) -> list[str]:
    """Return the ordered list of shared columns (order preserved from *df1*)."""
    return [c for c in df1.columns if c in df2.columns]

def _json_safe(value):
    """JSON-serialisable scalar with null, ISO-8601 datetimes, …"""
    if value is None or pd.isna(value):
        return None
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    return value

def summarise_column_diffs(
    df_base: pd.DataFrame,
    df_updated: pd.DataFrame,
) -> pd.DataFrame:
    """
    Simple per-column summary (unique / new / dropped value counts +
    flag for whether it appears in both tables).
    """
    cols = sorted(set(df_base.columns) | set(df_updated.columns))
    rows: List[Dict[str, object]] = []
    for col in cols:
        base_vals = set(df_base[col].dropna().unique()) if col in df_base.columns else set()
        upd_vals  = set(df_updated[col].dropna().unique()) if col in df_updated.columns else set()
        rows.append({
            "column":         col,
            "base_unique":    len(base_vals),
            "target_unique":  len(upd_vals),
            "new_values":     len(upd_vals - base_vals),
            "dropped_values": len(base_vals - upd_vals),
            "in_both_tables": col in df_base.columns and col in df_updated.columns,
        })
    return pd.DataFrame(rows)
