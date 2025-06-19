"""
Utility helpers for searchable dropdowns in Streamlit / Snowflake.
These functions mirror the approach used in the Cohort Builder app so you can
reuse the same UX patterns (search-as-you-type select boxes that cascade
Database ➜ Schema ➜ Table) inside any Streamlit page.
"""

import snowflake.connector
import pandas as pd
import streamlit as st
from snowflake.snowpark import Session
from typing import List, Dict, Callable, Any
from datetime import datetime, timezone


def quote_identifier(ident: str) -> str:
    """
    Safely quote a Snowflake identifier.

    Ensures the provided identifier is wrapped in double quotes,
    escaping any internal quotes per Snowflake syntax.

    Args:
        ident (str): The database, schema, or table identifier.

    Returns:
        str: The safely quoted identifier.

    Raises:
        ValueError: If the identifier is an empty string.
    """
    if not ident:
        raise ValueError("Database, schema and table names must not be empty.")
    return '"' + ident.replace('"', '""') + '"'


@st.cache_data(show_spinner=False)
def get_table_columns(
    session: Session,
    db: str,
    sch: str,
    tbl: str
) -> List[str]:
    """
    Retrieve the column names for a given Snowflake table and cache the result.

    Args:
        session (Session): An active Snowpark session.
        db (str): Database name.
        sch (str): Schema name.
        tbl (str): Table name.

    Returns:
        List[str]: A list of column names in order.

    This function is cached to avoid repeated metadata queries for tables whose schemas
    do not change frequently.
    """
    fq = f"{quote_identifier(db)}.{quote_identifier(sch)}.{quote_identifier(tbl)}"
    sql = (
        f"SELECT COLUMN_NAME "
        f"FROM {db}.INFORMATION_SCHEMA.COLUMNS "
        f"WHERE TABLE_SCHEMA = '{sch}' AND TABLE_NAME = '{tbl}' "
        f"ORDER BY ORDINAL_POSITION"
    )
    rows = session.sql(sql).collect()
    return [r[0] for r in rows]


def build_diff_queries(
    src: str,
    tgt: str,
    keys: List[str],
    common_cols: List[str],
) -> Dict[str, Callable[..., str]]:
    """
    Build SQL templates for computing diffs between two tables.

    Constructs a CTE (Common Table Expression) that full-outer-joins
    source and target on the given keys, and labels each row as 'new',
    'dropped', 'changed', or 'same'.

    Args:
        src (str): Fully qualified source table (e.g., 'DB.SCH.TBL').
        tgt (str): Fully qualified target table.
        keys (List[str]): List of join key column names.
        common_cols (List[str]): List of all shared column names.

    Returns:
        Dict[str, Callable[..., str]]: A dictionary containing:
            - 'new_count': SQL string to count new records.
            - 'dropped_count': SQL string to count dropped records.
            - 'changed_count': SQL string to count changed records.
            - 'page_sql': Function(diff_type, limit, offset) returning SQL for paginated results.
    """
    # Quote join keys
    q_keys = [quote_identifier(k) for k in keys]
    on_clause = " AND ".join([f"src.{q} = tgt.{q}" for q in q_keys])

    # Identify non-key columns for change detection
    non_key_cols = [c for c in common_cols if c not in keys]

    # Build unified key selection
    unified_keys = ", ".join([
        f"COALESCE(src.{quote_identifier(k)}, tgt.{quote_identifier(k)}) AS {quote_identifier(k)}"
        for k in keys
    ])

    # Build diff CASE expression
    conditions = " OR ".join([
        f"src.{quote_identifier(c)} IS DISTINCT FROM tgt.{quote_identifier(c)}"
        for c in non_key_cols
    ]) or "FALSE"
    diff_case = (
        f"CASE "
        f"WHEN src.{q_keys[0]} IS NULL THEN 'new' "
        f"WHEN tgt.{q_keys[0]} IS NULL THEN 'dropped' "
        f"WHEN ({conditions}) THEN 'changed' "
        f"ELSE 'same' END AS diff_type"
    )

    # Construct the CTE
    cte = (
        f"WITH diffs AS ("
        f"SELECT {unified_keys}, {diff_case} "
        f"FROM {src} src FULL OUTER JOIN {tgt} tgt ON {on_clause}"
        f")"
    )

    # Lambda to generate count queries
    def count_query(diff_type: str) -> str:
        return f"{cte} SELECT COUNT(*) AS cnt FROM diffs WHERE diff_type = '{diff_type}'"

    # Function to generate page SQL
    def page_sql(diff_type: str, limit: int, offset: int) -> str:
        order_cols = ", ".join(q_keys)
        return (
            f"{cte} SELECT * FROM diffs WHERE diff_type = '{diff_type}' "
            f"ORDER BY {order_cols} LIMIT {limit} OFFSET {offset}"
        )

    return {
        'new_count':     count_query('new'),
        'dropped_count': count_query('dropped'),
        'changed_count': count_query('changed'),
        'page_sql':      page_sql,
    }

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

@st.cache_data(show_spinner=False)
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
