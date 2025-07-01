"""
Optimized utility helpers for Snowflake-Streamlit integration
"""

import snowflake.connector
import pandas as pd
import streamlit as st
from snowflake.snowpark import Session
from typing import List, Dict, Callable, Any, Tuple
from datetime import datetime, timezone

@st.cache_data(show_spinner=False, ttl=3600)
def quote_identifier(ident: str) -> str:
    """Safely quote a Snowflake identifier"""
    if not ident:
        raise ValueError("Database, schema and table names must not be empty.")
    return '"' + ident.replace('"', '""') + '"'

@st.cache_data(show_spinner=False, ttl=3600)
def get_table_columns(
    _session: Session,
    db: str,
    sch: str,
    tbl: str
) -> List[str]:
    """Retrieve column names for a Snowflake table"""
    try:
        sql = (
            f"SELECT COLUMN_NAME "
            f"FROM {db}.INFORMATION_SCHEMA.COLUMNS "
            f"WHERE TABLE_SCHEMA = '{sch}' AND TABLE_NAME = '{tbl}' "
            f"ORDER BY ORDINAL_POSITION"
        )
        rows = _session.sql(sql).collect()
        return [r[0] for r in rows]
    except Exception:
        return []

@st.cache_data(show_spinner=False, ttl=3600)
def get_common_columns_optimized(
    _session: Session,
    db1: str, sch1: str, tbl1: str,
    db2: str, sch2: str, tbl2: str
) -> List[str]:
    """Get common columns between two tables using metadata"""
    cols1 = get_table_columns(_session, db1, sch1, tbl1)
    cols2 = get_table_columns(_session, db2, sch2, tbl2)
    return list(set(cols1) & set(cols2))

@st.cache_data(show_spinner=False, ttl=3600)
def summarise_column_diffs_optimized(
    _session: Session,
    db1: str, sch1: str, tbl1: str,
    db2: str, sch2: str, tbl2: str
) -> pd.DataFrame:
    """Column-level summary using metadata queries"""
    # Get column metadata for both tables
    meta1 = _session.sql(f"""
        SELECT COLUMN_NAME, DATA_TYPE 
        FROM {db1}.INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = '{sch1}' AND TABLE_NAME = '{tbl1}'
    """).to_pandas()

    meta2 = _session.sql(f"""
        SELECT COLUMN_NAME, DATA_TYPE 
        FROM {db2}.INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = '{sch2}' AND TABLE_NAME = '{tbl2}'
    """).to_pandas()

    # Find common columns
    common_cols = list(set(meta1['COLUMN_NAME']) & set(meta2['COLUMN_NAME']))

    # Build summary
    rows = []
    for col in common_cols:
        dtype1 = meta1[meta1['COLUMN_NAME'] == col]['DATA_TYPE'].values[0]
        dtype2 = meta2[meta2['COLUMN_NAME'] == col]['DATA_TYPE'].values[0]

        rows.append({
            "column": col,
            "source_type": dtype1,
            "target_type": dtype2,
            "type_match": dtype1 == dtype2,
            "in_both": True
        })

    # Add columns only in source
    for col in set(meta1['COLUMN_NAME']) - set(meta2['COLUMN_NAME']):
        dtype = meta1[meta1['COLUMN_NAME'] == col]['DATA_TYPE'].values[0]
        rows.append({
            "column": col,
            "source_type": dtype,
            "target_type": None,
            "type_match": False,
            "in_both": False
        })

    # Add columns only in target
    for col in set(meta2['COLUMN_NAME']) - set(meta1['COLUMN_NAME']):
        dtype = meta2[meta2['COLUMN_NAME'] == col]['DATA_TYPE'].values[0]
        rows.append({
            "column": col,
            "source_type": None,
            "target_type": dtype,
            "type_match": False,
            "in_both": False
        })

    return pd.DataFrame(rows)
