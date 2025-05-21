"""
Cohort Comparison Tool PoC 🧬🚀
Streamlit application for automated comparison of cohort‑derived datasets in Snowflake

Author : Mohamed Shez
Created: 2025‑05‑20
Last update: 2025‑05‑20

═══════════════════════════════════════════════════════════════════════════════
📌  At‑a‑Glance
───────────────────────────────────────────────────────────────────────────────
This tool replaces error‑prone manual checks with a scalable, structured
workflow that detects differences between two cohort‑derived datasets—whether
they have identical or variant schemas. Analysts can validate cohort changes,
 test logic modifications and build client‑ready diff summaries in seconds.

═══════════════════════════════════════════════════════════════════════════════
📝  Acceptance Criteria (work‑in‑progress)
───────────────────────────────────────────────────────────────────────────────
1️⃣  **Identical Schemas**
    • Accept two tables with matching column structures.
    • Report **NEW**, **DROPPED** and **CHANGED** rows.
    • For changed rows emit a JSON **change_summary** describing column‑level diffs.

2️⃣  **Different Schemas**
    • Accept tables with differing structures.
    • Produce patient‑level and NPI‑level difference summaries plus overall
      match counts.

3️⃣  **File‑level Summary**
    • If **PATIENT_ID** present → summarise new / dropped / matched patients.
    • If **NPI** present       → summarise new / dropped / matched NPIs.

4️⃣  **Usability & Outputs**
    • Flag schema mismatches clearly.
    • Show analyst‑friendly summaries and optional machine‑readable JSON diff logs.
    • Format output for downstream validation dashboards / approval workflows.

5️⃣  **Non‑functional (draft)**
    • Compare up to **X** rows within **X** minutes (TBC).
    • Log and surface malformed input or missing‑field errors.

═══════════════════════════════════════════════════════════════════════════════
"""

import streamlit as st
import snowflake.connector
import pandas as pd
from typing import Tuple, List, Dict
import json

################################################################################
# 🎨  Session‑state helpers & theming
################################################################################

def initialize_session_state() -> None:
    """Populate `st.session_state` with all keys we rely on, only once."""

    # Brand colours that pages can reuse
    st.session_state.card_bg_color   = st.session_state.get("card_bg_color",   "#eceff1")
    st.session_state.header_bg_color = st.session_state.get("header_bg_color", "#37474f")

    # Core selections / cache containers – trimmed for comparison use‑case
    default_keys = dict(
        selected_database_source   = "",
        selected_schema_source     = "",
        selected_table_source      = "",
        selected_database_target   = "",
        selected_schema_target     = "",
        selected_table_target      = "",
        join_key                   = "",
        new_records_df             = pd.DataFrame(),
        dropped_records_df         = pd.DataFrame(),
        changed_records_df         = pd.DataFrame(),
        comparison_ran             = False,
    )

    for k, v in default_keys.items():
        st.session_state.setdefault(k, v)

################################################################################
# 🔌  Snowflake connection helpers
################################################################################

def get_connection():
    """Establish a connection to Snowflake using Streamlit secrets."""
    return snowflake.connector.connect(
        user      = st.secrets["snowflake"]["user"],
        password  = st.secrets["snowflake"]["password"],
        account   = st.secrets["snowflake"]["account"],
        warehouse = st.secrets["snowflake"]["warehouse"],
        role      = st.secrets["snowflake"]["role"],
    )

################################################################################
# 🧮  Core comparison utilities
################################################################################

@st.cache_data(show_spinner=False)
def fetch_table(conn, database: str, schema: str, table: str) -> pd.DataFrame:
    """Pull a full table into a DataFrame (PoC scale only)."""
    query = f"SELECT * FROM {database}.{schema}.{table}"
    return pd.read_sql(query, conn)

@st.cache_data(show_spinner=False)
def compare_schemas(df1: pd.DataFrame, df2: pd.DataFrame) -> bool:
    """Return *True* if column orders & names match exactly."""
    return list(df1.columns) == list(df2.columns)

@st.cache_data(show_spinner=False)
def compute_diffs(
    df_base: pd.DataFrame,
    df_updated: pd.DataFrame,
    key: str,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Return new, dropped, and changed record DataFrames."""

    base    = df_base.set_index(key)
    updated = df_updated.set_index(key)

    new_records      = updated.loc[updated.index.difference(base.index)].reset_index()
    dropped_records  = base.loc[base.index.difference(updated.index)].reset_index()

    # Changed rows: iterate common keys once
    changed: List[Dict[str, Dict[str, Dict[str, str]]]] = []
    for k in base.index.intersection(updated.index):
        row_base    = base.loc[k]
        row_updated = updated.loc[k]
        diffs = {
            col: {"from": row_base[col], "to": row_updated[col]}
            for col in df_base.columns
            if row_base[col] != row_updated[col]
        }
        if diffs:
            changed.append({"key": k, "changes": diffs})

    changed_records = pd.DataFrame(changed)
    return new_records, dropped_records, changed_records

################################################################################
# 🏠  Main Streamlit application
################################################################################

def main():
    # Page config & session bootstrap
    st.set_page_config(
        page_title="Cohort Comparison",
        page_icon="🧪",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    initialize_session_state()

    st.title("Cohort Comparison Tool – PoC")

    # Sidebar – table selectors
    with st.sidebar:
        st.header("Source Table ⬇️")
        st.session_state.selected_database_source = st.text_input("Database (source)",  value=st.session_state.selected_database_source)
        st.session_state.selected_schema_source   = st.text_input("Schema (source)",    value=st.session_state.selected_schema_source)
        st.session_state.selected_table_source    = st.text_input("Table (source)",     value=st.session_state.selected_table_source)

        st.header("Target Table ⬆️")
        st.session_state.selected_database_target = st.text_input("Database (target)",  value=st.session_state.selected_database_target)
        st.session_state.selected_schema_target   = st.text_input("Schema (target)",    value=st.session_state.selected_schema_target)
        st.session_state.selected_table_target    = st.text_input("Table (target)",     value=st.session_state.selected_table_target)

        st.session_state.join_key = st.text_input("Join Key Column", value=st.session_state.join_key)

        compare_btn = st.button("🔍 Compare Tables")

    # Main panel – run comparison when clicked
    if compare_btn:
        if not st.session_state.join_key:
            st.error("❌ Please specify a join key column.")
            st.stop()

        with st.spinner("Connecting to Snowflake and fetching tables …"):
            conn = get_connection()
            df_source = fetch_table(conn,
                                    st.session_state.selected_database_source,
                                    st.session_state.selected_schema_source,
                                    st.session_state.selected_table_source)
            df_target = fetch_table(conn,
                                    st.session_state.selected_database_target,
                                    st.session_state.selected_schema_target,
                                    st.session_state.selected_table_target)

        if not compare_schemas(df_source, df_target):
            st.error("⚠️ Schemas do not match. Please select tables with identical structures.")
            st.stop()

        new_df, dropped_df, changed_df = compute_diffs(df_source, df_target, st.session_state.join_key)

        st.success("✅ Comparison complete!")

        st.session_state.new_records_df     = new_df
        st.session_state.dropped_records_df = dropped_df
        st.session_state.changed_records_df = changed_df
        st.session_state.comparison_ran     = True

    # Display results if available
    if st.session_state.comparison_ran:
        st.subheader("📈 Summary")
        col1, col2, col3 = st.columns(3)
        col1.metric("New Records",      len(st.session_state.new_records_df))
        col2.metric("Dropped Records",  len(st.session_state.dropped_records_df))
        col3.metric("Changed Records",  len(st.session_state.changed_records_df))

        st.markdown("---")
        st.subheader("🆕 New Records")
        st.dataframe(st.session_state.new_records_df)

        st.subheader("🗑️ Dropped Records")
        st.dataframe(st.session_state.dropped_records_df)

        st.subheader("🔄 Changed Records – JSON Diffs")
        st.json(st.session_state.changed_records_df.to_dict(orient="records"))

################################################################################
# 🚀  Script entry‑point
################################################################################

if __name__ == "__main__":
    main()
