"""
Cohort Comparison Tool 🧬🚀 - Optimized Version
PoC – Compares Snowflake tables with identical OR differing schemas.

Author  : Mohamed Shez
Created : 20-05-2025   |  Updated : 18-06-2025
"""

from __future__ import annotations

import base64
import html
import json
import os
import tempfile
import time
import warnings
from snowflake.snowpark import Session
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional

import pandas as pd
import streamlit as st
import utils
from utils import quote_identifier


# ──────────────────────────────────────────────────────────────────────────────
# 🔧 Constants & settings
# ──────────────────────────────────────────────────────────────────────────────
STAGE_NAME = "@streamlit_downloads"
PAGE_SIZE = 100  # Pagination size for results

warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy connectable.*")
warnings.filterwarnings("ignore", message="You have an incompatible version of 'pyarrow'.*")

# ──────────────────────────────────────────────────────────────────────────────
# 🛠 Re-usable helpers
# ──────────────────────────────────────────────────────────────────────────────
def init_state() -> Optional[Session]:
    """Initialise Streamlit session-state and Snowpark session."""
    defaults = {
        "selected_database_source": "",
        "selected_schema_source": "",
        "selected_table_source": "",
        "selected_database_target": "",
        "selected_schema_target": "",
        "selected_table_target": "",
        "join_key": [],
        "comparison_ran": False,
        "current_page": 0,
        "total_pages": 0,
        "diff_type": "new"
    }

    for k, v in defaults.items():
        st.session_state.setdefault(k, v)

    if 'session' in st.session_state:
        return st.session_state.session

    try:
        session = Session.builder.getOrCreate()
    except Exception as e1:
        try:
            section = st.secrets["snowflake"]
            connection_parameters = {
                "account": section["account"],
                "authenticator": section["authenticator"],
                "user": section["user"],
                "database": section["database"],
                "schema": section["schema"],
                "role": section["role"],
                "warehouse": section["warehouse"]
            }
            session = Session.builder.configs(connection_parameters).create()
        except Exception as e2:
            st.error(f"Failed to connect to Snowflake. Initial error: {e1}. Secondary error: {e2}")
            return None

    try:
        stage = STAGE_NAME.lstrip("@")
        session.sql(f"CREATE STAGE IF NOT EXISTS {stage}").collect()
    except Exception:
        pass

    st.session_state.session = session
    return session

# ──────────────────────────────────────────────────────────────────────────────
# 🚀 Snowflake Query Optimisation
# ──────────────────────────────────────────────────────────────────────────────
def build_diff_query(
        db_src: str,
        sch_src: str,
        tbl_src: str,
        db_tgt: str,
        sch_tgt: str,
        tbl_tgt: str,
        join_keys: List[str],
        diff_type: str,
        page: int = 0
) -> str:
    fq_src = f"{quote_identifier(db_src)}.{quote_identifier(sch_src)}.{quote_identifier(tbl_src)}"
    fq_tgt = f"{quote_identifier(db_tgt)}.{quote_identifier(sch_tgt)}.{quote_identifier(tbl_tgt)}"

    common_cols = utils.get_common_columns_optimized(
        st.session_state.session,
        db_src, sch_src, tbl_src,
        db_tgt, sch_tgt, tbl_tgt
    )

    q_keys = [quote_identifier(k) for k in join_keys]
    join_cond = " AND ".join([f"src.{k} = tgt.{k}" for k in q_keys])

    # Deduplication logic - ensure distinct results
    if diff_type == "new":
        return f"""
        SELECT
          tgt.*
        FROM {fq_tgt} AS tgt
        LEFT JOIN {fq_src} AS src
          ON {join_cond}
        WHERE src.{q_keys[0]} IS NULL
        QUALIFY
          ROW_NUMBER()
            OVER (
              PARTITION BY {", ".join([f"tgt.{k}" for k in q_keys])}
              ORDER BY tgt.INGESTION_DATE DESC
            ) = 1
        ORDER BY {", ".join([f"tgt.{k}" for k in q_keys])}
        LIMIT {PAGE_SIZE} OFFSET {page * PAGE_SIZE}
        """

    elif diff_type == "dropped":
        return f"""
        SELECT
          src.*
        FROM {fq_src} AS src
        LEFT JOIN {fq_tgt} AS tgt
          ON {join_cond}
        WHERE tgt.{q_keys[0]} IS NULL
        QUALIFY
          ROW_NUMBER()
            OVER (
              PARTITION BY {", ".join([f"src.{k}" for k in q_keys])}
              ORDER BY src.INGESTION_DATE DESC
            ) = 1
        ORDER BY {", ".join([f"src.{k}" for k in q_keys])}
        LIMIT {PAGE_SIZE} OFFSET {page * PAGE_SIZE}
        """

    elif diff_type == "changed":
        non_key_cols = [quote_identifier(c) for c in common_cols if c not in join_keys]
        comparisons  = " OR ".join([f"src.{col} IS DISTINCT FROM tgt.{col}" for col in non_key_cols])

        return f"""
        SELECT
          src.*
        FROM {fq_src} AS src
        JOIN {fq_tgt} AS tgt
          ON {join_cond}
        WHERE {comparisons}
        QUALIFY
          ROW_NUMBER()
            OVER (
              PARTITION BY {", ".join([f"src.{k}" for k in q_keys])}
              ORDER BY src.INGESTION_DATE DESC
            ) = 1
        ORDER BY {", ".join([f"src.{k}" for k in q_keys])}
        LIMIT {PAGE_SIZE} OFFSET {page * PAGE_SIZE}
        """

    return ""


def get_diff_count(
        db_src: str,
        sch_src: str,
        tbl_src: str,
        db_tgt: str,
        sch_tgt: str,
        tbl_tgt: str,
        join_keys: List[str],
        diff_type: str
) -> int:
    """Get count of differences without loading full dataset"""
    fq_src = f"{quote_identifier(db_src)}.{quote_identifier(sch_src)}.{quote_identifier(tbl_src)}"
    fq_tgt = f"{quote_identifier(db_tgt)}.{quote_identifier(sch_tgt)}.{quote_identifier(tbl_tgt)}"

    # Get common columns
    common_cols = utils.get_common_columns_optimized(
        st.session_state.session,
        db_src, sch_src, tbl_src,
        db_tgt, sch_tgt, tbl_tgt
    )

    # Build join condition
    q_keys = [quote_identifier(k) for k in join_keys]
    join_cond = " AND ".join([f"src.{k} = tgt.{k}" for k in q_keys])

    # Deduplicated counts
    if diff_type == "new":
        query = f"""
            SELECT COUNT(DISTINCT {", ".join([f"tgt.{k}" for k in q_keys])})
            FROM {fq_tgt} tgt
            LEFT JOIN {fq_src} src ON {join_cond}
            WHERE {' AND '.join([f'src.{k} IS NULL' for k in q_keys])}
        """
    elif diff_type == "dropped":
        query = f"""
            SELECT COUNT(DISTINCT {", ".join([f"src.{k}" for k in q_keys])})
            FROM {fq_src} src
            LEFT JOIN {fq_tgt} tgt ON {join_cond}
            WHERE {' AND '.join([f'tgt.{k} IS NULL' for k in q_keys])}
        """
    elif diff_type == "changed":
        # Only compare non-key columns
        non_key_cols = [quote_identifier(c) for c in common_cols if c not in join_keys]
        comparisons = [f"src.{col} IS DISTINCT FROM tgt.{col}" for col in non_key_cols]

        query = f"""
            SELECT COUNT(DISTINCT {", ".join([f"src.{k}" for k in q_keys])})
            FROM {fq_src} src
            JOIN {fq_tgt} tgt ON {join_cond}
            WHERE {' OR '.join(comparisons)}
        """

    result = st.session_state.session.sql(query).collect()
    return result[0][0] if result else 0

# ──────────────────────────────────────────────────────────────────────────────
# 🔄 Reset callback functions - Clear session state for specific tables or all
# ──────────────────────────────────────────────────────────────────────────────
def reset_source_table():
    st.session_state.selected_database_source = ""
    st.session_state.selected_schema_source = ""
    st.session_state.selected_table_source = ""
    st.session_state.join_key = []

def reset_target_table():
    st.session_state.selected_database_target = ""
    st.session_state.selected_schema_target = ""
    st.session_state.selected_table_target = ""
    st.session_state.join_key = []

def reset_all():
    reset_keys = [
        "selected_database_source", "selected_schema_source", "selected_table_source",
        "selected_database_target", "selected_schema_target", "selected_table_target",
        "join_key", "comparison_ran", "current_page", "total_pages", "diff_type"
    ]
    for key in reset_keys:
        if key in st.session_state:
            del st.session_state[key]


# ──────────────────────────────────────────────────────────────────────────────
# 🖥 Sidebar – database / table / key selectors
# ──────────────────────────────────────────────────────────────────────────────
def render_sidebar() -> Tuple[bool, Tuple[str, str, str, str, str, str]]:
    """Render sidebar controls and return the 'Compare' button state + IDs."""
    sess = st.session_state.session
    if not sess:
        st.sidebar.error("⛔️ No Snowpark session; initialise connection.")
        return False, ("",) * 6

    with st.sidebar:
        # Logo
        try:
            with open("dxrx_logo.png", "rb") as f:
                st.image(f.read(), width=100)
        except Exception:
            st.warning("⚠️ Logo not found or cannot be loaded.")

        # Source table selectors
        st.header("⬇️ Source Table")
        dbs = [""] + [r[1] for r in sess.sql("SHOW DATABASES").collect()]
        db_src = st.selectbox("Database (source)", dbs, key="selected_database_source")

        schs = [""] + [r[1] for r in sess.sql(f"SHOW SCHEMAS IN DATABASE {db_src}").collect()] if db_src else []
        sch_src = st.selectbox("Schema (source)", schs, key="selected_schema_source", disabled=not db_src)

        tbls = [""] + [
            r[0] for r in sess.sql(
                f"SELECT TABLE_NAME FROM {db_src}.INFORMATION_SCHEMA.TABLES "
                f"WHERE TABLE_SCHEMA='{sch_src}'"
            ).collect()
        ] if sch_src else []
        tsrc = st.selectbox("Table (source)", tbls, key="selected_table_source", disabled=not sch_src)

        # Reset Source Table button
        st.button("🔄 Reset Source Table",
                  on_click=reset_source_table,
                  key="reset_source_button")

        st.markdown("<br>", unsafe_allow_html=True)

        # Target table selectors
        st.header("🎯 Target Table")
        db_tgt = st.selectbox("Database (target)", dbs, key="selected_database_target")

        schs_t = [""] + [r[1] for r in sess.sql(f"SHOW SCHEMAS IN DATABASE {db_tgt}").collect()] if db_tgt else []
        sch_tgt = st.selectbox("Schema (target)", schs_t, key="selected_schema_target", disabled=not db_tgt)

        tbls_t = [""] + [
            r[0] for r in sess.sql(
                f"SELECT TABLE_NAME FROM {db_tgt}.INFORMATION_SCHEMA.TABLES "
                f"WHERE TABLE_SCHEMA='{sch_tgt}'"
            ).collect()
        ] if sch_tgt else []
        tgt = st.selectbox("Table (target)", tbls_t, key="selected_table_target", disabled=not sch_tgt)

        # Reset Target Table button
        st.button("🔄 Reset Target Table",
                  on_click=reset_target_table,
                  key="reset_target_button")

        st.markdown("<br>", unsafe_allow_html=True)

        # Join key
        st.header("🔗 Join Key(s)")
        st.caption("Pick one or more columns to form the composite key.")

        cols = utils.get_table_columns(sess, db_src, sch_src, tsrc) if tsrc else []

        default_keys = st.session_state.get("join_key", [])
        st.multiselect(
            label="Join key(s)",
            options=cols,
            default=default_keys,
            key="join_key",
            help="One or more columns that uniquely identify each row",
        )

        # Reset All button
        st.button("🔄 Reset All",
                  on_click=reset_all,
                  key="reset_all_button")

        valid = all(
            [db_src, sch_src, tsrc, db_tgt, sch_tgt, tgt, len(st.session_state.join_key) > 0]
        )
        st.markdown("<br>", unsafe_allow_html=True)
        valid = all([db_src, sch_src, tsrc, db_tgt, sch_tgt, tgt, len(st.session_state.join_key) > 0])
        run_btn = st.button("🔍 Compare Tables", disabled=not valid)

    return run_btn, (db_src, sch_src, tsrc, db_tgt, sch_tgt, tgt)

# ──────────────────────────────────────────────────────────────────────────────
# 🖼 Results renderer
# ──────────────────────────────────────────────────────────────────────────────
def render_results(db_src, sch_src, tbl_src, db_tgt, sch_tgt, tbl_tgt):
    """Render comparison results with pagination and graph/table views"""
    sess = st.session_state.session
    join_keys = st.session_state.join_key
    diff_type = st.session_state.get("diff_type", "new")
    current_page = st.session_state.get("current_page", 0)

    # Get counts
    new_count = get_diff_count(db_src, sch_src, tbl_src, db_tgt, sch_tgt, tbl_tgt, join_keys, "new")
    dropped_count = get_diff_count(db_src, sch_src, tbl_src, db_tgt, sch_tgt, tbl_tgt, join_keys, "dropped")
    changed_count = get_diff_count(db_src, sch_src, tbl_src, db_tgt, sch_tgt, tbl_tgt, join_keys, "changed")

    # Calculate total pages for current diff type
    total_count = {
        "new": new_count,
        "dropped": dropped_count,
        "changed": changed_count
    }[diff_type]

    total_pages = max(1, (total_count + PAGE_SIZE - 1) // PAGE_SIZE)
    st.session_state.total_pages = total_pages

    # Build and execute query
    query = build_diff_query(
        db_src, sch_src, tbl_src,
        db_tgt, sch_tgt, tbl_tgt,
        join_keys, diff_type, current_page
    )

    with st.spinner(f"Fetching {diff_type} records..."):
        df = sess.sql(query).to_pandas()

    # Display results
    st.success("✅ Comparison complete")

    # Create tabs for table and graph views
    tab1, tab2 = st.tabs(["📋 Table View", "📈 Graph View"])

    with tab1:
        # Column summary
        st.subheader("📊 Column-level summary")
        col_summary = utils.summarise_column_diffs_optimized(
            sess,
            db_src, sch_src, tbl_src,
            db_tgt, sch_tgt, tbl_tgt
        )
        st.dataframe(col_summary)

        # Metrics
        n, d, c = st.columns(3)
        n.metric("New rows", new_count)
        d.metric("Dropped rows", dropped_count)
        c.metric("Changed rows", changed_count)

        # Diff type selector
        diff_options = {
            "new": f"New Records ({new_count})",
            "dropped": f"Dropped Records ({dropped_count})",
            "changed": f"Changed Records ({changed_count})"
        }
        selected_diff = st.selectbox("Select difference type", options=list(diff_options.keys()),
                                     format_func=lambda x: diff_options[x],
                                     key="diff_type_selector")

        if selected_diff != diff_type:
            st.session_state.diff_type = selected_diff
            st.session_state.current_page = 0
            st.rerun()

        # Display current diff type
        st.subheader(diff_options[selected_diff])

        if not df.empty:
            # Calculate row range for display
            start_row = current_page * PAGE_SIZE + 1
            end_row = min((current_page + 1) * PAGE_SIZE, total_count)
            if end_row > total_count:
                end_row = total_count

            st.dataframe(df)

            # Pagination controls with manual input
            col1, col2, col3, col4, col5 = st.columns([1, 2, 1, 3, 2])

            with col1:
                if st.button("⬅️ Previous", disabled=(current_page == 0)):
                    st.session_state.current_page -= 1
                    st.rerun()

            with col2:
                st.markdown(
                    f"**Page {current_page + 1} of {total_pages}**  \n"
                    f"Rows **{start_row} - {end_row}** of **{total_count}**"
                )

            with col3:
                if st.button("Next ➡️", disabled=(current_page >= total_pages - 1)):
                    st.session_state.current_page += 1
                    st.rerun()

            with col4:
                new_page = st.number_input(
                    "Go to page:",
                    min_value=1,
                    max_value=total_pages,
                    value=current_page + 1,
                    step=1,
                    format="%d"
                )

            with col5:
                if st.button("Jump"):
                    if 1 <= new_page <= total_pages:
                        st.session_state.current_page = new_page - 1
                        st.rerun()

            # Export buttons with descriptive filenames
            st.divider()
            st.subheader("Export Results")
            export_col1, export_col2 = st.columns(2)

            # Map diff types to filename components
            filename_map = {
                "new": "new-records",
                "dropped": "dropped-records",
                "changed": "changed-records"
            }
            filename_base = filename_map.get(diff_type, "comparison-results")

            with export_col1:
                csv = df.to_csv(index=False).encode("utf-8")
                st.download_button(
                    "💾 Download CSV",
                    data=csv,
                    file_name=f"{filename_base}.csv",
                    mime="text/csv"
                )

            with export_col2:
                json_data = df.to_json(orient="records", indent=2)
                st.download_button(
                    "📥 Download JSON",
                    data=json_data,
                    file_name=f"{filename_base}.json",
                    mime="application/json"
                )
        else:
            st.info(f"No {selected_diff.replace('_', ' ')} found")

    with tab2:
        # Graph view
        st.subheader("📊 Summary Visualisation")

        # Create summary data for the bar chart
        summary_data = pd.DataFrame({
            "Change Type": ["New", "Dropped", "Changed"],
            "Count": [new_count, dropped_count, changed_count]
        })

        # Bar chart visualisation
        st.bar_chart(summary_data.set_index("Change Type"))

        # Additional metrics display
        st.subheader("📈 Detailed Metrics")
        col1, col2, col3 = st.columns(3)
        col1.metric("New Rows", new_count, delta_color="off")
        col2.metric("Dropped Rows", dropped_count, delta_color="off")
        col3.metric("Changed Rows", changed_count, delta_color="off")

        # Pie chart for proportion visualisation
        if (new_count + dropped_count + changed_count) > 0:
            st.subheader("🧩 Change Proportions")
            pie_data = summary_data.copy()
            pie_data["Percentage"] = pie_data["Count"] / (new_count + dropped_count + changed_count) * 100
            pie_data = pie_data[pie_data["Count"] > 0]  # Filter out zero counts

            if not pie_data.empty:
                st.write("Distribution of changes:")
                st.dataframe(pie_data[["Change Type", "Count", "Percentage"]].round(1))
            else:
                st.info("No changes found between the tables")

# ──────────────────────────────────────────────────────────────────────────────
# 🔗 Main
# ──────────────────────────────────────────────────────────────────────────────
def main() -> None:
    st.set_page_config(page_title="Cohort Comparison Tool – PoC", page_icon="🧪", layout="wide")
    st.title("Cohort Comparison Tool 🧬🚀 – PoC")

    session = init_state()
    if not session:
        st.error("❌ Failed to initialize Snowflake session")
        return

    clicked, ids = render_sidebar()
    db_src, sch_src, tbl_src, db_tgt, sch_tgt, tbl_tgt = ids

    if clicked:
        st.session_state.comparison_ran = True
        st.session_state.current_page = 0
        st.session_state.diff_type = "new"
        st.cache_data.clear()

    if st.session_state.comparison_ran:
        with st.spinner("Running comparison..."):
            render_results(db_src, sch_src, tbl_src, db_tgt, sch_tgt, tbl_tgt)

if __name__ == "__main__":
    main()
