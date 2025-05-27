"""
Cohort Comparison Tool 🧬🚀
Streamlit application for comparing Snowflake cohort datasets via Snowpark; supports flexible key-based diffs and generic column summaries.

Author : Mohamed Shez
Created : 2025‑05‑20  |  Updated : 2025‑05‑22
"""

from __future__ import annotations

import json
import warnings
from datetime import datetime, timezone
from typing import Dict, List, Tuple

import pandas as pd
import streamlit as st

# Suppress warnings
warnings.filterwarnings(
    "ignore", message="pandas only supports SQLAlchemy connectable.*"
)
warnings.filterwarnings(
    "ignore", message="You have an incompatible version of 'pyarrow'.*"
)

MAX_ROWS = 10_000  # Limit to 10k rows

################################################################################
# 🎛 Session & Snowpark connection
################################################################################

def init_state() -> None:
    """Initialise session state and Snowpark session."""
    defaults: Dict[str, object] = dict(
        selected_database_source="",
        selected_schema_source="",
        selected_table_source="",
        selected_database_target="",
        selected_schema_target="",
        selected_table_target="",
        join_key="",
        new_records_df=pd.DataFrame(),
        dropped_records_df=pd.DataFrame(),
        changed_records_df=pd.DataFrame(),
        column_diff_summary=pd.DataFrame(),
        comparison_ran=False,
    )
    for k, v in defaults.items():
        st.session_state.setdefault(k, v)

    def reset_all() -> None:
        """Clear user selections and results, then reinitialise comparison_ran."""
        for key in defaults.keys():
            st.session_state.pop(key, None)
        # Ensure comparison_ran exists to avoid missing attribute
        st.session_state["comparison_ran"] = False
    st.session_state["reset_all"] = reset_all

    if "session" not in st.session_state:
        from snowflake.snowpark import Session  # type: ignore
        sess = Session.builder.getOrCreate()
        st.session_state.session = sess

################################################################################
# 🗄 Data access & diff utilities
################################################################################

# @st.cache_data(show_spinner=False)
def fetch_table(_sess, db: str, sch: str, tbl: str) -> pd.DataFrame:
    """Load table via Snowpark into a pandas DataFrame."""
    return _sess.sql(f"SELECT * FROM {db}.{sch}.{tbl}").to_pandas()

# @st.cache_data(show_spinner=False)
def compare_schemas_strict(df1: pd.DataFrame, df2: pd.DataFrame) -> bool:
    """Return True if columns and dtypes match exactly."""
    return list(df1.columns) == list(df2.columns) and all(
        df1.dtypes.values == df2.dtypes.values
    )

# @st.cache_data(show_spinner=False)
def compute_diffs(
    df_base: pd.DataFrame, df_updated: pd.DataFrame, key: str
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Row-level diff: new, dropped, and changed DataFrames."""
    if key not in df_base.columns or key not in df_updated.columns:
        raise ValueError(f"❌ Join key '{key}' missing from one of the tables.")
    if df_base[key].duplicated().any() or df_updated[key].duplicated().any():
        raise ValueError(f"❌ Join key '{key}' must be unique in both tables.")
    base = df_base.set_index(key)
    upd = df_updated.set_index(key)
    new_df = upd.loc[upd.index.difference(base.index)].reset_index()
    drop_df = base.loc[base.index.difference(upd.index)].reset_index()
    ts = datetime.now(timezone.utc).isoformat()
    changed: List[Dict[str, object]] = []
    for k in base.index.intersection(upd.index):
        diffs = {
            c: {"from": base.at[k, c], "to": upd.at[k, c]}
            for c in base.columns
            if base.at[k, c] != upd.at[k, c]
        }
        if diffs:
            changed.append({"key": k, "timestamp": ts, "changes": diffs})
    return new_df, drop_df, pd.DataFrame(changed)

# @st.cache_data(show_spinner=False)
def summarise_column_diffs(
    df_base: pd.DataFrame, df_updated: pd.DataFrame
) -> pd.DataFrame:
    """Generic column summary for any identifier columns."""
    cols = set(df_base.columns) | set(df_updated.columns)
    rows: List[Dict[str, object]] = []
    for col in sorted(cols):
        base_vals = set(df_base[col].dropna().unique()) if col in df_base.columns else set()
        upd_vals = set(df_updated[col].dropna().unique()) if col in df_updated.columns else set()
        rows.append({
            "column": col,
            "base_unique": len(base_vals),
            "target_unique": len(upd_vals),
            "new_values": len(upd_vals - base_vals),
            "dropped_values": len(base_vals - upd_vals),
        })
    return pd.DataFrame(rows)

################################################################################
# 🖥 Sidebar inputs
################################################################################

def render_sidebar() -> Tuple[bool, Tuple[str, str, str, str, str, str]]:
    """Render sidebar selects using Snowpark session only."""
    sess = st.session_state.session
    # If no Snowpark session, show error and exit
    if sess is None:
        st.sidebar.error("⛔️🔌 No Snowpark session; initialise connection.")
        return False, ("",)*6

    with st.sidebar:
        # Display logo above sidebar via binary to avoid file path issues
        try:
            with open("dxrx_logo.png", "rb") as img_file:
                img_bytes = img_file.read()
            st.image(img_bytes, width=100)
        except Exception:
            st.warning("⚠️ Logo not found or cannot be loaded.")

        # Source Table
        st.header("⬇️ Source Table")
        dbs = [""] + [row[1] for row in sess.sql("SHOW DATABASES").collect()]
        db_src = st.selectbox("Database (source)", dbs, key="selected_database_source")
        schs = [""]
        if db_src:
            schs += [row[1] for row in sess.sql(f"SHOW SCHEMAS IN DATABASE {db_src}").collect()]
        sch_src = st.selectbox(
            "Schema (source)", schs,
            key="selected_schema_source", disabled=not db_src
        )
        tbls = [""]
        if sch_src:
            tbls += [row[0] for row in sess.sql(
                f"SELECT TABLE_NAME FROM {db_src}.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='{sch_src}'"
            ).collect()]
        tsrc = st.selectbox(
            "Table (source)", tbls,
            key="selected_table_source", disabled=not sch_src
        )
        if st.button("🔄 Reset Source Table"):
            for k in ("selected_database_source", "selected_schema_source", "selected_table_source"):
                st.session_state.pop(k, None)
        st.markdown("<br>", unsafe_allow_html=True)

        # Target Table
        st.header("🎯 Target Table")
        db_tgt = st.selectbox("Database (target)", dbs, key="selected_database_target")
        schs_t = [""]
        if db_tgt:
            schs_t += [row[1] for row in sess.sql(f"SHOW SCHEMAS IN DATABASE {db_tgt}").collect()]
        sch_tgt = st.selectbox(
            "Schema (target)", schs_t,
            key="selected_schema_target", disabled=not db_tgt
        )
        tbls_t = [""]
        if sch_tgt:
            tbls_t += [row[0] for row in sess.sql(
                f"SELECT TABLE_NAME FROM {db_tgt}.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='{sch_tgt}'"
            ).collect()]
        tgt = st.selectbox(
            "Table (target)", tbls_t,
            key="selected_table_target", disabled=not sch_tgt
        )
        if st.button("🔄 Reset Target Table"):
            for k in ("selected_database_target", "selected_schema_target", "selected_table_target"):
                st.session_state.pop(k, None)
        st.markdown("<br>", unsafe_allow_html=True)

        # Join Key Column
        st.header("🔗 Join Key Column")
        st.caption("Select the unique key column used to match records between source and target. This must be non-null and unique.")
        cols = []
        if db_src and sch_src and tsrc:
            cols = [row[0] for row in sess.sql(
                f"SELECT COLUMN_NAME FROM {db_src}.INFORMATION_SCHEMA.COLUMNS "
                f"WHERE TABLE_SCHEMA='{sch_src}' AND TABLE_NAME='{tsrc}' "
                f"ORDER BY ORDINAL_POSITION"
            ).collect()]
        opts = [""] + cols
        jk = st.selectbox(
            "Join key", opts,
            index=opts.index(st.session_state.get("join_key", "")) if st.session_state.get("join_key", "") in opts else 0,
            key="join_key",
            disabled=len(opts) <= 1
        )

        # Reset All Selections
        if st.button("🔄 Reset All"):
            st.session_state.reset_all()

        st.markdown("<br>", unsafe_allow_html=True)

        # Compare action button
        valid = all([db_src, sch_src, tsrc, db_tgt, sch_tgt, tgt, jk])
        run_btn = st.button("🔍 **Compare Tables**", disabled=not valid)

    return run_btn, (db_src, sch_src, tsrc, db_tgt, sch_tgt, tgt)

################################################################################
# 🖼 Results rendering
################################################################################

def render_results() -> None:
    st.success("✅ Comparison complete")
    st.subheader("📊 Column-level summary")
    st.dataframe(st.session_state.column_diff_summary)
    st.download_button(
        "📥 Download column summary",
        st.session_state.column_diff_summary.to_csv(index=False),
        "column_summary.csv",
        key="download_col_summary"
    )
    n, d, c = st.columns(3)
    n.metric("New rows", len(st.session_state.new_records_df))
    d.metric("Dropped rows", len(st.session_state.dropped_records_df))
    c.metric("Changed rows", len(st.session_state.changed_records_df))
    st.download_button(
        "📥 Download new rows CSV",
        st.session_state.new_records_df.to_csv(index=False),
        "new_rows.csv",
        key="download_new"
    )
    st.download_button(
        "📥 Download dropped rows CSV",
        st.session_state.dropped_records_df.to_csv(index=False),
        "dropped_rows.csv",
        key="download_drop"
    )
    st.download_button(
        "📥 Download changed JSON",
        json.dumps(
            st.session_state.changed_records_df.to_dict(orient="records"),
            indent=2, default=str
        ),
        "changed_rows.json",
        key="download_change"
    )
    with st.expander("🆕 New records"):
        st.dataframe(st.session_state.new_records_df.head(MAX_ROWS))
    with st.expander("🗑️ Dropped records"):
        st.dataframe(st.session_state.dropped_records_df.head(MAX_ROWS))
    with st.expander("🔄 Changed records – diffs"):
        st.json(st.session_state.changed_records_df.to_dict(orient="records"))

################################################################################
# 🔗 Main
################################################################################

def main() -> None:
    st.set_page_config(page_title="🛠️ Cohort Comparison Tool", page_icon="🧪", layout="wide")
    st.title("Cohort Comparison Tool – PoC")

    init_state()

    # Sidebar selection
    clicked, ids = render_sidebar()

    # Show instructions until a run is triggered
    if not st.session_state.comparison_ran and not clicked:
        st.markdown(
            """
            **At a Glance:**  
            • Select your **Source Table** by choosing `Database` → `Schema` → `Table`.  
            • Select your **Target Table** similarly.  
            • Pick a **Join Key** column that uniquely identifies rows.  
            • Click **Compare Tables** to compute and display diffs:
              - `New records`  
              - `Dropped records`  
              - `Changed records` with a **JSON change summary**
            """,
            unsafe_allow_html=True,
        )
        return

    # Perform comparison when user clicks
    if clicked:
        ds, ss, tsrc, db_tgt, sch_tgt, tgt = ids
        sess = st.session_state.session
        # Fetch & compute diffs with spinner
        with st.spinner("🔄 Fetching & computing diffs…"):
            import time
            t0 = time.time()
            df_base = fetch_table(sess, ds, ss, tsrc)
            df_target = fetch_table(sess, db_tgt, sch_tgt, tgt)
            t1 = time.time()
            st.info(f"✅ Fetched tables in {t1 - t0:.2f}s (rows: source={len(df_base)}, target={len(df_target)})")

            # Load tables
            try:
                df_base = fetch_table(sess, ds, ss, tsrc)
                df_target = fetch_table(sess, db_tgt, sch_tgt, tgt)
            except Exception as e:
                st.error(f"❌ Error loading tables: {e}")
                return

            # Schema validation: must match exact schemas to proceed
            if not compare_schemas_strict(df_base, df_target):
                st.error("❌ Table schemas do not match exactly. Please select tables with identical schemas before comparing.")
                return

            # Guard large datasets
            if len(df_base) > MAX_ROWS or len(df_target) > MAX_ROWS:
                st.warning(f"⚠️ Large (> {MAX_ROWS:,}) tables may take longer to process.")
                st.info("❗️ Limited to 10k rows for this PoC.")

            # Compute diffs
            try:
                new_df, drop_df, change_df = compute_diffs(
                    df_base, df_target, st.session_state.join_key
                )
                t2 = time.time()
                st.info(f"✅ Computed diffs in {t2 - t1:.2f}s")
            except ValueError as e:
                st.error(str(e))
                return

            # Save to state
            st.session_state.new_records_df = new_df
            st.session_state.dropped_records_df = drop_df
            st.session_state.changed_records_df = change_df
            st.session_state.column_diff_summary = summarise_column_diffs(df_base, df_target)
            st.session_state.comparison_ran = True

    # Render results if available
    if st.session_state.comparison_ran:
        render_results()

if __name__ == "__main__":
    main()
