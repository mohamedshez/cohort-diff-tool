"""
Cohort Comparison Tool 🧬🚀
Streamlit application for comparing Snowflake cohort datasets via Snowpark;
supports composite key-based diffs and generic column summaries.

Author : Mohamed Shez
Created : 20-05-2025  |  Updated : 16-06-2025
"""

from __future__ import annotations

import json
import warnings
import time
from datetime import datetime, timezone
from typing import Dict, List, Tuple

import base64, html, os, tempfile
import pandas as pd
import streamlit as st

# Suppress irrelevant warnings
warnings.filterwarnings(
    "ignore", message="pandas only supports SQLAlchemy connectable.*"
)
warnings.filterwarnings(
    "ignore", message="You have an incompatible version of 'pyarrow'.*"
)

MAX_ROWS = 10_000  # Limit to 10k rows
MAX_EMBED_SIZE = 2_000_000 # Limit the max embed size
STAGE_NAME     = "@streamlit_downloads"

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
        join_key=[],  # now a list for multiselect
        new_records_df=pd.DataFrame(),
        dropped_records_df=pd.DataFrame(),
        changed_records_df=pd.DataFrame(),
        column_diff_summary=pd.DataFrame(),
        comparison_ran=False,
    )
    for k, v in defaults.items():
        st.session_state.setdefault(k, v)

    def reset_all() -> None:
        for key in defaults:
            st.session_state.pop(key, None)
        # reinitialise comparison_ran
        st.session_state["comparison_ran"] = False

    st.session_state["reset_all"] = reset_all

    if "session" not in st.session_state:
        from snowflake.snowpark import Session  # type: ignore
        st.session_state.session = Session.builder.getOrCreate()

################################################################################
# 🗄 Data access & diff utilities
################################################################################

# @st.cache_data(show_spinner=False)
# def fetch_table(_sess, db: str, sch: str, tbl: str) -> pd.DataFrame:
#     """Load table via Snowpark into a pandas DataFrame."""
#     return _sess.sql(f"SELECT * FROM {db}.{sch}.{tbl}").to_pandas()

def _quote(ident: str) -> str:
    if not ident:
        raise ValueError("Database, schema and table names must not be empty.")
    return '"' + ident.replace('"', '""') + '"'

@st.cache_data(show_spinner=False)          # ⇦ keep the decorator
def fetch_table(_sess, db: str, sch: str, tbl: str) -> pd.DataFrame:
    """Load an entire table into a pandas DataFrame, safely quoted."""
    fq_name = f"{_quote(db)}.{_quote(sch)}.{_quote(tbl)}"
    return _sess.table(fq_name).to_pandas()

@st.cache_data(show_spinner=False)
def compare_schemas_strict(df1: pd.DataFrame, df2: pd.DataFrame) -> bool:
    """Return True if columns and dtypes match exactly."""
    return list(df1.columns) == list(df2.columns) and all(
        df1.dtypes.values == df2.dtypes.values
    )

@st.cache_data(show_spinner=False)
def _json_safe(value):
    """Return a JSON-serialisable scalar (null, str, int, float, …)."""
    # Missing values → None  (→ JSON null)
    if value is None or pd.isna(value):
        return None
    # Pandas / Python datetimes → ISO-8601 strings
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    # Anything else is already fine (int, float, str, bool, UUID, …)
    return value

@st.cache_data(show_spinner=False)
def compute_diffs(
    df_base: pd.DataFrame,
    df_updated: pd.DataFrame,
    keys: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Row-level diff with composite key: new, dropped, changed (JSON-safe)."""

    # 1) per-key existence & uniqueness
    for k in keys:
        if k not in df_base.columns or k not in df_updated.columns:
            raise ValueError(f"❌ Join key '{k}' missing from one of the tables.")
        if df_base[k].duplicated().any() or df_updated[k].duplicated().any():
            raise ValueError(f"❌ Join key '{k}' must be unique in both tables.")

    # 2) composite uniqueness
    if df_base.duplicated(subset=keys).any() or df_updated.duplicated(subset=keys).any():
        raise ValueError(f"❌ Composite join key {keys} must be unique in both tables.")

    # 3) build indices
    base = df_base.set_index(keys)
    upd  = df_updated.set_index(keys)

    # 4) new & dropped rows
    new_df  = upd.loc[upd.index.difference(base.index)].reset_index()
    drop_df = base.loc[base.index.difference(upd.index)].reset_index()

    # 5) changed rows
    cols_to_compare = [c for c in df_base.columns if c not in keys]
    ts = datetime.now(timezone.utc).isoformat()

    changed: list[dict[str, object]] = []
    for idx in base.index.intersection(upd.index):
        diffs = {
            col: {
                "from": _json_safe(base.at[idx, col]),
                "to":   _json_safe(upd.at[idx, col]),
            }
            for col in cols_to_compare
            if not (
                pd.isna(base.at[idx, col]) and pd.isna(upd.at[idx, col])
            ) and base.at[idx, col] != upd.at[idx, col]
        }
        if diffs:
            changed.append(
                {"key": idx, "timestamp": ts, "changes": diffs}
            )

    # note: changed is still turned into a DataFrame so existing UI keeps working
    return new_df, drop_df, pd.DataFrame(changed)

def summarise_column_diffs(
    df_base: pd.DataFrame,
    df_updated: pd.DataFrame,
) -> pd.DataFrame:
    """Generic column summary showing unique/new/dropped values."""
    cols = sorted(set(df_base.columns) | set(df_updated.columns))
    rows: List[Dict[str, object]] = []
    for col in cols:
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
            with open("dxrx_logo.png", "rb") as f:
                st.image(f.read(), width=100)
        except Exception:
            st.warning("⚠️ Logo not found or cannot be loaded.")

        # Source Table
        st.header("⬇️ Source Table")
        dbs = [""] + [r[1] for r in sess.sql("SHOW DATABASES").collect()]
        db_src = st.selectbox("Database (source)", dbs, key="selected_database_source")
        schs = [""] + ([r[1] for r in sess.sql(f"SHOW SCHEMAS IN DATABASE {db_src}").collect()] if db_src else [])
        sch_src = st.selectbox("Schema (source)", schs, key="selected_schema_source", disabled=not db_src)
        tbls = [""] + ([r[0] for r in sess.sql(
            f"SELECT TABLE_NAME FROM {db_src}.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='{sch_src}'"
        ).collect()] if sch_src else [])
        tsrc = st.selectbox("Table (source)", tbls, key="selected_table_source", disabled=not sch_src)
        if st.button("🔄 Reset Source Table"):
            for k in ("selected_database_source", "selected_schema_source", "selected_table_source"):
                st.session_state.pop(k, None)

        st.markdown("<br>", unsafe_allow_html=True)

        # Target Table
        st.header("🎯 Target Table")
        db_tgt = st.selectbox("Database (target)", dbs, key="selected_database_target")
        schs_t = [""] + ([r[1] for r in sess.sql(f"SHOW SCHEMAS IN DATABASE {db_tgt}").collect()] if db_tgt else [])
        sch_tgt = st.selectbox("Schema (target)", schs_t, key="selected_schema_target", disabled=not db_tgt)
        tbls_t = [""] + ([r[0] for r in sess.sql(
            f"SELECT TABLE_NAME FROM {db_tgt}.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='{sch_tgt}'"
        ).collect()] if sch_tgt else [])
        tgt = st.selectbox("Table (target)", tbls_t, key="selected_table_target", disabled=not sch_tgt)
        if st.button("🔄 Reset Target Table"):
            for k in ("selected_database_target", "selected_schema_target", "selected_table_target"):
                st.session_state.pop(k, None)

        st.markdown("<br>", unsafe_allow_html=True)

        # Join Key(s)
        st.header("🔗 Join Key(s)")
        st.caption("Pick one or more columns to form a composite key.")
        cols = ([r[0] for r in sess.sql(
            f"SELECT COLUMN_NAME FROM {db_src}.INFORMATION_SCHEMA.COLUMNS "
            f"WHERE TABLE_SCHEMA='{sch_src}' AND TABLE_NAME='{tsrc}' "
            f"ORDER BY ORDINAL_POSITION"
        ).collect()] if tsrc else [])

        # Ensure join_key is always a list and only contains valid columns
        default_keys = st.session_state.get("join_key", [])
        if not isinstance(default_keys, list):
            default_keys = []
        default_keys = [k for k in default_keys if k in cols]

        jk_multi = st.multiselect(
            label="Join key(s)",
            options=cols,
            default=default_keys,
            key="join_key",
            help="Pick one or more columns to form a composite key"
        )

        # Reset All / Compare
        if st.button("🔄 Reset All"):
            st.session_state.reset_all()
        valid = all([db_src, sch_src, tsrc, db_tgt, sch_tgt, tgt]) and len(st.session_state.join_key) > 0
        st.markdown("<br>", unsafe_allow_html=True)
        run_btn = st.button("🔍 Compare Tables", disabled=not valid)

    return run_btn, (db_src, sch_src, tsrc, db_tgt, sch_tgt, tgt)

################################################################################
# 🖼 Results rendering  – Snowflake-safe (no st.download_button)
################################################################################

def _data_uri(data: bytes, filename: str, mime: str) -> str:
    """Return an <a download> tag whose href is a base-64 data URI."""
    b64 = base64.b64encode(data).decode()
    return (
        f'<a download="{html.escape(filename, quote=True)}" '
        f'href="data:{mime};base64,{b64}">📥 {html.escape(filename)}</a>'
    )

def _presigned_link(sess, data: bytes, filename: str, mime: str) -> str:
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp.write(data)
        tmp_path = tmp.name
    sess.file.put(tmp_path, STAGE_NAME, overwrite=True)
    url = sess.sql(
        f"SELECT GET_PRESIGNED_URL('{STAGE_NAME}','{os.path.basename(tmp_path)}')"
    ).collect()[0][0]
    return (
        f'<a target="_blank" rel="noopener noreferrer" href="{html.escape(url, True)}">'
        f'📥 {html.escape(filename)}</a>'
    )

def _download_anchor(sess, data: bytes, filename: str, mime: str) -> str:
    return (
        _data_uri(data, filename, mime)
        if len(data) <= MAX_EMBED_SIZE
        else _presigned_link(sess, data, filename, mime)
    )

def render_results() -> None:
    st.success("✅ Comparison complete")

    # ------------------------------------------------------------------ #
    # 1️⃣  Cache payloads - ADD JSON FORMATS
    # ------------------------------------------------------------------ #
    if "col_summary_bytes" not in st.session_state:
        # CSV payload
        st.session_state.col_summary_bytes = (
            st.session_state.column_diff_summary.to_csv(index=False).encode()
        )
        # NEW: JSON payload
        st.session_state.col_summary_json_bytes = json.dumps(
            st.session_state.column_diff_summary.to_dict(orient="records"),
            indent=2,
            default=str
        ).encode()

    if "new_rows_bytes" not in st.session_state:
        # CSV payload
        st.session_state.new_rows_bytes = (
            st.session_state.new_records_df.to_csv(index=False).encode()
        )
        # NEW: JSON payload
        st.session_state.new_rows_json_bytes = json.dumps(
            st.session_state.new_records_df.to_dict(orient="records"),
            indent=2,
            default=str
        ).encode()

    if "dropped_rows_bytes" not in st.session_state:
        # CSV payload
        st.session_state.dropped_rows_bytes = (
            st.session_state.dropped_records_df.to_csv(index=False).encode()
        )
        # NEW: JSON payload
        st.session_state.dropped_rows_json_bytes = json.dumps(
            st.session_state.dropped_records_df.to_dict(orient="records"),
            indent=2,
            default=str
        ).encode()

    if "changed_rows_bytes" not in st.session_state:
        # Existing JSON payload (unchanged)
        st.session_state.changed_rows_bytes = json.dumps(
            st.session_state.changed_records_df.to_dict(orient="records"),
            indent=2,
            default=str,
        ).encode()

    sess = st.session_state.session

    # ------------------------------------------------------------------ #
    # 2️⃣  Column summary (always show) - ADD JSON DOWNLOAD
    # ------------------------------------------------------------------ #
    st.subheader("📊 Column-level summary")
    st.dataframe(st.session_state.column_diff_summary, use_container_width=True)

    # NEW: Show both CSV and JSON download options
    st.markdown(
        _download_anchor(sess,
                         st.session_state.col_summary_bytes,
                         "column_summary.csv",
                         "text/csv"),
        unsafe_allow_html=True,
    )
    st.markdown(
        _download_anchor(sess,
                         st.session_state.col_summary_json_bytes,
                         "column_summary.json",
                         "application/json"),
        unsafe_allow_html=True,
    )

    # ------------------------------------------------------------------ #
    # 3️⃣  Metrics row (unchanged)
    # ------------------------------------------------------------------ #
    new_ct     = len(st.session_state.new_records_df)
    dropped_ct = len(st.session_state.dropped_records_df)
    changed_ct = len(st.session_state.changed_records_df)

    n, d, c = st.columns(3)
    n.metric("New rows",     new_ct)
    d.metric("Dropped rows", dropped_ct)
    c.metric("Changed rows", changed_ct)

    # ------------------------------------------------------------------ #
    # 4️⃣  Conditional download links + expanders - ADD JSON FOR ALL
    # ------------------------------------------------------------------ #
    if new_ct:
        # NEW: Show both CSV and JSON
        st.markdown(
            _download_anchor(sess,
                             st.session_state.new_rows_bytes,
                             "new_rows.csv",
                             "text/csv"),
            unsafe_allow_html=True,
        )
        st.markdown(
            _download_anchor(sess,
                             st.session_state.new_rows_json_bytes,
                             "new_rows.json",
                             "application/json"),
            unsafe_allow_html=True,
        )
        with st.expander("🆕 New records", expanded=False):
            st.dataframe(
                st.session_state.new_records_df.head(MAX_ROWS),
                use_container_width=True,
            )

    if dropped_ct:
        # NEW: Show both CSV and JSON
        st.markdown(
            _download_anchor(sess,
                             st.session_state.dropped_rows_bytes,
                             "dropped_rows.csv",
                             "text/csv"),
            unsafe_allow_html=True,
        )
        st.markdown(
            _download_anchor(sess,
                             st.session_state.dropped_rows_json_bytes,
                             "dropped_rows.json",
                             "application/json"),
            unsafe_allow_html=True,
        )
        with st.expander("🗑️ Dropped records", expanded=False):
            st.dataframe(
                st.session_state.dropped_records_df.head(MAX_ROWS),
                use_container_width=True,
            )

    if changed_ct:
        # Existing JSON download (unchanged)
        st.markdown(
            _download_anchor(sess,
                             st.session_state.changed_rows_bytes,
                             "changed_rows.json",
                             "application/json"),
            unsafe_allow_html=True,
        )
        with st.expander("🔄 Changed records – diffs", expanded=False):
            st.json(
                st.session_state.changed_records_df.to_dict(orient="records")
            )

################################################################################
# 🔗 Main
################################################################################

def main() -> None:
    st.set_page_config(
        page_title="Cohort Comparison Tool – PoC",
        page_icon="🧪",
        layout="wide"
    )
    st.title("Cohort Comparison Tool 🧬🚀 – PoC")

    init_state()

    # Sidebar selection
    clicked, ids = render_sidebar()

    # Show instructions until first run
    if not st.session_state.comparison_ran and not clicked:
        st.markdown(
            """
            **At a Glance:**  
            • Select **Source** & **Target** tables via `Database` → `Schema` → `Table`.  
            • Pick one or more **Join Key(s)** columns.  
            • Click **Compare Tables** to compute diffs:
              - New rows  
              - Dropped rows  
              - Changed rows with JSON change summary
            """,
            unsafe_allow_html=True,
        )
        return

    # Run comparison
    if clicked:
        db_src, sch_src, tbl_src, db_tgt, sch_tgt, tbl_tgt = ids
        sess = st.session_state.session

        with st.spinner("🔄 Fetching & computing diffs..."):
            t0 = time.time()
            df_base   = fetch_table(sess, db_src, sch_src, tbl_src)
            df_target = fetch_table(sess, db_tgt, sch_tgt, tbl_tgt)
            t1 = time.time()
            st.info(f"✅ Fetched tables in {t1 - t0:.2f}s (rows: source={len(df_base)}, target={len(df_target)})")

            # Schema check
            if not compare_schemas_strict(df_base, df_target):
                st.error("❌ Table schemas do not match exactly. Please select tables with identical schemas before comparing.")
                return

            # Row guard
            if len(df_base)>MAX_ROWS or len(df_target)>MAX_ROWS:
                st.warning(f"⚠️ Large (> {MAX_ROWS:,}) may take longer.")

            # Diff
            try:
                new_df, drop_df, change_df = compute_diffs(
                    df_base, df_target, st.session_state.join_key  # composite list
                )
            except ValueError as e:
                st.error(f"❌ {e}")
                return

            # Save and render
            st.session_state.new_records_df       = new_df
            st.session_state.dropped_records_df   = drop_df
            st.session_state.changed_records_df   = change_df
            st.session_state.column_diff_summary  = summarise_column_diffs(df_base, df_target)
            st.session_state.comparison_ran       = True

        render_results()

    # Persist results on rerun
    elif st.session_state.comparison_ran:
        render_results()

if __name__ == "__main__":
    main()
