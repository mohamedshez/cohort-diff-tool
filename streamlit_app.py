"""
Cohort Comparison Tool 🧬🚀
Step-2 PoC – compares Snowflake tables with **identical OR differing** schemas.

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
from typing import Dict, List, Tuple

import pandas as pd
import streamlit as st
import utils


# ──────────────────────────────────────────────────────────────────────────────
# 🔧 Constants & settings
# ──────────────────────────────────────────────────────────────────────────────
MAX_ROWS       = 1_000         # UI display guard for 1k rows
MAX_EMBED_SIZE = 2_000_000     # ~2 MB before using presigned URL
MAX_LIMIT      = 1_000_000     # 1M Snowpark row limit for `to_pandas()`
STAGE_NAME     = "@streamlit_downloads"

warnings.filterwarnings(
    "ignore", message="pandas only supports SQLAlchemy connectable.*"
)
warnings.filterwarnings(
    "ignore", message="You have an incompatible version of 'pyarrow'.*"
)

# ──────────────────────────────────────────────────────────────────────────────
# 🛠 Re-usable helpers – candidate for utils.py in a later refactor
# ──────────────────────────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False)
def fetch_table(_sess: Session, db: str, sch: str, tbl: str) -> pd.DataFrame:
    fq_name = f"{utils._quote(db)}.{utils._quote(sch)}.{utils._quote(tbl)}"
    return _sess.table(fq_name).to_pandas()

@st.cache_data(show_spinner=False)
def compare_schemas_strict(df1: pd.DataFrame, df2: pd.DataFrame) -> bool:
    """True ⇢ columns & dtypes match **exactly** (order as well)."""
    return (
        list(df1.columns) == list(df2.columns)
        and all(df1.dtypes.values == df2.dtypes.values)
    )

@st.cache_data(show_spinner=False, ttl=60)
def compute_diffs(
    df_base: pd.DataFrame,
    df_updated: pd.DataFrame,
    keys: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Row-level diff (new / dropped / changed) with composite key,
    automatically deduplicating on the key columns."""
    # 1️⃣ Ensure join-key columns exist
    for k in keys:
        if k not in df_base.columns or k not in df_updated.columns:
            raise ValueError(f"Join key '{k}' missing from one of the tables.")

    # 2️⃣ Drop duplicate rows on the key(s), keep first occurrence
    df_base = df_base.drop_duplicates(subset=keys, keep="first")
    df_updated = df_updated.drop_duplicates(subset=keys, keep="first")

    # 3️⃣ Re-index for diffing
    base = df_base.set_index(keys)
    upd  = df_updated.set_index(keys)

    # 4️⃣ New records (in target but not in source)
    new_idx  = upd.index.difference(base.index)
    new_df   = upd.loc[new_idx].reset_index()

    # 5️⃣ Dropped records (in source but not in target)
    drop_idx = base.index.difference(upd.index)
    drop_df  = base.loc[drop_idx].reset_index()

    # 6️⃣ Changed records
    # Only compare non-key columns
    common_cols   = [c for c in df_base.columns if c not in keys]
    intersect_idx = base.index.intersection(upd.index)

    changes: List[Dict[str, object]] = []
    ts = datetime.now(timezone.utc).isoformat()
    for idx in intersect_idx:
        diffs: Dict[str,Dict[str,object]] = {}
        for col in common_cols:
            a, b = base.at[idx, col], upd.at[idx, col]
            if pd.isna(a) and pd.isna(b):
                continue
            if (a != b) or (pd.isna(a) != pd.isna(b)):
                if pd.isna(a):
                    change_type = "ADDED"
                elif pd.isna(b):
                    change_type = "REMOVED"
                else:
                    change_type = "MODIFIED"
                diffs[col] = {
                    "change_type": change_type,
                    "from": utils._json_safe(a),
                    "to":   utils._json_safe(b),
                }
        if diffs:
            # Pack key into a dict (handles multi-column keys)
            key_dict = (
                {k: idx[i] for i,k in enumerate(keys)}
                if len(keys) > 1
                else {keys[0]: idx}
            )
            changes.append({"key": key_dict, "timestamp": ts, "changes": diffs})

    return new_df, drop_df, pd.DataFrame(changes)


# ──────────────────────────────────────────────────────────────────────────────
# 🎛 State & Snowpark connection
# ──────────────────────────────────────────────────────────────────────────────
def init_state() -> None:
    """Initialise Streamlit session-state and Snowpark session."""
    defaults: Dict[str, object] = dict(
        selected_database_source="",
        selected_schema_source="",
        selected_table_source="",
        selected_database_target="",
        selected_schema_target="",
        selected_table_target="",
        join_key=[],
        selected_columns=[],
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
        st.session_state["comparison_ran"] = False

    st.session_state["reset_all"] = reset_all

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

    # Ensure our download stage exists
    try:
        stage = STAGE_NAME.lstrip("@")
        session.sql(f"CREATE STAGE IF NOT EXISTS {stage}").collect()
    except Exception:
        pass

    st.session_state.session = session
    return session


# ──────────────────────────────────────────────────────────────────────────────
# 🖥 Sidebar – database / table / key selectors
# ──────────────────────────────────────────────────────────────────────────────
def render_sidebar() -> Tuple[bool, Tuple[str, str, str, str, str, str]]:
    """Render sidebar controls and return the 'Compare' button state + IDs."""
    sess = st.session_state.session
    if sess is None:
        st.sidebar.error("⛔️ No Snowpark session; initialise connection.")
        return False, ("",) * 6

    with st.sidebar:
        # (logo & UI identical to Step-1)
        try:
            with open("logo.png", "rb") as f:
                st.image(f.read(), width=100)
        except Exception:
            st.warning("⚠️ Logo not found or cannot be loaded.")

        # Source table selectors
        st.header("⬇️ Source Table")
        dbs = [""] + [r[1] for r in sess.sql("SHOW DATABASES").collect()]
        db_src = st.selectbox("Database (source)", dbs, key="selected_database_source")

        schs = (
            [""]
            + [r[1] for r in sess.sql(f"SHOW SCHEMAS IN DATABASE {db_src}").collect()]
            if db_src
            else []
        )
        sch_src = st.selectbox("Schema (source)", schs, key="selected_schema_source", disabled=not db_src)

        tbls = (
            [""]
            + [
                r[0]
                for r in sess.sql(
                    f"SELECT TABLE_NAME FROM {db_src}.INFORMATION_SCHEMA.TABLES "
                    f"WHERE TABLE_SCHEMA='{sch_src}'"
                ).collect()
            ]
            if sch_src
            else []
        )
        tsrc = st.selectbox("Table (source)", tbls, key="selected_table_source", disabled=not sch_src)
        if st.button("🔄 Reset Source Table"):
            for k in ("selected_database_source", "selected_schema_source", "selected_table_source"):
                st.session_state.pop(k, None)

        st.markdown("<br>", unsafe_allow_html=True)

        # Target table selectors
        st.header("🎯 Target Table")
        db_tgt = st.selectbox("Database (target)", dbs, key="selected_database_target")

        schs_t = (
            [""]
            + [r[1] for r in sess.sql(f"SHOW SCHEMAS IN DATABASE {db_tgt}").collect()]
            if db_tgt
            else []
        )
        sch_tgt = st.selectbox("Schema (target)", schs_t, key="selected_schema_target", disabled=not db_tgt)

        tbls_t = (
            [""]
            + [
                r[0]
                for r in sess.sql(
                    f"SELECT TABLE_NAME FROM {db_tgt}.INFORMATION_SCHEMA.TABLES "
                    f"WHERE TABLE_SCHEMA='{sch_tgt}'"
                ).collect()
            ]
            if sch_tgt
            else []
        )
        tgt = st.selectbox("Table (target)", tbls_t, key="selected_table_target", disabled=not sch_tgt)
        if st.button("🔄 Reset Target Table"):
            for k in ("selected_database_target", "selected_schema_target", "selected_table_target"):
                st.session_state.pop(k, None)

        st.markdown("<br>", unsafe_allow_html=True)

        # Join key
        st.header("🔗 Join Key(s)")
        st.caption("Pick one or more columns to form the composite key.")

        cols = (
            [
                r[0]
                for r in sess.sql(
                    f"SELECT COLUMN_NAME FROM {db_src}.INFORMATION_SCHEMA.COLUMNS "
                    f"WHERE TABLE_SCHEMA='{sch_src}' AND TABLE_NAME='{tsrc}' "
                    f"ORDER BY ORDINAL_POSITION"
                ).collect()
            ]
            if tsrc
            else []
        )

        default_keys = st.session_state.get("join_key", [])
        if not isinstance(default_keys, list):
            default_keys = []
        default_keys = [k for k in default_keys if k in cols]

        st.multiselect(
            label="Join key(s)",
            options=cols,
            default=default_keys,
            key="join_key",
            help="One or more columns that uniquely identify each row",
        )

        # Buttons
        if st.button("🔄 Reset All"):
            st.session_state.reset_all()

        valid = all(
            [db_src, sch_src, tsrc, db_tgt, sch_tgt, tgt, len(st.session_state.join_key) > 0]
        )
        st.markdown("<br>", unsafe_allow_html=True)
        run_btn = st.button("🔍 Compare Tables", disabled=not valid)

    return run_btn, (db_src, sch_src, tsrc, db_tgt, sch_tgt, tgt)


# ──────────────────────────────────────────────────────────────────────────────
# 📥 Result helpers (download anchors unchanged)
# ──────────────────────────────────────────────────────────────────────────────
def _data_uri(data: bytes, filename: str, mime: str) -> str:
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
    # Always try data-URI for small payloads,
    # otherwise attempt presigned-link but fall back if that errors.
    if len(data) <= MAX_EMBED_SIZE:
        return _data_uri(data, filename, mime)
    try:
        return _presigned_link(sess, data, filename, mime)
    except Exception:
        # Fallback to data-URI if presigned link fails
        # e.g. stage not authorized or missing
        return _data_uri(data, filename, mime)


# ──────────────────────────────────────────────────────────────────────────────
# 🖼 Results renderer
# ──────────────────────────────────────────────────────────────────────────────
def render_results() -> None:
    st.success("✅ Comparison complete")

    # 1️⃣ Cache payloads
    def _cache_bytes(state_key: str, df: pd.DataFrame) -> None:
        if state_key not in st.session_state:
            st.session_state[state_key] = df.to_csv(index=False).encode()
            st.session_state[state_key.replace("_bytes", "_json_bytes")] = json.dumps(
                df.to_dict(orient="records"), indent=2, default=str
            ).encode()

    _cache_bytes("col_summary_bytes",  st.session_state.column_diff_summary)
    _cache_bytes("new_rows_bytes",     st.session_state.new_records_df)
    _cache_bytes("dropped_rows_bytes", st.session_state.dropped_records_df)

    if "changed_rows_bytes" not in st.session_state:
        st.session_state.changed_rows_bytes = json.dumps(
            st.session_state.changed_records_df.to_dict(orient="records"),
            indent=2,
            default=str,
        ).encode()

    sess = st.session_state.session

    # 2️⃣ Column summary
    st.subheader("📊 Column-level summary")
    tab1, tab2 = st.tabs(["📋 Table View", "📈 Graph View"])

    # ➕ Metrics
    with tab1:
        st.dataframe(st.session_state.column_diff_summary, use_container_width=True)

        st.markdown(
            _download_anchor(sess, st.session_state.col_summary_bytes, "column_summary.csv", "text/csv"),
            unsafe_allow_html=True,
        )
        st.markdown(
            _download_anchor(sess, st.session_state.col_summary_json_bytes, "column_summary.json", "application/json"),
            unsafe_allow_html=True,
        )

        # 3️⃣ Metrics row
        new_ct     = len(st.session_state.new_records_df)
        dropped_ct = len(st.session_state.dropped_records_df)
        changed_ct = len(st.session_state.changed_records_df)

        n, d, c = st.columns(3)
        n.metric("New rows",     new_ct)
        d.metric("Dropped rows", dropped_ct)
        c.metric("Changed rows", changed_ct)

        # 4️⃣ Downloads + expanders
        if new_ct:
            st.markdown(
                _download_anchor(sess, st.session_state.new_rows_bytes, "new_rows.csv", "text/csv"),
                unsafe_allow_html=True,
            )
            st.markdown(
                _download_anchor(sess, st.session_state.new_rows_json_bytes, "new_rows.json", "application/json"),
                unsafe_allow_html=True,
            )
            with st.expander("🆕 New records", expanded=False):
                st.dataframe(st.session_state.new_records_df.head(MAX_ROWS), use_container_width=True)

        if dropped_ct:
            st.markdown(
                _download_anchor(sess, st.session_state.dropped_rows_bytes, "dropped_rows.csv", "text/csv"),
                unsafe_allow_html=True,
            )
            st.markdown(
                _download_anchor(sess, st.session_state.dropped_rows_json_bytes, "dropped_rows.json", "application/json"),
                unsafe_allow_html=True,
            )
            with st.expander("🗑️ Dropped records", expanded=False):
                st.dataframe(st.session_state.dropped_records_df.head(MAX_ROWS), use_container_width=True)

        if changed_ct:
            st.markdown(
                _download_anchor(sess, st.session_state.changed_rows_bytes, "changed_rows.json", "application/json"),
                unsafe_allow_html=True,
            )
            with st.expander("🔄 Changed records – diffs", expanded=False):
                st.json(st.session_state.changed_records_df.to_dict(orient="records"))

    # 📊 Chart (built-in)
    with tab2:
        col1, col2 = st.columns(2)

        # ➕ Metrics
        with col1:
            new_ct     = len(st.session_state.new_records_df)
            dropped_ct = len(st.session_state.dropped_records_df)
            changed_ct = len(st.session_state.changed_records_df)

            st.metric("New Rows", new_ct)
            st.metric("Dropped Rows", dropped_ct)
            st.metric("Changed Rows", changed_ct)

        # 📊 Chart (built-in)
        with col2:
            chart_data = pd.DataFrame({
                "Change Type": ["New", "Dropped", "Changed"],
                "Count": [new_ct, dropped_ct, changed_ct]
            })
            st.bar_chart(chart_data.set_index("Change Type"))


# ──────────────────────────────────────────────────────────────────────────────
# 🔗 Main
# ──────────────────────────────────────────────────────────────────────────────
def main() -> None:
    st.set_page_config(page_title="Cohort Comparison Tool – PoC", page_icon="🧪", layout="wide")
    st.title("Cohort Comparison Tool 🧬🚀 – PoC")

    init_state()
    clicked, ids = render_sidebar()

    # First-run helper text
    if not st.session_state.comparison_ran and not clicked:
        st.markdown(
            """
            **How to use:**  
            1. Pick **Source** & **Target** tables (they *can* live in different DB/schemas).  
            2. Select one or more **Join Key** columns present in *both* tables.  
            3. Click **Compare Tables**.  
               * If schemas match ➜ full comparison.  
               * If schemas differ ➜ only shared columns are compared.
            """,
            unsafe_allow_html=True,
        )
        return

    # Run comparison
    if clicked:
        db_src, sch_src, tbl_src, db_tgt, sch_tgt, tbl_tgt = ids
        sess = st.session_state.session

        with st.spinner("🔄 Fetching & computing diffs…"):
            # ───── Init progress bar ───────────────────────────────────────────
            progress_placeholder = st.empty()
            progress_bar = progress_placeholder.progress(0)

            # ── 1) Fetch tables ────────────────────────────────────────────────
            t0 = time.time()
            df_base_raw = fetch_table(sess, db_src, sch_src, tbl_src)
            progress_bar.progress(10)

            df_target_raw = fetch_table(sess, db_tgt, sch_tgt, tbl_tgt)
            progress_bar.progress(20)

            st.info(
                f"✅ Fetched tables in {time.time() - t0:.2f}s "
                f"(rows: source={len(df_base_raw):,}, target={len(df_target_raw):,})"
            )
            progress_bar.progress(30)

            # ── 2) Column intersection + validation ─────────────────────────────
            common_cols = utils.get_common_columns(df_base_raw, df_target_raw)
            if not common_cols:
                progress_placeholder.empty()
                st.error("❌ No matching columns between the two tables – nothing to compare.")
                return
            progress_bar.progress(40)

            missing_keys = [k for k in st.session_state.join_key if k not in common_cols]
            if missing_keys:
                progress_placeholder.empty()
                st.error(f"❌ Selected join key(s) {missing_keys} not found in both tables.")
                st.info(f"❗️ Please pick a key that exists in both tables and try again.")
                return
            progress_bar.progress(50)

            # ── 3) Column selection for differing schemas ───────────────────────
            if not compare_schemas_strict(df_base_raw, df_target_raw):
                # remove bar when we show the “schemas differ” info
                progress_placeholder.empty()
                st.info("ℹ️ Schemas differ – comparing only shared columns.")
                cols_to_keep = st.session_state.join_key + [
                    c for c in common_cols if c not in st.session_state.join_key
                ]
                df_base = df_base_raw[cols_to_keep]
                df_target = df_target_raw[cols_to_keep]
            else:
                st.success("✅ Schemas are identical – performing full column comparison.")
                df_base = df_base_raw
                df_target = df_target_raw
            progress_bar.progress(70)

            # ── 4) Row-count guard ─────────────────────────────────────────────
            if len(df_base) > MAX_ROWS or len(df_target) > MAX_ROWS:
                progress_placeholder.empty()
                st.warning(f"⚠️ Large tables (> {MAX_ROWS:,} rows) may take longer.")
                if len(df_base) == MAX_LIMIT or len(df_target) == MAX_LIMIT:
                    st.warning(
                        f"⚠️ One of the tables has exceeded {MAX_LIMIT:,} rows, "
                        "which may lead to performance issues. "
                        "Consider filtering by choosing more than one join key for narrow search."
                    )
            progress_bar.progress(80)

            # ── 5) Compute diffs ────────────────────────────────────────────────
            try:
                new_df, drop_df, change_df = compute_diffs(
                    df_base, df_target, st.session_state.join_key
                )
            except ValueError as e:
                progress_placeholder.empty()
                st.error(f"❌ {e}")
                return
            progress_bar.progress(90)

            # ── 6) Store & summarise ───────────────────────────────────────────
            st.session_state.new_records_df      = new_df
            st.session_state.dropped_records_df  = drop_df
            st.session_state.changed_records_df  = change_df
            st.session_state.column_diff_summary = utils.summarise_column_diffs(df_base, df_target)
            st.session_state.comparison_ran      = True

            # ── 7) Finish & clear bar ──────────────────────────────────────────
            progress_bar.progress(100)
            progress_placeholder.empty()

        render_results()

    # Keep results on rerun
    elif st.session_state.comparison_ran:
        render_results()


if __name__ == "__main__":
    main()
