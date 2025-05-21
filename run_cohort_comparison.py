"""
Cohort Comparison Tool PoC 🧬🚀
Streamlit application for automated comparison of cohort-derived datasets in Snowflake (sidebar layout)

Author : Mohamed Shez
Created: 2025-05-20 | Updated: 2025-05-21
"""

import streamlit as st
import snowflake.connector
import pandas as pd
from typing import Tuple, List, Dict
from datetime import datetime, timezone

from utils_selects import database_selectbox, list_tables, list_schemas

MAX_ROWS = 100_000  # PoC guard

################################################################################
# 🎨  Session / connection
################################################################################

def init_state():
    defaults = dict(
        selected_database_source="", selected_schema_source="", selected_table_source="",
        selected_database_target="", selected_schema_target="", selected_table_target="",
        join_key="",
        new_records_df=pd.DataFrame(), dropped_records_df=pd.DataFrame(), changed_records_df=pd.DataFrame(),
        comparison_ran=False,
    )
    for k, v in defaults.items():
        st.session_state.setdefault(k, v)

    if "sf_conn" not in st.session_state:
        try:
            st.session_state.sf_conn = snowflake.connector.connect(
                user=st.secrets["snowflake"]["user"],
                password=st.secrets["snowflake"]["password"],
                account=st.secrets["snowflake"]["account"],
                warehouse=st.secrets["snowflake"]["warehouse"],
                role=st.secrets["snowflake"]["role"],
            )
        except st.runtime.secrets.StreamlitSecretNotFoundError:
            st.session_state.sf_conn = None
            st.warning("🔑 No Snowflake secrets found – add `.streamlit/secrets.toml` and refresh.")
        except Exception as err:
            st.session_state.sf_conn = None
            st.error(f"❌ Could not connect to Snowflake: {err} or it could be that you may have multiple instances of this app running. Please close all other instances and try again.")

################################################################################
# 🧮  Diff helpers
################################################################################

@st.cache_data(show_spinner=False)
def fetch_table(_conn, db: str, sch: str, tbl: str) -> pd.DataFrame:
    return pd.read_sql(f"SELECT * FROM {db}.{sch}.{tbl}", _conn)

@st.cache_data(show_spinner=False)
def compare_schemas_strict(df1: pd.DataFrame, df2: pd.DataFrame) -> bool:
    return list(df1.columns) == list(df2.columns) and all(df1.dtypes.values == df2.dtypes.values)

@st.cache_data(show_spinner=False)
def compute_diffs(df_base: pd.DataFrame, df_updated: pd.DataFrame, key: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    base, upd = df_base.set_index(key), df_updated.set_index(key)
    new_df  = upd.loc[upd.index.difference(base.index)].reset_index()
    drop_df = base.loc[base.index.difference(upd.index)].reset_index()
    ts = datetime.now(timezone.utc).isoformat()
    changed: List[Dict] = []
    for k in base.index.intersection(upd.index):
        diffs = {c: {"from": base.at[k, c], "to": upd.at[k, c]} for c in df_base.columns if base.at[k, c] != upd.at[k, c]}
        if diffs:
            changed.append({"key": k, "timestamp": ts, "changes": diffs})
    return new_df, drop_df, pd.DataFrame(changed)

# ---------------------------------------------------------------------------
# Column list for join-key dropdown
# ---------------------------------------------------------------------------

def list_columns(conn, db: str, sch: str, tbl: str) -> List[str]:
    cur = conn.cursor()
    cur.execute(
        f"SELECT COLUMN_NAME FROM {db}.INFORMATION_SCHEMA.COLUMNS\n"
        f"WHERE TABLE_SCHEMA='{sch}' AND TABLE_NAME='{tbl}' ORDER BY ORDINAL_POSITION"
    )
    cols = [row[0] for row in cur.fetchall()]
    cur.close()
    return cols

################################################################################
# 🖥️  Sidebar input panel
################################################################################

def render_sidebar():
    conn = st.session_state.sf_conn
    if conn is None:
        st.sidebar.info("🔌 No Snowflake connection. Add secrets and refresh.")
        return False, (None, None, None, None, None, None)

    with st.sidebar:
        # Source selectors
        st.header("⬇️ Source Table")
        db_src = database_selectbox(st, conn, "selected_database_source")
        sch_list = [] if not db_src else list_schemas(conn, db_src)
        sch_src = st.selectbox(
            "Schema (source)",
            options=[""] + sch_list,
            index=([""] + sch_list).index(st.session_state.selected_schema_source) if st.session_state.selected_schema_source in sch_list else 0,
            disabled=not db_src,
            key="selected_schema_source"
        )
        tbl_list = [] if not sch_src else list_tables(conn, db_src, sch_src)
        tbl_src = st.selectbox(
            "Table (source)",
            options=[""] + tbl_list,
            index=([""] + tbl_list).index(st.session_state.selected_table_source) if st.session_state.selected_table_source in tbl_list else 0,
            disabled=not sch_src,
            key="selected_table_source"
        )

        st.markdown("<br>", unsafe_allow_html=True)

        # Target selectors
        st.header("🎯 Target Table")
        db_tgt = database_selectbox(st, conn, "selected_database_target")
        sch_list_t = [] if not db_tgt else list_schemas(conn, db_tgt)
        sch_tgt = st.selectbox(
            "Schema (target)",
            options=[""] + sch_list_t,
            index=([""] + sch_list_t).index(st.session_state.selected_schema_target) if st.session_state.selected_schema_target in sch_list_t else 0,
            disabled=not db_tgt,
            key="selected_schema_target"
        )
        tbl_list_t = [] if not sch_tgt else list_tables(conn, db_tgt, sch_tgt)
        tbl_tgt = st.selectbox(
            "Table (target)",
            options=[""] + tbl_list_t,
            index=([""] + tbl_list_t).index(st.session_state.selected_table_target) if st.session_state.selected_table_target in tbl_list_t else 0,
            disabled=not sch_tgt,
            key="selected_table_target"
        )

        st.markdown("<br>", unsafe_allow_html=True)

        # Join-key dropdown
        st.header("🔗 Join Key Column")
        join_opts = [] if not tbl_src else list_columns(conn, db_src, sch_src, tbl_src)
        join_key = st.selectbox(
            "Join key", options=[""] + join_opts,
            index=([""] + join_opts).index(st.session_state.join_key) if st.session_state.join_key in join_opts else 0,
            disabled=not tbl_src,
            key="join_key"
        )

        valid = all([db_src, sch_src, tbl_src, db_tgt, sch_tgt, tbl_tgt, join_key])
        run_btn = st.button("🔍 Compare Tables", disabled=not valid)

    return run_btn, (db_src, sch_src, tbl_src, db_tgt, sch_tgt, tbl_tgt)

################################################################################
# 🖼  Main result area
################################################################################

def render_results():
    st.success("Comparison complete!")
    c1, c2, c3 = st.columns(3)
    c1.metric("New", len(st.session_state.new_records_df))
    c2.metric("Dropped", len(st.session_state.dropped_records_df))
    c3.metric("Changed", len(st.session_state.changed_records_df))

    with st.expander("🆕 New Records"):
        st.dataframe(st.session_state.new_records_df)
    with st.expander("🗑️ Dropped Records"):
        st.dataframe(st.session_state.dropped_records_df)
    with st.expander("🔄 Changed Records – JSON diffs"):
        st.json(st.session_state.changed_records_df.to_dict(orient="records"))

################################################################################
# 🔗 main
################################################################################

def main():
    st.set_page_config(page_title="Cohort Comparison", page_icon="🧪", layout="wide", initial_sidebar_state="expanded")
    st.title("Cohort Comparison Tool – PoC")

    init_state()
    run_clicked, ids = render_sidebar()

    if run_clicked:
        conn = st.session_state.sf_conn
        dbs, schs, tbls, dbt, scht, tblt = ids
        df_src = fetch_table(conn, dbs, schs, tbls)
        df_tgt = fetch_table(conn, dbt, scht, tblt)

        # quick row-count guard
        if len(df_src) > MAX_ROWS or len(df_tgt) > MAX_ROWS:
            st.warning(f"One of the tables exceeds {MAX_ROWS:,} rows; comparison may be slow or fail – aborting.")
        elif not compare_schemas_strict(df_src, df_tgt):
            # Detailed schema mismatch error
            src = f"{dbs}.{schs}.{tbls}"
            tgt = f"{dbt}.{scht}.{tblt}"
            st.error("⚠️ Schemas do not match:")
            st.markdown(
                f"- **Source**: `{src}`  \n- **Target**: `{tgt}`"
            )
            st.error("Please pick tables with identical column names, order and data types.")
            return
        else:
            new_df, drop_df, chg_df = compute_diffs(df_src, df_tgt, st.session_state.join_key)
            st.session_state.update({
                "new_records_df": new_df,
                "dropped_records_df": drop_df,
                "changed_records_df": chg_df,
                "comparison_ran": True,
            })

    if st.session_state.comparison_ran:
        render_results()

if __name__ == "__main__":
    main()
