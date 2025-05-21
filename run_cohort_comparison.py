"""
Cohort Comparison Tool PoC 🧬🚀
Streamlit application for automated comparison of cohort-derived datasets in Snowflake (sidebar layout)

Author : Mohamed Shez
Created: 2025-05-20 | Updated: 2025-05-22
"""

import streamlit as st
import warnings
# suppress pandas DBAPI warning about non-SQLAlchemy connections
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy connectable.*')
# suppress pyarrow version compatibility warning from Snowflake connector
warnings.filterwarnings('ignore', message='You have an incompatible version of \'pyarrow\' installed.*')
import snowflake.connector
import pandas as pd
import json
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

    def reset_fields():
        for key in list(defaults.keys()):
            st.session_state.pop(key, None)
        # clear results
        for k in ['new_records_df','dropped_records_df','changed_records_df']:
            st.session_state.pop(k, None)
    st.session_state['reset_fields'] = reset_fields

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
            st.error(f"❌ Could not connect to Snowflake: {err}. Please close other instances and try again.")

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
        diffs = {c: {"from": base.at[k, c], "to": upd.at[k, c]}
                 for c in df_base.columns if base.at[k, c] != upd.at[k, c]}
        if diffs:
            changed.append({"key": k, "timestamp": ts, "changes": diffs})
    return new_df, drop_df, pd.DataFrame(changed)

# ---------------------------------------------------------------------------
# Column list for join-key dropdown
# ---------------------------------------------------------------------------

def list_columns(conn, db: str, sch: str, tbl: str) -> List[str]:
    cur = conn.cursor()
    cur.execute(
        f"SELECT COLUMN_NAME FROM {db}.INFORMATION_SCHEMA.COLUMNS "
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
        return False, (None,)*6

    with st.sidebar:
        st.header("⬇️ Source Table")
        db_src = database_selectbox(st, conn, "selected_database_source")
        sch_list = [] if not db_src else list_schemas(conn, db_src)
        sch_src = st.selectbox("Schema (source)", [""]+sch_list, disabled=not db_src, key="selected_schema_source")
        tbl_list = [] if not sch_src else list_tables(conn, db_src, sch_src)
        tbl_src = st.selectbox("Table (source)", [""]+tbl_list, disabled=not sch_src, key="selected_table_source")

        st.markdown("<br>", unsafe_allow_html=True)

        st.header("🎯 Target Table")
        db_tgt = database_selectbox(st, conn, "selected_database_target")
        sch_list_t = [] if not db_tgt else list_schemas(conn, db_tgt)
        sch_tgt = st.selectbox("Schema (target)", [""]+sch_list_t, disabled=not db_tgt, key="selected_schema_target")
        tbl_list_t = [] if not sch_tgt else list_tables(conn, db_tgt, sch_tgt)
        tbl_tgt = st.selectbox("Table (target)", [""]+tbl_list_t, disabled=not sch_tgt, key="selected_table_target")

        st.markdown("<br>", unsafe_allow_html=True)

        st.header("🔗 Join Key Column")
        join_opts = [] if not tbl_src else list_columns(conn, db_src, sch_src, tbl_src)
        join_key = st.selectbox("Join key", [""]+join_opts, disabled=not tbl_src, key="join_key")

        if st.button("🔄 Reset Selections"):
            st.session_state['reset_fields']()

        valid = all([db_src, sch_src, tbl_src, db_tgt, sch_tgt, tbl_tgt, join_key])
        run_btn = st.button("🔍 Compare Tables", disabled=not valid)

    return run_btn, (db_src, sch_src, tbl_src, db_tgt, sch_tgt, tbl_tgt)

################################################################################
# 🧩  Variant-schema summary
################################################################################

def variant_summary(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    out = []
    for key in ['PATIENT_ID','NPI']:
        if key in df1.columns and key in df2.columns:
            set1, set2 = set(df1[key]), set(df2[key])
            out.append({
                'level': key,
                'new': len(set2 - set1),
                'dropped': len(set1 - set2),
                'matched': len(set1 & set2)
            })
    return pd.DataFrame(out)

################################################################################
# 🖼  Main result area
################################################################################

def render_results(variant: bool=False, variant_df=None):
    st.success("Comparison complete!")
    if variant:
        st.subheader("Variant-schema Summary")
        st.dataframe(variant_df)
        # download summary
        st.download_button("📥 Download Summary CSV", variant_df.to_csv(index=False), "variant_summary.csv")
    else:
        c1,c2,c3 = st.columns(3)
        c1.metric("New", len(st.session_state.new_records_df))
        c2.metric("Dropped", len(st.session_state.dropped_records_df))
        c3.metric("Changed", len(st.session_state.changed_records_df))
        # downloads
        st.download_button("📥 Download New CSV", st.session_state.new_records_df.to_csv(index=False), "new_records.csv")
        st.download_button("📥 Download Dropped CSV", st.session_state.dropped_records_df.to_csv(index=False), "dropped_records.csv")
        st.download_button("📥 Download Changed JSON", json.dumps(st.session_state.changed_records_df.to_dict(orient='records'), indent=2), "changed_records.json")
        with st.expander("🆕 New Records"): st.dataframe(st.session_state.new_records_df)
        with st.expander("🗑️ Dropped Records"): st.dataframe(st.session_state.dropped_records_df)
        with st.expander("🔄 Changed Records – JSON diffs"): st.json(st.session_state.changed_records_df.to_dict(orient='records'))

################################################################################
# 🔗 main
################################################################################

def main():
    st.set_page_config(page_title="Cohort Comparison", page_icon="🧪", layout="wide")
    st.title("Cohort Comparison Tool – PoC")

    init_state()
    run_clicked, ids = render_sidebar()
    if run_clicked:
        conn = st.session_state.sf_conn
        dbs, schs, tbls, dbt, scht, tblt = ids
        df_src = fetch_table(conn, dbs, schs, tbls)
        df_tgt = fetch_table(conn, dbt, scht, tblt)
        # row guard
        if len(df_src)>MAX_ROWS or len(df_tgt)>MAX_ROWS:
            st.warning(f"Large table (> {MAX_ROWS:,} rows) – may fail.")
        # strict-schema
        if compare_schemas_strict(df_src, df_tgt):
            new_df, drop_df, chg_df = compute_diffs(df_src, df_tgt, st.session_state.join_key)
            st.session_state.update({"new_records_df":new_df,"dropped_records_df":drop_df,"changed_records_df":chg_df})
            render_results()
        else:
            # variant-schema branch
            var_df = variant_summary(df_src, df_tgt)
            render_results(variant=True, variant_df=var_df)

if __name__=="__main__": main()
