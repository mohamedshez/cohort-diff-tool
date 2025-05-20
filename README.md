# Cohort Diff Tool PoC 🧬🚀

Streamlit application for automated comparison of cohort‑derived datasets in Snowflake.
#### Inspired and Built from cohort-builder example - https://github.com/Snowflake-Labs/sfguide-cohort-builder

* **Author**: Mohamed Shez (SWE)
* **Created**: 20‑May‑2025
* **Last updated**: 20‑May‑2025
* **Version**: 0.1.0
* **Status**: PoC (Proof of Concept)
* **License**: Apache 2.0
* **Contact**:
* * GitHub - (https://github.com/shez1461)

---

## 📌 At‑a‑Glance

The Cohort Comparison Tool replaces error‑prone manual checks with a **scalable, structured workflow** that detects differences between two cohort‑derived datasets—whether they have **identical or variant schemas**. Analysts can validate cohort changes, test logic modifications and build client‑ready diff summaries in seconds.

---

## 📝 Acceptance Criteria

| #   | Category               | Requirement                                                                                                                                                                                         |
| --- |------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1️⃣ | Identical Schemas      | • Accept two tables with matching column structures  <br>• Report **NEW**, **DROPPED** and **CHANGED** rows  <br>• Emit JSON `change_summary` for changed rows                                      |
| 2️⃣ | Different Schemas      | • Accept tables with differing structures  <br>• Produce **stock‑level** and **NPI‑level** difference summaries plus overall match counts                                                         |
| 3️⃣ | File‑level Summary     | • If `STOCK_ID` present → summarise **new / dropped / matched stocks**  <br>• If `UUID` present → summarise **new / dropped / matched UUIDs**                                                       |
| 4️⃣ | Usability & Outputs    | • Flag schema mismatches clearly  <br>• Show analyst‑friendly summaries and optional machine‑readable JSON diff logs  <br>• Format output for downstream validation dashboards / approval workflows |
| 5️⃣ | Non‑functional (draft) | • Compare up to **X** rows within **X** minutes (TBC)  <br>• Log and surface malformed input or missing‑field errors                                                                                |

---

## 🏗️ Architecture

* **Streamlit** front‑end (runs inside Snowflake Native Apps *or* locally).
* **Snowflake** back‑end for data retrieval and compute.
* **Pandas** for in‑memory diff logic (PoC‑scale).

```
┌─────────┐     SQL    ┌─────────────┐
│Streamlit│  ───────▶  │ Snowflake   │
│  UI     │  ◀───────  │ (tables)    │
└─────────┘  DataFrame └─────────────┘
```

> ⚠️  For large datasets the diff logic should migrate to **Snowpark** or **SQL MERGE** routines; this PoC keeps everything in memory for simplicity.

---

## 🚀 Quick Start

### 1 ‑ Prerequisites

* Python `3.9+`
* `pip install -r requirements.txt`
* Snowflake account with a role that can `SELECT` the tables you plan to compare.

### 2 ‑ Configure Secrets

Create a `.streamlit/secrets.toml` (local) or use Snowflake secret manager (native app):

```toml
[snowflake]
user = "YOUR_USER"
password = "YOUR_PASSWORD"
account = "YOUR_ACCOUNT"
warehouse = "YOUR_WAREHOUSE"
role = "YOUR_ROLE"
```

### 3 ‑ Run Locally

```bash
streamlit run cohort_diff_app.py
```

### 4 ‑ Run Inside Snowflake

1. Upload `cohort_diff_app.py` to the Snowflake **/app** directory.
2. Create a Streamlit app in the Snowflake UI and point it to the script.

---

## 🖥️ Usage

1. **Select source & target tables** (can be in different databases/schemas).
2. Enter the **join key** (e.g. `STOCK_ID`).
3. Click **🔍 Compare Tables**.
4. Review the **Summary metrics** and explore **New**, **Dropped**, and **Changed** record tabs.
5. Copy or download the JSON diff logs for downstream validation.

---

## 🧩 Extending This PoC

* **Schema‑variant comparison** – switch to column‑set intersection & union logic.
* **Large‑scale data** – push diff computation into Snowpark UDFs / JavaScript.
* **Export options** – write diff outputs back to Snowflake tables or S3 as Parquet.

---

## 📄 License

Apache 2.0 (see `LICENSE` file).

---

## 🙏 Acknowledgements

* Snowflake Labs Streamlit examples & Udemy courses for initial concept & implementation.

---

Happy comparing! 🎉
