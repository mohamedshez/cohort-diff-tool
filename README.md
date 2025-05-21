# Cohort Diff Tool PoC 🧬🚀

Streamlit application for automated comparison of cohort‑derived datasets in Snowflake.

**Author**  Mohamed Shez   |   **Created** 20‑May‑2025   |   **Last updated** 21‑May‑2025

---
## 📌 At‑a‑Glance
A lightweight tool that spots **NEW**, **DROPPED** and **CHANGED** rows between two Snowflake tables that **share identical columns and data‑types**. Diff results include per‑record JSON change summaries stamped with UTC timestamps, ready for analyst validation or audit trails.

---
## 📝 Acceptance Criteria (PoC scope)
| # | Requirement                                                                         |
|---|-------------------------------------------------------------------------------------|
|1️⃣| Accept two tables with matching column **names, order _and_ dtypes**                |
|   | Detect <br>• New records <br>• Dropped records <br>• Changed records (column‑level) |
|   | Emit `change_summary` JSON **+ timestamp** for each changed record                  |
|2️⃣| Schema‑mismatch → clear error message, halt processing                              |
|3️⃣| Wizard UI (3 steps: Source ▸ Target ▸ Review) matches internal style                |
|4️⃣| On‑screen results only (no export)                                                  |
|5️⃣| Warn if either table > **100 000 rows** (PoC memory guard)                          |

---
## 🏗️ Architecture
```
┌─────────┐     SQL    ┌───────────┐
│Streamlit│  ───────▶  │ Snowflake │
│  UI     │  ◀───────  │ (tables)  │
└─────────┘  DataFrame └───────────┘

IntelliJ ▶︎ run_cohort_comparison.py ─┐
                                     ▼
    streamlit_snowflake_poc.py (UI) — utils (diff logic)
                                     ▼
                               Snowflake tables
```
* **Streamlit** front‑end with a simple step wizard.
* **Pandas** in‑memory diff (good up to ~500k rows). Upgrade to Snowpark for large prod tables.

---
## 🚀 Quick Start (local)
```bash
# 1. Clone / open the project in IntelliJ or PyCharm
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt  # streamlit, snowflake-connector-python, pandas

# 2. Configure Snowflake credentials
mkdir -p .streamlit && nano .streamlit/secrets.toml
```
```toml
[snowflake]
user = "YOUR_USER"
password = "YOUR_PASSWORD"
account = "YOUR_ACCOUNT"
warehouse = "YOUR_WH"
role = "DXRX_DEVELOPER"
```
```bash
# 3. Launch via helper script
python run_cohort_comparison.py
```
Open the printed `http://localhost:8501` link → follow the wizard.

---
## 🖥️ Usage Flow
1. **Step 1 – Source**: pick database / schema / table.  
2. **Step 2 – Target**: pick second table + join key.  
3. **Step 3 – Review**: verify selections ➜ **Run Comparison**.  
4. Expand result panels to inspect diffs.

---
## 🔄 Change Summary Example
```json
{
  "key": "12345",
  "timestamp": "2025-05-21T12:34:56.789Z",
  "changes": {
    "GENDER": {"from": "M", "to": "F"},
    "AGE":     {"from": 45,  "to": 46}
  }
}
```

---
## ✨ Extending This PoC
* **Schema‑variant support** – intersect/union column sets, pad NULLs.
* **Large datasets** – push diff to Snowpark UDF or SQL MERGE.
* **Exports** – add CSV / JSON download buttons or write tables back to Snowflake.

---
## License
Apache 2.0
