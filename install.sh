#!/usr/bin/env bash
set -e  # stop on first error

###############################################################################
# 1 – Create & activate a virtual-env (if you prefer system Python, skip this)
###############################################################################
python -m venv venv
# shellcheck disable=SC1091
source venv/bin/activate         # Windows PowerShell: .\venv\Scripts\Activate.ps1

###############################################################################
# 2 – Install Python dependencies
###############################################################################
pip install --upgrade pip
pip install streamlit snowflake-connector-python pandas snowflake-snowpark-python

###############################################################################
# 3 – Store Snowflake credentials for Streamlit
###############################################################################
mkdir -p .streamlit

cat > .streamlit/secrets.toml <<'EOF'
[snowflake]
account  = "MSVVWOV-KJB88781"
user     = "MSHEZ"
password = "<place_your_password_here>"
role     = "ACCOUNTADMIN"
warehouse= "COHORT_BUILDER_LOAD_WH"
database = "SHEZ_RESEARCH_DEV"
schema   = "MOHAMED_SHEZ"
EOF

echo "✅ venv ready, packages installed, secrets.toml created."
echo "Run the app with:  streamlit run cohort_comparison_app.py"
