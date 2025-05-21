"""
Runner script for the Cohort Comparison Tool PoC 🧬🚀

Open this file in IntelliJ IDEA (or PyCharm) and press the green **Run** button
(or ⌃R / Shift + F10). The script will launch the Streamlit app automatically
using the correct CLI invocation, so you don’t have to type anything in the
terminal.

If you rename the main app file, update `APP_FILE` below.
"""

import subprocess
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Config – name of the Streamlit application file (relative to this script)
# ---------------------------------------------------------------------------
APP_FILE = "run_cohort_comparison.py"  # change if you renamed the app file


def main() -> None:
    """Launch the Streamlit application via `streamlit run`."""
    app_path = Path(__file__).with_name(APP_FILE)

    if not app_path.exists():
        sys.exit(f"❌ Cannot find app file: {app_path}")

    # Equivalent to running: `streamlit run <app_path>`
    subprocess.run([sys.executable, "-m", "streamlit", "run", str(app_path)])


if __name__ == "__main__":
    main()
